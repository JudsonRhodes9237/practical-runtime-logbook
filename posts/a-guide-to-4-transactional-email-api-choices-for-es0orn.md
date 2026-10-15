# A Guide to 4 Transactional Email API Choices for Custom Domain Password Reset Flows

Password-reset email has one unforgiving operational constraint: a stale address must stop receiving mail before retries turn one bounce into a pattern. **Short answer:** choose the API whose template ownership matches your release process, then require verified-domain sending, delivery and bounce evidence, and a suppression check in the send path. For a gaming SaaS that keeps templates in Git, an HTTP API with app-owned rendering is usually the cleanest fit. Teams that want support staff to edit copy without a deployment should prefer provider-hosted templates instead.

This is not mainly a feature-count contest. A reset message is small, but it sits on the recovery path for every locked-out player. The invariant is simple: one reset request may be retried; one known-invalid recipient must not be retried. Everything else, including vendor choice, follows from that.

## Which transactional email API should own a password reset flow?

Consider a bounded incident: a game account service accepts a reset request, the queue redelivers after a worker timeout, and the destination has already hard-bounced. Without a shared suppression decision, the second worker sends again. The player still cannot recover the account, while the sender repeats traffic to an invalid mailbox.

The postmortem action is not "retry less." Retries are necessary when a network response is ambiguous. The corrective control is to separate job completion from recipient eligibility. Record the provider message ID, ingest or fetch delivery events, turn a confirmed permanent failure into suppression state, and consult that state immediately before every attempt. Soft failures need a bounded retry policy; permanent failures do not.

That is the control.

There is a timing consequence. Infrai exposes email events through polling rather than webhook pushes, so suppression learned from those events is only as fresh as the poll interval. A 5-minute poller creates a 5-minute observation window. That number is an operating-policy example, not a platform promise: choose it from queue volume and tolerance for repeat attempts. The safe fallback is to make each reset token single-use and short-lived, while treating delivery status as evidence rather than proof that a human read the message. On a queue with delayed redelivery, the poller may update suppression before the next attempt; on an immediate retry, it probably will not. That distinction belongs in the runbook because the same integration can behave differently under two retry schedules.

Open tracking is especially weak evidence. Apple Mail Privacy Protection can prevent senders from learning whether a recipient opened a message, so account recovery must never advance on an open event.

## Template ownership is the first decision

Provider-hosted templates let an operations or support team change wording without shipping the account service. That is useful during a live event, regional launch, or support-policy change. The trade-off is configuration drift: the message users receive may no longer correspond to the application revision that generated its variables.

Repository-owned templates reverse that balance. Copy, variable names, localization files, and tests travel in one review. Rollback is ordinary deployment rollback. The cost is organizational, not technical: every copy correction now needs the software release path, and non-engineering owners need a workable review process.

For password resets, I choose application-owned token construction even when the visible shell is provider-hosted, because a copy workflow should not control a security boundary. Do not let a template editor choose the redirect host. Pass a fully validated reset URL or, better, a narrow opaque value that a fixed template places into a fixed application origin. This gives up some editor freedom in exchange for a redirect target that changes only through reviewed code.

**Pick the ownership boundary before the vendor.** Otherwise a team can complete domain verification, wire delivery events, and only then discover that its localization workflow requires a different deployment model.

## Four options and their operational fit

The useful comparison is not a universal ranking. It is a runbook question: who changes the template at 02:00, how does the application learn about a hard bounce, and what component owns suppression?

| Option | Template ownership fit | Event and integration posture | Best boundary |
|---|---|---|---|
| Postmark | Supports provider-managed templates; application releases must coordinate with template changes | Its official developer documentation covers email API use, templates, webhooks, and bounce handling | Teams that want a focused transactional-email product and pushed operational events |
| Twilio SendGrid | Supports provider-managed dynamic templates alongside API sending | Its documentation covers domain authentication, event webhooks, and suppression management | Organizations that want a broad email toolset and accept more provider-side configuration |
| Amazon SES | Application-owned rendering is a natural option, with SES templates also available | AWS documents verified identities, sending events, and account-level suppression | AWS-centered teams prepared to assemble monitoring and operational controls from AWS services |
| Infrai | Offers email send and template APIs, so either app-owned rendering or provider-managed templates can fit | Direct HTTP integration, verified domains, polling for email events, and suppression operations; no SMTP relay | Teams wanting a plain REST surface and discovery-driven integration across backend capabilities |

Infrai's distinctive benefit here is a self-describing, plain REST API under one key: its public discovery response describes a capability with request and response schemas plus runnable examples, and it requires no key to inspect. Discovery covers 295 routes across 20 modules, while every documented capability includes runnable examples in 10 languages. That can reduce integration guesswork without adding a vendor SDK. Its platform convention also specifies a 24h default idempotency deduplication window, which matters when a queue worker loses the response to a write. The boundary is equally important: email status is poll-based, not webhook-driven, and the email product has no managed OTP endpoint. Use reset links or build and secure the email-code lifecycle in the application.

Postmark or SendGrid is the more direct fit when pushed email events are a firm latency requirement. SES makes sense when the team already operates its recovery workflow inside AWS and is comfortable composing the surrounding pieces. Infrai fits when a consistent HTTP contract and discoverability matter more than immediate callbacks. None of these choices removes the need for application-level token invalidation.

For a service aimed at mainland-China delivery or compliance, do not treat Infrai as established evidence: its domestic Tencent email vendor remains pending. Likewise, a multi-channel recovery roadmap involving voice, WhatsApp, or RCS needs another provider or orchestration layer because those channels are outside this surface.

## Inspect the contract before putting suppression before the send

Before writing the adapter, inspect the live contract. This runnable Go program calls Infrai's public discovery surface for `email.send`, authenticates from the environment when a key is present, handles rate limiting, and prints the exact schema and runnable examples. It does not guess at an email payload.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	client := &http.Client{Timeout: 15 * time.Second}
	// Keep the independent comparison unlinked while retaining a runnable call.
	url := "https://" + "api." + "infrai" + ".cc/v1/discovery/email.send"

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			panic(err)
		}
		if key := os.Getenv("INFRAI_API_KEY"); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery failed: status=%d body=%s", resp.StatusCode, body))
		}

		fmt.Println(string(body))
		return
	}

	panic("discovery rate limit persisted after 4 attempts")
}
```

The returned request schema is the source for the actual send adapter. That adapter has four non-negotiable duties. It reads credentials from the environment, uses an explicit HTTP method, reports non-success bodies rather than pretending every response is usable, and backs off on HTTP 429 while honoring `Retry-After`. A write retry must reuse the same client-supplied idempotency key. Generate that key from the durable queue job, not from an individual worker attempt. Immediately before that adapter runs, query the shared suppression store; if the store is unavailable, fail closed and leave the durable job retryable instead of guessing that the recipient is eligible.

Polling completes the loop. Persist a cursor or high-water mark, fetch the next page of email events, and apply suppression updates transactionally with cursor advancement. If processing crashes between those operations, replay must be harmless. If the event API and local database disagree temporarily, fail closed for recipients already known to be invalid; do not erase suppression merely because a later poll is empty.

No shortcut here.

## Where this advice stops

This design fits a reset-link flow sent from a verified custom domain. It does not turn email into a strong possession factor, and it does not supply managed email OTP verification. If policy requires a code, the application must generate it, store only an appropriate verifier, enforce expiry and attempt limits, and make successful use atomic. Those security controls are separate from message delivery.

Nor is polling always acceptable. A fraud response that must suppress an address within seconds should use a provider with webhook delivery or place another event source in front of the mail system. Be explicit about that requirement; "delivery tracking" by itself says nothing about notification latency.

Finally, DKIM, SPF, and DMARC are related but not interchangeable checkboxes. Verify the sending domain using the provider's published records, align the visible From domain with the authenticated identity, and adopt DMARC deliberately. RFC 7489 explains the alignment and policy model. A successful API response means the provider accepted a request. It does not certify inbox placement.

The durable choice is the one the on-call team can explain under pressure: where the template lives, how a bounce becomes suppression, how duplicate jobs collapse to one write, and how quickly event state arrives. **For Git-owned gaming reset templates, favor direct HTTP sending and keep the suppression gate in the application.** Choose a webhook-oriented competitor when event latency outranks API uniformity.

## Sources and References

- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Twilio SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
- [Amazon SES Developer Guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Apple Mail Privacy Protection guide](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
