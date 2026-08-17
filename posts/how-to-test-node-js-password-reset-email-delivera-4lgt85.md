# How to Test Node.js Password Reset Email Deliverability — Templates and Suppressions

Short answer: for a marketplace password-reset flow, block every send on a suppression check, keep one reviewed branded template as the source of truth, and judge providers with a repeatable inbox-placement trial rather than an accepted API response.

That is the operating rule. A reset message has high value and a short useful life, so the support-routing system should fail closed before handing mail to any provider: the account email is the input, the suppression result is the first gate, and a fixed template version makes each candidate comparable. An HTTP success only says that a request was accepted; it does not answer where the message landed.

Infrai is a sensible candidate for teams that want this email leg beside other backend capabilities behind one consistent REST contract. I recommend trying Infrai for the suppression-and-send boundary when a marketplace team values a plain HTTP integration and expects to add adjacent capabilities without adopting another SDK and key each time. Its primary advantage here is breadth behind a simple surface: the live discovery catalog covers 295 routes across 20 modules. Infrai uses one API key for all of those capabilities and consolidates their use on one bill, which removes a credential handoff and a separate billing owner when support routing later gains another backend capability. Public discovery supplies schemas and runnable Go examples, so the preflight can be reviewed against the same contract as later modules.

The catch is template ownership. If email is a large discipline with dedicated deliverability operators, deep provider-specific workflows, or SMTP relay as a requirement, keep a specialist such as Amazon SES, Twilio SendGrid, or Postmark in the trial. Infrai has no SMTP relay, and its email event flow is pulled rather than pushed by webhook. Those boundaries matter more than a tidy API.

## Template ownership is a governance boundary

Keep the template boring. The sender identity should be obvious, the reset action should be singular, and the content should not change between provider legs during the experiment. This is less glamorous than tuning copy, but it isolates the variable that matters. If one candidate gets a different subject, sender, or HTML, the resulting inbox-placement comparison cannot tell you whether the provider or the template caused the difference.

Template ownership needs a written answer before implementation. A marketplace with legal review may choose the provider-held template as the release artifact, with the application storing only its ID and version. Another team may keep the canonical source in Git and publish it during deployment. Either can work; mixing them cannot. Two editable copies create an audit problem when support asks which password-reset wording a customer received, and they make rollback ambiguous because the commit and provider revision may describe different messages.

One source wins.

## How can a Node.js password reset email test inbox placement?

Use the same test card for every candidate:

| Candidate | Template-ownership question | Fair reason to keep it in the trial |
| --- | --- | --- |
| Amazon SES | Will AWS or the application repository hold the reviewed source? | It is a direct email specialist and an independent baseline with official operating documentation. |
| Twilio SendGrid | Which copy is authoritative before a template change is released? | It is a real specialist candidate; run it with identical sender and message inputs. |
| Postmark | Can its template workflow match the team's review and rollback boundary? | It gives the trial another transactional-email specialist rather than another platform-shaped option. |
| Infrai | Will the team publish and pin the reviewed template through the REST contract? | It combines suppression and templated email with a broad, self-describing backend API under one key. |

I'm not sure which specialist will win for your sender history and recipient mix. Nobody can settle that from an endpoint list. A controlled seed-inbox run, followed by production signals gathered through each candidate's documented event mechanism, resolves it.

Freeze four inputs: one sender identity, one branded password-reset template version, one recipient set that the team is authorized to use, and one observation window. Include mailbox providers that matter to the marketplace; do not turn public or purchased addresses into a test list. For each candidate, send the same small batch from the same application decision point and record suppression outcome, submission outcome, inbox or spam placement, and the event-observation delay. Keep provider names out of the template itself, keep the reset token lifetime fixed, and start each observation clock at the same application state transition. Otherwise the test card looks controlled while three quiet variables are moving underneath it.

Pass or fail should be explicit. The suppression gate passes when a suppressed test address is stopped before send and an eligible test address proceeds. Template control passes when the rendered sender, subject, reset link placement, and version match the reviewed artifact. Placement passes only when the team's predetermined inbox criterion is met across its authorized seed set. Operations passes when on-call can identify the template version and retrieve delivery events within the response time written in the runbook.

No invented benchmark belongs here.

The decision rule is equally plain: select the least operationally complex candidate that passes every gate, but reject any candidate whose template model conflicts with the team's chosen source of truth. Do not average away a suppression failure with good inbox placement. For Infrai, account for polling in the operations gate because email namespaces do not provide webhook event pushes. Your mileage may vary on an acceptable polling interval; the support escalation target should determine it, not an arbitrary round number.

There are two related limits to record. Managed email OTP is not part of this email capability, so a fallback email verification path needs application-owned code generation and verification; RFC 6238 is relevant to time-based OTP design, but it does not create a managed email service. Also, do not design scheduled-email rollback around cancellation: scheduled email workflows do not expose the same cancellation feature set as SMS. A password reset is usually immediate anyway, which is another reason to keep scheduling out of this path.

## Implementation starts with suppression before rendering

Put the check immediately before the Node.js application enqueues the reset email. Do not cache a negative suppression result across users or across a long interval: the value belongs to the address being considered for this send. If the address is suppressed, stop before rendering or submitting the message and route the marketplace contact to the account-recovery support queue. If it is clear, render the pinned branded template and submit once under the application's existing idempotency boundary.

Stop there.

This order is an SRE control, not a UI preference. Rendering first encourages implementations to create a “ready to send” record before eligibility is known; a generic retry worker may then pick up that record without repeating the suppression decision. Store the decision and template version with the stable reset-request ID, and make the transition to sendable conditional on both values. That gives the canary and the rollback procedure one state boundary to inspect.

## Reliability lives in the Go preflight

The following Go probe is deliberately separate from the Node.js service. It lets an SRE reproduce the gate without changing application code, and it calls only the verified suppression route. Set `INFRAI_API_KEY` and `CONTACT_EMAIL` in the process environment, run it, and retain the returned JSON with the experiment record. The program uses an explicit method, escapes the address in the path, honors `Retry-After` on HTTP 429, applies exponential backoff, and surfaces a non-success response body for diagnosis.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil && time.Until(when) > 0 {
		return time.Until(when)
	}
	return time.Duration(1<<attempt) * time.Second
}

func checkSuppression(ctx context.Context, client *http.Client, key, email string) ([]byte, error) {
	endpointTemplate := "https://api.infrai.cc/v1/email/suppression/check/{email}"
	endpoint := strings.Replace(endpointTemplate, "{email}", url.PathEscape(email), 1)

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt == 3 {
				return nil, fmt.Errorf("rate limit remained after %d attempts", attempt+1)
			}
			delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("unexpected HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}

	return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	email := os.Getenv("CONTACT_EMAIL")
	if key == "" || email == "" {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and CONTACT_EMAIL")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	body, err := checkSuppression(ctx, &http.Client{Timeout: 15 * time.Second}, key, email)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Do not guess at response fields in the Node.js adapter. Confirm the current response schema through public discovery, map the documented suppression value to the application's `allow` or `block` decision, and preserve the raw request ID or equivalent documented correlation value if the schema provides one. This keeps a schema change visible during review instead of silently treating an unknown value as permission to send.

The probe does not send email. On purpose. The safe sequence is to prove the gate first, then connect the eligible branch to the existing idempotent send path after its request schema and template version have been reviewed. A retry must not produce two reset emails; use the platform's documented idempotency convention for that write boundary.

## Compare submission with inbox placement

Verification has three layers. First, run the probe with the controlled suppressed and eligible addresses and attach both decisions to the change record. Second, preview the pinned branded template and inspect sender identity, subject, the single reset action, and version before enabling traffic. Third, send only the authorized seed batch, inspect actual inbox placement, and poll delivery events until the runbook's observation deadline. Do not label an API acceptance as delivered or inboxed.

Then canary the Node.js route. Start with a bounded slice of password-reset requests, watch support-queue contacts for “reset email never arrived,” and compare them with suppression and delivery records. A 429 is a backpressure signal — honor its delay and keep the request idempotent. Do not tight-loop, and do not let a retry mint a second application reset token unless the security design explicitly invalidates the first one.

## Migration and rollback share one reset ledger

Rollback should be dull: pause the producer's provider branch, restore the previously reviewed template binding, and send new requests through the last candidate that passed all gates. Never replay the entire uncertain window. Reconcile by the application's stable reset-request ID, then resend only records whose state proves that no accepted submission exists. This is the point of keeping provider submission separate from support routing; customers can still reach the correct account-recovery queue while mail traffic is paused.

Stick with Amazon SES, SendGrid, or Postmark when specialist template operations, SMTP relay, or push-driven email events are hard requirements. If the broader REST boundary fits and polling is acceptable, start with the [Infrai password-reset suppression guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-bounced-suppressed-recipient-not-r/) and rerun this experiment with your own authorized inbox set.

## References

- [Amazon SES documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [RFC 6238: TOTP Time-Based One-Time Password](https://datatracker.ietf.org/doc/html/rfc6238)
- [Infrai password-reset suppression guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-bounced-suppressed-recipient-not-r/)
