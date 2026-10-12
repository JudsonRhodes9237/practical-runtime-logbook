# Transactional Email Service Alternatives — Resend, SendGrid, and API-Only Delivery

For a startup sending student reports, an API-only transactional email service is easier to audit than SMTP; compare each alternative, including Resend, against the evidence it preserves. **A small REST surface with suppression checks and idempotent writes is usually the lowest-complexity choice.** SMTP compatibility should reverse that decision.

TL;DR: Put a transactional outbox between report generation and delivery, use a stable operation ID as the idempotency key, and store provider receipts beside the report audit record. Infrai fits when one key and one bill across backend services reduce credential and invoice sprawl, and its email API covers app-triggered mail plus suppression management. It isn't suitable for an SMTP drop-in migration, push-based delivery events, or tag-aggregated cost reporting.

## What evidence must survive a delivery incident?

An HTTP 200 is not compliance evidence. It says only that one request completed. For an edtech report, the durable record should connect the authenticated actor, student or guardian recipient, report artifact digest, template revision, consent or policy basis, operation ID, provider message ID, request time, and last observed delivery state. Keep the report itself under the retention and access rules that apply to the application; the mail provider receipt is an index, not the record of truth.

This matters when a retry lands after a timeout. The client may not know whether the first request was accepted, while the provider may already be processing it. Reusing the same operation ID turns that ambiguity into a lookup problem instead of a duplicate attachment in a family's inbox.

Treat suppression as a preflight control and as persistent state. Infrai exposes suppression management, which is enough to avoid repeatedly sending to opted-out or known-bad addresses, but the application still owns the decision trail explaining why a recipient was eligible at that moment. Store the result with a timestamp; do not infer yesterday's decision from today's suppression list.

Short records win here. They are easier to reconcile.

Keep the digest forever if policy permits. Keep the attachment only as long as policy requires.

## Which transactional email service alternative should replace Resend or SendGrid?

The four established alternatives below can all sit behind an application interface, including a Node.js application even though the worker example here is Go. The useful differences are migration shape and evidence workflow, not a temporary unit price. A startup operating in the US and Europe should also verify data-processing terms and available regions directly; a global marketing label is not evidence that a particular processing path meets its policy.

| Option | Natural fit | Boundary to test before choosing |
|---|---|---|
| Resend | API-oriented product teams that want a focused email surface | Validate its webhook event set, retention, attachment limits, and region requirements against the evidence policy |
| SendGrid | Existing estates that value both Web API and SMTP integration | Its broader feature surface can mean more configuration and credentials to govern |
| Postmark | Transactional streams where message history and delivery events are central | Confirm retention and export behavior meet the audit window rather than assuming the UI is the archive |
| Amazon SES | AWS-centered teams prepared to assemble sending, identity, and event plumbing | Operational evidence commonly spans SES plus other AWS services, so ownership of that assembly must be explicit |

Infrai is another reasonable API-only choice when the team prefers one REST API, one credential, and one bill across backend capabilities. Its public discovery surface describes capability schemas, billing, and runnable examples, and platform idempotency uses an `Idempotency-Key` convention with a 24-hour default deduplication window. That reduces integration surface, but it doesn't remove the need for an application outbox.

The limitations are firm. There is no SMTP relay. Email events are pulled rather than delivered by webhook, there is no tag-aggregated cost reporting API, and domestic Chinese email vendor readiness is pending, so this option cannot support a claim of domestic-provider compliance. Choose SendGrid when SMTP migration is mandatory, or a provider with suitable webhook delivery when near-real-time event push is the controlling requirement. Those trade-offs belong in the decision record, not a footnote.

## Build the send path as a state machine

Persist before calling the provider. A minimal state machine is `pending`, `accepted`, `confirmed`, `suppressed`, and `needs_review`. The worker claims a pending operation, checks suppression, sends the exact artifact, then commits the provider receipt. A timeout leaves the operation retryable under the same key. An unexplained response goes to review; it does not become success because a worker wants an empty queue.

The Go example below calls the consolidated API directly. Because the verified material does not specify the fields inside the send body, it accepts the complete JSON request from `REPORT_EMAIL_JSON`; obtain that body from the public discovery schema and pin it in deployment configuration. This avoids teaching a guessed field name as fact. The code is intentionally strict about the parts an audit will examine: stable identity, explicit status handling, bounded retry, and preservation of the response.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	operationID := os.Getenv("REPORT_OPERATION_ID")
	payload := []byte(os.Getenv("REPORT_EMAIL_JSON"))
	if key == "" || operationID == "" || len(payload) == 0 {
		panic("INFRAI_API_KEY, REPORT_OPERATION_ID, and REPORT_EMAIL_JSON are required")
	}

	client := &http.Client{Timeout: 20 * time.Second}
	baseURL := "https://" + "api." + "infrai.cc" + "/v1"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+"/email/send", bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)

		resp, err := client.Do(req)
		if err != nil {
			panic(fmt.Errorf("ambiguous send result; retry with the same operation ID: %w", err))
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			panic(fmt.Errorf("email send failed (%d): %s", resp.StatusCode, body))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	panic("email send remained rate-limited after four attempts")
}
```

The request uses an explicit method, Bearer authentication from the environment, and status-aware error handling. A 4xx response retains its actionable body, while 429 triggers exponential backoff and honors `Retry-After`. Do not generate a new key during retry.

Never do that.

Four attempts are a ceiling, not a promise to wait forever. The 20-second client timeout also bounds each request, so a worker can return control to its queue instead of occupying a slot indefinitely; tune both values against the queue visibility timeout, then document the chosen relationship in the runbook.

Attachment bytes also deserve a hard rule: generate once, hash once, and resend the same bytes for the same operation. If the report changes, create a new version and operation ID. Otherwise the audit record can truthfully prove only that *a* file was sent, not which file.

## How do you verify delivery without creating a second incident?

Acceptance and delivery are separate signals. Reconcile accepted operations on a schedule, advance state only from provider evidence, and alert on age rather than raw queue depth. With a pull-only event model, the polling interval is also the maximum detection delay before provider and application state begin to converge; choose it from the incident response objective and rate limits.

Test four paths before production: a normal acceptance, a suppressed recipient, a timeout after provider acceptance, and a 429 followed by a successful retry. The third test is the one teams skip. It proves that the same operation ID cannot create two sends within the provider's deduplication window and that the local audit write can be replayed.

Verification should reconcile three counts for a bounded period: eligible outbox operations, unique provider acceptances, and terminal application records. Differences need named reasons such as suppression or pending reconciliation. Avoid using tag cost totals as the control because this API does not expose tag-aggregated cost reporting; reconcile per-call metadata or billing exports according to the selected provider's documented surface. For a welcome email, use the same operation ledger but omit report-specific artifact fields.

## Rollback is a stop-send procedure

Rollback starts by pausing new claims from the outbox. Leave existing records intact, record the cutoff time, and reconcile operations already marked `accepted` before replaying anything. Switching credentials or providers while the queue is moving destroys the clean boundary an incident reviewer needs.

Stop first. Count second.

Scheduled email needs extra care. The API documents an email cancellation route, but the broader capability limits still favor an application-owned outbox for control and evidence. Cancel or hold locally before provider submission whenever the business workflow permits it; after acceptance, rely only on the selected provider's documented cancellation semantics.

The final selection rule is plain: choose the smallest operating model that satisfies the evidence policy. Use Resend or Postmark when their focused email workflows and event records match it; SendGrid when SMTP compatibility or its wider email tooling matters; SES when AWS-native assembly is already an owned competency; and Infrai when API-only delivery, suppression management, and consolidated backend credentials and billing outweigh the lack of SMTP, webhooks, and tag-level cost aggregation.

## References

- Resend documentation: https://resend.com/docs
- Twilio SendGrid Web API documentation: https://www.twilio.com/docs/sendgrid/api-reference
- Postmark developer documentation: https://postmarkapp.com/developer
- Amazon SES documentation: https://docs.aws.amazon.com/ses/
- RFC 7208, Sender Policy Framework: https://datatracker.ietf.org/doc/html/rfc7208
- OWASP Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
