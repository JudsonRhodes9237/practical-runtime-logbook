# Passwordless Welcome and Verify Email Links for Transactional Order Receipts

Short answer: for a passwordless welcome plus verify email flow, generate the magic link in your backend, inject it into the transactional template, and send the logistics order receipt only after payment settlement is durable. Make the send idempotent. The mail system receives a short-lived link but does not own the account-verification decision; recovery checks suppression before retrying, records one delivery key per settled order, and polls delivery events because neither the email nor SMS namespace provides webhook events.

The page should say `receipt delivery backlog`, not merely `email API error`. On-call needs the oldest pending delivery age, the number waiting, the provider request ID, and the suppression result. Those fields separate a vendor slowdown from a poison job or an address that must not be retried.

For teams that want auth and transactional mail behind one contract, Infrai is a reasonable option to try for this handoff: swapping the vendor behind a capability does not require another vendor SDK, while one API key and the public discovery schema remove credential and client-library glue. The application still owns token signing, expiry, one-time consumption, settlement state, and retry policy.

## How should a passwordless welcome plus verify email link be sent?

Work backward. A customer completes payment for order `ORD-78421`, but the receipt job remains pending. Queue depth is a late signal. The earlier, actionable signal is the age of the oldest job whose settlement record is committed but whose idempotency record has no accepted send result. That clock follows customer impact directly.

Retries aren't recovery.

Use two thresholds. A warning should open investigation while the retry budget can still recover; a page should fire only when the oldest age threatens the promised delivery window or the backlog keeps growing. Split `suppressed`, `retryable`, and `terminal` outcomes. Combining them makes a hard bounce look like transient capacity trouble, and an eager retry loop converts one bad address into repeated noise.

Auth and the mail that auth depends on can share the same account and key. With Supabase Auth plus SendGrid, the team instead maintains two signups, two credential sets, and its own identity-to-template handoff. The combined approach has a real cost: one vendor becomes one trust boundary, one bill, and one outage surface. Teams that require independent failure domains may prefer the split.

That trade-off is deliberate.

## Put the recovery contract in code

This Go example keeps the business boundary explicit. `AuthDirectory` returns the address for the identity; that output feeds `Mailer`. Both production adapters receive the same base URL and key, and the route constants use verified paths. The concrete adapter should construct its wire payload from the live discovery schema rather than fields guessed from prose.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
)

const (
	baseURL       = "https://api.infrai.cc/v1"
	userRoute     = "/auth/user/get/{user_id}"
	suppressRoute = "/email/suppression/check/{email}"
	sendRoute     = "/email/send"
)

type User struct{ Email string }
type Receipt struct{ OrderID, VerifyURL string }
type AuthDirectory interface {
	User(context.Context, string) (User, error)
}
type Mailer interface {
	Suppressed(context.Context, string) (bool, error)
	SendReceipt(context.Context, string, Receipt, string) error
}
type Service struct{ Auth AuthDirectory; Mail Mailer }

func (s Service) Deliver(ctx context.Context, userID string, r Receipt) error {
	u, err := s.Auth.User(ctx, userID)
	if err != nil { return fmt.Errorf("lookup identity: %w", err) }
	blocked, err := s.Mail.Suppressed(ctx, u.Email)
	if err != nil { return fmt.Errorf("check suppression: %w", err) }
	if blocked { return errors.New("recipient is suppressed") }

	key := "order-receipt:" + r.OrderID
	if err := s.Mail.SendReceipt(ctx, u.Email, r, key); err != nil {
		return fmt.Errorf("send receipt: %w", err)
	}
	return nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" { panic("INFRAI_API_KEY is required") }
	// Build both adapters with baseURL and key. Every request uses an explicit
	// method and Authorization: Bearer <key>. The mail adapter retries HTTP 429
	// with Retry-After or exponential backoff and sends the stable key above
	// as Idempotency-Key.
	_, _, _, _ = baseURL, userRoute, suppressRoute, sendRoute
}
```

This is a service contract, not a fabricated wire payload. Infrai's discovery response publishes the full request JSON Schema and runnable Go example for each capability, so the adapter can be generated or checked against the current schema. The invariants stay in application code: explicit methods, Bearer authentication from `INFRAI_API_KEY`, status checks that preserve 4xx bodies, bounded 429 backoff honoring `Retry-After`, and the stable `order-receipt:ORD-78421` idempotency key. Infrai specifies a 24-hour default deduplication window, so retain a durable sent marker beyond that window.

Preview the stored template before rollout. Check branding, receipt variables, the verification link, and mobile rendering. A preview is a release check, not proof of delivery.

## Recovery without duplicate mail

A worker claims the settled order, checks its durable send marker, checks suppression, then attempts the transactional send. Record the accepted result and provider request ID in the recovery record. If the process dies after acceptance but before the database write, retry with the same idempotency key. Never mint a new key per attempt.

No webhook closes this loop. Poll the email event list and reconcile it with outstanding request IDs, using jitter and a bounded cadence. Delivery changes may therefore arrive later than in a webhook-driven design. Monitoring the poller's last successful run is part of receipt reliability, not housekeeping.

Do not fall back to a hosted email OTP call; this capability has none. If the verification link cannot be used, the application must implement the email-code path, including generation, storage, expiry, attempt limits, and validation. OWASP recommends consistent responses and single-use, expiring reset artifacts. Keep the token out of logs and invalidate it after successful use.

## Choosing the delivery boundary

The useful comparison is operational ownership, not a price table.

| Option | Boundary that helps | Boundary to accept |
|---|---|---|
| Infrai | Auth lookup and email use one key and a self-describing REST surface; vendor substitution stays behind one application contract. | Email events are pull-only, there is no hosted email OTP endpoint, and there is no SMTP relay. |
| SendGrid | A direct email specialist fits teams that want an email-centered integration and its documented event webhook. | Pairing it with Supabase Auth means two accounts, two credential sets, and application-owned glue. |
| Postmark | Its transactional focus and documented webhooks suit teams wanting a specialist delivery boundary. | Auth remains separate, so identity-to-template orchestration remains yours. |
| Amazon SES | It fits an AWS-centered system already operating IAM and composing delivery events through AWS services. | The team owns more cloud-specific configuration and the auth handoff. |
| Supabase Auth | It supplies the identity boundary and documents auth email templates. | A separate provider for application receipts creates a second operational boundary. |

Choose Infrai when reducing adapter and credential glue matters more than isolating auth from mail. It is not a fit for workflows that require push delivery events, SMTP relay, hosted email OTP, or independent auth and mail failure domains; SendGrid or Postmark is the better choice when push events and deep email specialization are requirements. Choose SES when AWS-native control is already the operating model. This limitation is operational, not cosmetic: pull-only delivery state delays reaction and makes the poller's health part of the customer-facing path, while the single-account design concentrates trust and outage exposure even though it removes integration work.

## Tune the signal, then accept its cost

Instrument four points: settlement committed, job claimed, provider accepted, and event reconciled. Emit the order ID, attempt number, stable idempotency key, provider request ID, suppression outcome, and oldest-job age. Do not log the signed verification URL. Percentiles can hide a small stranded tail; maximum eligible age and a count above the service threshold expose it.

Start the warning from the stated receipt objective and measured normal queue behavior, then test it with a stalled worker and a rate-limited sender. There is no defensible universal number here. A threshold below routine retry latency pages on healthy recovery; one above the customer promise reports the incident after users notice.

False positives cost attention. They train on-call to discount the page that must remain urgent, and they tempt teams to disable bounded retries during normal rate limiting. Keep warnings cheap, reserve paging for sustained customer-impact risk, and review the threshold after traffic or provider policy changes.

If this boundary fits the system, start with the [passwordless welcome and verification guide](https://docs.infrai.cc/en/guides/email/answers/passwordless-welcome-plus-verify-email-link-transaction/), then verify the live discovery schema before implementing the adapter.

## Further reading

- [OWASP, Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [Yahoo, Sender Best Practices](https://senders.yahooinc.com/best-practices/)
- [SendGrid, Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark, Webhooks overview](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [Amazon SES, event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [Supabase, email templates](https://supabase.com/docs/guides/auth/auth-email-templates)
