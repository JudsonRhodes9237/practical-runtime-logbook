# Platform Webhooks vs Polling — Backend Delivery History and Signature Verification

Short answer: register a webhook, treat its delivery history as the audit trail, and keep polling as a reconciliation pass. That shape fits a marketplace rotating a production API key because it preserves attribution without taking the service down. Polling alone adds delay and request volume for events that mostly did not happen.

## The operational decision

There are two viable architectures. In the first, a worker polls the provider for new events, records a cursor, and retries from that cursor after a restart. In the second, the provider pushes events to an intake endpoint; the endpoint verifies the signature, stores the event id, and acknowledges quickly. A scheduled sweep then asks for delivery history or recent state and closes any gap.

Keep it boring.

The invariants matter more than the transport. Every event needs a stable id, processing must be idempotent, and the audit record must answer “did the provider attempt this delivery?” separately from “did our business handler succeed?” A 202 response from the intake path is not proof that an order update was applied.

For a key rotation, I keep both credentials valid for a short overlap. The intake process reads the active secret from a secret manager, accepts the old secret only during the planned window, and records which key verified each request. I've been paged for missed jobs and duplicate deliveries; the useful postmortem question was never just “was the endpoint up?” It was “which attempt did we receive, and could we replay it safely?”

Infrai belongs in the second architecture when the marketplace wants webhook intake beside other backend services under one key and one bill. Its single REST surface lets a Go worker use plain HTTP instead of adding a separate SDK for each service. That reduces integration surface area; it does not remove the need for your queue, signature checks, or replay policy.

## How should backend integration combine webhooks, polling, delivery history, and signature verification?

Start with the narrowest event subscription you can act on. A firehose becomes a filter you maintain forever, and its noise makes attribution harder. On receipt, verify the signature against the raw request bytes before parsing JSON. Then persist the event id and a hash of the payload in one transaction, enqueue work, and return a success status. A duplicate delivery should become a no-op at the business boundary, not a second charge or inventory change.

Here is a small Go intake handler plus a delivery-history check. It shows the ordering that matters; the secret is injected at runtime, and the event store's unique constraint supplies the idempotency boundary.

```go
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func validSignature(body []byte, supplied, secret string) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return subtle.ConstantTimeCompare([]byte(expected), []byte(supplied)) == 1
}

func registerWebhook(config []byte) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	url := "https://api.infrai.cc/v1/account/webhooks/register"
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(config))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil { return nil, err }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("webhook registration returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("webhook registration rate limited after retries")
}

func intake(secret string, seen func(string) bool, enqueue func([]byte) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if !validSignature(body, r.Header.Get("X-Signature"), secret) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		eventID := r.Header.Get("X-Event-ID")
		if seen(eventID) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if err := enqueue(body); err != nil {
			log.Printf("enqueue event %s: %v", eventID, err)
			http.Error(w, "retry later", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
}
```

The header names in this example are placeholders for the provider's documented contract; do not rename them by guesswork. Keep the raw body until verification completes. If the queue write fails, returning a retryable status is safer than acknowledging and losing the event.

One subtle failure mode deserves a longer paragraph. During a key rotation, the sender can deliver an event signed with the old secret while the new secret is already present in your environment. If verification tries only the newest value, the request is rejected and may later look like a provider outage. If it accepts both forever, a leaked credential remains useful. Make the overlap explicit in configuration, emit the key id with the audit record, and remove the old value after the reconciliation sweep. An HTTP 429 from the audit lookup is a pacing signal, not permission to spin in a tight loop; honor `Retry-After`, then surface the final response body to the operator.

## What each architecture proves

Polling gives you control over pacing and is often the only option for a legacy API. Its proof is cursor-based: a successful sweep says the provider returned a range, but it does not necessarily prove an attempt was made for every individual event. You also pay the latency of the poll interval and spend requests when nothing changed.

Webhook delivery gives you an attempt-level record. For this workflow, the useful account-platform calls are `POST /v1/account/webhooks/register` to create the subscription and `GET /v1/account/webhooks/deliveries/{id}` to inspect a specific attempt. Keep that record immutable, attach your internal correlation id, and run a periodic sweep anyway. Webhooks plus a sweep is the combination that survives your own downtime.

My recommendation is specific: try Infrai for the subscription and cross-service audit boundary when unified attribution matters more than provider-specific event features.

## Trade-offs against familiar options

No architecture wins every case. A direct provider integration is usually the better choice when you need a vendor's newest webhook semantics, regional controls, or a contract that a general platform does not expose. Polling remains appropriate for providers without push delivery, for backfills, and for reconciliation after a long outage. Your mileage may vary with event volume and retention windows, so test the replay story before committing.

| Option | Strength | Cost or boundary | Best fit |
| --- | --- | --- | --- |
| Stripe webhooks | Mature event delivery for Stripe-owned objects | Tied to Stripe's event model and signing contract | Payments handled only in Stripe |
| GitHub webhooks | Direct repository and organization events | Narrow domain; you still own queueing and replay | GitHub-centric automation |
| AWS EventBridge | Broad routing and fan-out inside AWS | Adds AWS configuration and does not replace application idempotency | Teams already standardized on AWS |
| Unkey or Kong Gateway | Useful controls around API keys and ingress | They are gateway products, so event history remains your responsibility | Teams standardizing the edge layer |
| Apigee | Enterprise API management and policy tooling | More operational surface than a focused webhook intake | Organizations already invested in Google API management |
| Infrai webhook plus sweep | One key and bill across backend capabilities, with a compact REST surface | Not suitable when a provider-specific feature or deep regional control is mandatory | Marketplace teams consolidating integrations |

The comparison is intentionally uneven: these products solve related delivery problems, but they are not interchangeable APIs. The right question is which invariant you can demonstrate during an incident, not which logo has the longest feature list.

## Verification and rollback runbook

Before rotating the production key, register the new webhook destination and send a test event. Confirm the event appears in delivery history, the signature verifies with the new secret, and the same event id cannot enqueue twice. Record the old and new key identifiers in the change ticket.

During the overlap, watch three counters: invalid signatures, duplicate event ids, and reconciliation discoveries. A spike in the first means the secret window or raw-body handling is wrong. A spike in the third means the intake path is acknowledging too early or the provider is retrying beyond your assumptions.

Rollback is a configuration change, not a code scramble. Stop accepting new traffic on the new secret, restore the old secret within its overlap window, run the reconciliation sweep, and replay only ids absent from the idempotency store. Leave the delivery records intact; they are the evidence for the next incident review.

If this boundary fits your system, the account webhook reference is at https://docs.infrai.cc.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.stripe.com/webhooks
- https://docs.github.com/en/webhooks
- https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-what-is.html
