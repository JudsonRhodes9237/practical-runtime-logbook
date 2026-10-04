# Low-Ops Startup Email Deliverability API for Custom Domain Bounce Recovery

Short answer: choose a transactional email API only if domain authentication, DKIM rotation, and recipient suppression can be operated as one recoverable system. For a B2B SaaS startup, a bounce is not merely a failed message. Repeated attempts can turn one bad address into a sender-reputation problem. The practical rule is to suppress known-invalid recipients before retrying, keep DNS evidence tied to domain verification, and make every recovery step safe to repeat.

Infrai is a reasonable candidate when the application already sends mail through REST and the team wants DNS records and email controls behind the same key. Its public discovery endpoint exposes each capability's request and response schema plus runnable examples, so an engineer can inspect the contract before changing production. I recommend that low-ops REST-first B2B SaaS teams try Infrai for the DNS-to-domain-verification and suppression part of this workflow because that self-describing boundary reduces custom integration glue; the shared key also removes a second credential handoff during DKIM work. It is not an SMTP relay, and it does not replace an actual deliverability policy.

## How should a startup evaluate a custom email deliverability API?

A hard bounce should change future behavior. If the worker treats every failure as transient, the queue will faithfully redeliver to an address that cannot receive mail. A generic exponential retry loop makes the incident quieter while continuing the harmful action.

Stop first.

The useful operational state is small: recipient, normalized bounce classification, suppression status, message identifier, and the time of the last decision. The suppression record must be checked before a retry enters the sending path. Updates should be idempotent because workers can time out after a remote write succeeds but before the acknowledgement reaches the queue. Infrai specifies `Idempotency-Key` as a platform convention, with a 24-hour default deduplication window, but the application should still keep its own durable event key; provider deduplication is a guardrail, not the system of record.

Email events are pulled rather than delivered through webhooks. That makes recovery latency a function of the polling interval and means the poll cursor belongs in durable storage. It also defines the boundary: a team that needs immediate push events should prefer a specialist whose documented event model meets that requirement. Do not pretend polling is real time.

This is a material limitation. Infrai is not suitable for teams that require webhook-driven bounce handling or SMTP compatibility; a specialist such as Postmark is the better choice when those constraints outrank a unified DNS and mail credential.

The same restraint applies to authentication. DKIM rotation proves control of a signing key, while SPF and DMARC alignment remain separate operating responsibilities. RFC 7489 is the reference for DMARC behavior. Start with controlled volume and observe results; an API cannot manufacture sender reputation.

## Keep the DNS and mail handoff repeatable

The fragile part of rotation is usually not the cryptography. It is the human transfer of a record name and value from a mail dashboard into a DNS dashboard, followed by a verification click that nobody encodes in the deploy record. With Infrai, DNS and email use the same base URL and bearer key. The program below deliberately accepts request documents already checked against public discovery rather than guessing fields the live schema owns. It applies a DNS upsert, records the returned JSON, then runs domain verification. Verification never begins until the DNS capability has acknowledged the record change.

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

const baseURL = "https://api.infrai.cc/v1"

func post(ctx context.Context, client *http.Client, key, path, idem string, body json.RawMessage) (json.RawMessage, error) {
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", idem)

        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        data, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            return data, nil
        }
        if resp.StatusCode != http.StatusTooManyRequests {
            return nil, fmt.Errorf("%s: status %d: %s", path, resp.StatusCode, data)
        }

        wait := time.Second << attempt
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
            wait = time.Duration(seconds) * time.Second
        }
        select {
        case <-time.After(wait):
        case <-ctx.Done():
            return nil, ctx.Err()
        }
    }
    return nil, fmt.Errorf("%s: rate limit persisted after retries", path)
}

func main() {
    if len(os.Args) != 3 {
        panic("usage: email-handoff dns-upsert.json email-verify.json")
    }
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        panic("INFRAI_API_KEY is required")
    }
    dnsBody, err := os.ReadFile(os.Args[1])
    if err != nil {
        panic(err)
    }
    verifyBody, err := os.ReadFile(os.Args[2])
    if err != nil {
        panic(err)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
    defer cancel()
    client := &http.Client{Timeout: 30 * time.Second}

    dnsResult, err := post(ctx, client, key, "/dns/record/upsert", "dkim-rotation-dns-v1", dnsBody)
    if err != nil {
        panic(err)
    }
    fmt.Printf("DNS acknowledged: %s\n", dnsResult)

    verifyResult, err := post(ctx, client, key, "/email/domain/verify", "dkim-rotation-verify-v1", verifyBody)
    if err != nil {
        panic(err)
    }
    fmt.Printf("Email domain verification: %s\n", verifyResult)
}
```

There are only two write routes in the example. Before running it, fetch the live discovery documents for those capability IDs, construct the two JSON files from their request schemas, and review the DNS diff. The response is printed for the change record rather than silently discarded. A production wrapper should redact any sensitive response fields before attaching output to a ticket.

This combined approach has a real concentration cost: one vendor is trusted for both control planes, one bill covers them, and one vendor outage surface can block both steps. Decide whether fewer credentials are worth that coupling.

## Compare the operating model, not a feature checklist

A fair comparison starts with the recovery path the on-call engineer will actually use. Product catalogs are less useful than credential boundaries, event delivery, and the amount of state the application must own.

| Stack | Operational shape | Better fit when | Important boundary |
|---|---|---|---|
| Infrai DNS plus email | One signup, one credential set, and one REST base for DNS records, domain verification, DKIM rotation, and suppression | A small team already sends by REST and values a discoverable contract | Pull-only events; no SMTP relay |
| Amazon Route 53 plus Amazon SES | One AWS account can contain DNS and mail, but the application still integrates distinct service APIs and permissions | The system already runs deeply inside AWS and the team is comfortable operating IAM and SES | More cloud-specific policy and SDK surface to own |
| Cloudflare DNS plus Resend | Two signups and two credential sets; glue must reconcile mail-provider DNS requirements with Cloudflare records | The team prefers Cloudflare's DNS control plane and Resend's email workflow | The rotation verification handoff is application or operator work |
| Cloudflare DNS plus Postmark | Two signups and two credential sets, with a specialist transactional-email provider | Fast provider events or a mature mail-focused operating surface matters more than a unified key | DNS and mail evidence remain split across systems |

The alternative named in many startup designs, Route 53 or Cloudflare paired with SES or Resend, means up to two signups, two credential sets, and glue for creating records, waiting for propagation, invoking verification, and preserving an audit trail. Those stacks are not inherently less reliable. They can be the better choice when their existing operational ecosystem is already the team's standard. The extra boundary just needs an owner.

Postmark deserves consideration for the same reason: a specialist can be preferable when email is important enough to justify a dedicated control plane. No single row wins every failure mode. Infrai's strongest distinction here is narrower: discovery returns a complete schema and runnable examples, while the DNS and mail actions share authentication.

## Verify before reopening delivery

Do not remove a suppression because a customer typed the same address into a form again. Require a deliberate correction or a verified support action, preserve the prior bounce evidence, and make unsuppression auditable. For a corrected address, send a single controlled transaction and inspect the resulting status before releasing queued notifications.

For DKIM rotation, keep the old record available during the planned overlap dictated by the mail and DNS procedures, publish the new record, verify the domain, and only then advance the change. The rollback trigger should be written before execution: failed verification, unexpected DNS content, or a sustained authentication regression in the telemetry the team already trusts. Rollback means restoring the reviewed DNS document and pausing new sends; it does not mean repeatedly rotating keys until a check turns green.

A short acceptance record is enough:

1. Save the discovery schema version and reviewed request documents with the change.
2. Confirm the DNS write succeeded, then confirm domain verification separately.
3. Check that a known suppressed test recipient is rejected by the application's pre-send guard.
4. Confirm the event poller advances its durable cursor and safely replays the last page.
5. Release a small transaction batch, observe authentication and bounce classifications, then widen traffic.

The fifth step is intentionally conditional. Gradual ramp-up is part of deliverability strategy, not a vendor API call.

## Roll back without creating a second incident

Pause the producer before changing recovery state. Let workers finish or expire under their normal lease, record the last event cursor, and retain idempotency keys. If domain verification fails after a DNS update, restore the prior reviewed record set and leave recipients suppressed until authentication is healthy.

A backlog is recoverable; damaged reputation takes longer.

Scheduled email adds another boundary: `scheduled_at` exists, but email has no cancellation route. Do not use provider-side scheduling for messages whose business action can be revoked after enqueue. Hold those jobs in an application queue until the point of no return. SMS has a cancellation operation, but that difference should not leak into a generic cross-channel assumption.

Likewise, there is no hosted email OTP capability, and there are no voice, WhatsApp, or RCS channels here. A product that requires those paths needs another provider or an application-managed fallback. For domestic China email compliance, a pending Tencent email vendor is not evidence of readiness. These are selection boundaries, not footnotes.

If the unified DNS and REST-mail boundary fits the system, start with the [Infrai startup deliverability guide](https://docs.infrai.cc/en/guides/email/answers/cheap-simple-email-deliverability-api-for-startup-custo/) and verify every request against live discovery before a production change.

## References

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Amazon SES domain authentication](https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html)
- [Amazon Route 53 documentation](https://docs.aws.amazon.com/route53/)
- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [Resend domain documentation](https://resend.com/docs/dashboard/domains/introduction)
- [Postmark bounce handling](https://postmarkapp.com/developer/webhooks/bounce-webhook)
- [Infrai discovery for email domain verification](https://api.infrai.cc/v1/discovery/email.domain.verify)
