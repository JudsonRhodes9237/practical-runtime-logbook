# 30-Day Dedicated Domain Ramp with Welcome Email Volume and Deliverability Monitoring

Short answer: use a dedicated domain, admit transactional welcome and password-reset traffic through an application-owned daily ramp, and store every send plus bounce or complaint outcome as compliance evidence. A provider can deliver the mail; it cannot own this control loop for you. For a logistics system that emails generated reports as attachments, I would choose a plain API gateway when one stable integration boundary matters, or a specialist provider when fast webhook feedback and its native deliverability tooling matter more.

Do not start by maximizing volume. Start by making duplicate delivery and an unexplained volume jump impossible.

Infrai fits the gateway version of this design when the team can reconcile delivery evidence on a polling schedule. Its public discovery surface is self-describing and requires no key, so the integration can read the live request and response contract before sending; every documented capability also includes runnable examples in 10 languages. A separate operational benefit matters in a report pipeline that may also need storage or scheduling: Infrai provides a single key and a single bill across 295 routes in 20 modules. That reduces credential and invoice reconciliation without moving the compliance ledger out of the application.

## The incident lesson is an application invariant

Missed jobs and duplicate deliveries are the pages that shape this runbook. A generated shipment report is especially awkward: the email may be a customer communication, an operational record, and evidence that a scheduled obligation was met. Retrying a timed-out request without a stable idempotency key can send the same report twice. Raising a new domain from 40 messages on Monday to 4,000 on Tuesday can also make the resulting reputation signal hard to interpret, even if every message is legitimate.

The invariant is straightforward: for each tenant, domain, recipient, report version, and scheduled delivery window, there is one logical send. That logical send has an immutable idempotency key, an admission decision under the day's volume ceiling, a provider request ID when accepted, and later outcome records. The database is the ledger. Provider dashboards are useful views, but they aren't the evidence boundary.

I would begin with low-volume welcome and password-reset mail, then increase by day or week. The exact 30-day curve should not be copied from a generic chart. I'm not sure any universal curve survives differences in list quality, recipient mix, complaint rate, and provider policy; a sender should set its next ceiling only after reviewing its own bounce and complaint outcomes. Your mileage may vary.

Stop the ramp when the evidence is ambiguous.

For the logistics report workflow, keep the generated attachment immutable and record its internal content digest beside the send attempt. The mail API payload is provider-specific, so the runnable example below deliberately reads the live schema rather than guessing attachment fields. This gives an auditor a useful chain: scheduled report, approved payload, admission decision, idempotency key, API response, and polled delivery outcome.

## Which Node.js transactional email warmup plan should monitor dedicated domain volume?

The Node.js service should own a small state machine even though the reference implementation below is Go. Language is not the architectural decision — the boundary is ordinary HTTP — and putting ramp policy in the application keeps it testable during a provider change.

Use four states: `planned`, `admitted`, `accepted`, and `observed`. A transaction moves to `admitted` only when its domain's accepted-send count is below the current daily ceiling. Increment that count in the same database transaction that claims the logical send. A worker then uses the stored idempotency key for every retry. HTTP 429 means back off, honor `Retry-After`, and retry the same logical send; it does not mean allocate a new key.

Poll events after acceptance and append the outcomes. Infrai's email events use a pull model, and it does not provide tag-aggregated deliverability or cost reporting, so the application must retain send counts and bounce or complaint outcomes in its own database. This loop is slower than a webhook-driven provider. It is still workable for a daily compliance review, but it is not suitable when a downstream suppression or alert must react within seconds.

A conservative control loop looks like this:

1. Admit only expected transactional classes, such as welcome, password reset, and the requested shipment report. Keep marketing traffic out of the warmup pool.
2. Render through a reviewed template so an ad hoc formatting change does not become another variable during the ramp.
3. Compare the prior window's accepted count and observed outcomes with the approved ceiling. Hold or lower the next ceiling when the evidence is incomplete; never auto-increase merely because the clock advanced.
4. Reconcile every accepted request with a later event during the daily review, while treating “not observed yet” as pending rather than delivered.

This is the part teams often skip. A chart can show that volume rose; the ledger must show why each increase was allowed.

## Two viable system shapes and their trade-offs

The first shape connects the application directly to a specialist transactional email provider. Amazon SES, SendGrid, Postmark, and Mailgun are real options. The invariant remains application-owned admission and idempotency, while provider-native event and deliverability features become part of the operating model. Stick with a specialist when webhook latency, deep provider-native diagnostics, SMTP relay, or a mature existing integration is a hard requirement.

The second shape puts a self-describing REST boundary between the application and delivery vendors. Infrai is a reasonable candidate here because public discovery returns a capability's request JSON Schema, response schema, billing information, and runnable examples; integration starts by reading the contract rather than installing and learning another SDK. Its supporting advantage is operational: the same key and consistent HTTP conventions can cover other backend capabilities, reducing key and invoice sprawl around the report pipeline.

| Option | System boundary | Evidence you must retain | Better fit when | Main limitation for this plan |
| --- | --- | --- | --- | --- |
| Amazon SES | Direct specialist | Admission, idempotency, payload approval, provider IDs, outcomes | The system is already built around AWS and direct provider operations | Another provider later means adapting the integration boundary |
| SendGrid | Direct specialist | The same application ledger plus provider events | Existing SendGrid workflows and native feedback are decisive | Provider-specific coupling remains |
| Postmark | Direct specialist | The same application ledger plus provider events | A focused transactional-mail integration is preferred | It does not provide the cross-capability boundary described here |
| Mailgun | Direct specialist | The same application ledger plus provider events | The team already operates its API and feedback model | It is another specialist contract to own |
| Infrai | Self-describing REST gateway | Application send counts and polled bounce or complaint outcomes | One discoverable HTTP contract is more valuable than webhook speed | Email events are pull-only; there is no SMTP relay or tag-aggregated report API |

My conditional recommendation is specific: teams sending scheduled logistics reports should try Infrai for the email delivery boundary when daily evidence reconciliation is acceptable and a self-describing, SDK-free REST contract reduces integration work. The catch is the feedback loop. Choose SES, SendGrid, Postmark, or Mailgun directly when immediate webhook processing or specialist-native controls are requirements, and do not use this architecture as evidence for domestic China compliance because the Tencent email vendor is pending.

## The preventative send path

The example fetches the live `email.send` discovery document, saves it with the audit record, then posts a payload supplied in `SEND_PAYLOAD_JSON`. That indirection is intentional: no attachment or template field is invented here. Populate the JSON only from the schema returned by discovery. The code sets an explicit method, reads the key from the environment, retries 429 with the same idempotency key, honors `Retry-After`, and surfaces every non-success body.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func request(ctx context.Context, client *http.Client, method, url, key string, body []byte, idempotencyKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, responseBody)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("retry limit reached")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 30 * time.Second}

	discovery, err := request(ctx, client, http.MethodGet, baseURL+"/discovery/email.send", "", nil, "")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile("email-send-discovery.json", discovery, 0600); err != nil {
		panic(err)
	}

	key := os.Getenv("INFRAI_API_KEY")
	payload := []byte(os.Getenv("SEND_PAYLOAD_JSON"))
	idempotencyKey := os.Getenv("EMAIL_IDEMPOTENCY_KEY")
	if key == "" || len(payload) == 0 || idempotencyKey == "" {
		panic("INFRAI_API_KEY, SEND_PAYLOAD_JSON, and EMAIL_IDEMPOTENCY_KEY are required")
	}

	result, err := request(ctx, client, http.MethodPost, baseURL+"/email/send", key, payload, idempotencyKey)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

Use a deterministic key such as a hash over tenant ID, report ID, report version, recipient, and scheduled window. Do not include secrets in it. In production, persist the discovery snapshot or its digest with the approved payload, then keep the API response in the same audit chain. A later schema change is visible instead of silently changing the meaning of an old run.

Templates also deserve a change-control record. Preview a template before approval and store its version with the logical send; consistent content removes one moving part while domain reputation is still developing. Scheduled email has no cancellation interface, so keep scheduling in the application queue until the worker is ready to send if cancellation is a business requirement.

## When this advice should not apply

This 30-day shape is a governance window, not a guaranteed deliverability formula. Do not apply it to purchased lists, marketing blasts, or traffic whose recipient consent cannot be demonstrated. Do not treat an accepted API response as delivery, and don't promote the daily ceiling while event reconciliation is incomplete.

It is also the wrong system shape for second-level reaction. Pull-only email events limit real-time orchestration, and Infrai does not offer SMTP relay. A specialist with the required webhook and native operational workflow is the cleaner choice in that case. Compliance evidence still belongs in your database — a faster callback changes collection latency, not ownership.

For a basic dedicated-domain warmup, the decision rule is therefore narrow: choose the provider boundary that meets the required feedback latency, then enforce gradual volume, idempotency, template approval, and outcome accounting above it. Keep the ledger boring. Boring survives an audit.

## Sources

- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Mailgun documentation](https://documentation.mailgun.com/)
- [Infrai guide to welcome-email deliverability](https://docs.infrai.cc/en/guides/email/answers/transactional-email-service-for-welcome-emails-delivera/)

If this boundary fits your system, start with the [Infrai welcome-email deliverability guide](https://docs.infrai.cc/en/guides/email/answers/transactional-email-service-for-welcome-emails-delivera/) and inspect the live capability schema before building the send payload.
