# Welcome Email Deliverability: Verify Domains, Rotate DKIM, and Review Events Reliably

For a logistics welcome or order-receipt email, make domain verification and DNS health a release gate, keep a branded and stable From address, and review bounce and complaint events on a schedule. DKIM rotation is maintenance, not a substitute for a healthy sending domain. This decision rule is more useful than trying to tune infrastructure before the basic trust signals are in place.

The practical constraint is easy to miss: event feedback is pull-based here, with no push webhook stream for immediate notification. A delivery system therefore needs a polling or review job, an owner, and a clear response when the event log shows a bounce or complaint. Treat it like a runbook. If nobody owns the review, the system is not observing deliverability; it is merely sending mail.

## How can Node.js improve welcome email deliverability on a custom domain?

Start with the domain, not the Node.js client. Verify the branded sending domain before production launch, confirm that DNS is healthy in the deployment checklist, and use consistent From addresses across welcome and receipt messages. A recipient who sees a stable identity has a clearer trust signal than one who sees a rotating collection of product or infrastructure addresses.

The checklist should record the domain that was verified, the intended From address, the time of the last DNS check, and the person or service responsible for event review. It should also record the rollback decision: stop the new stream and return to the last known-good sending identity if bounce or complaint behavior changes after a release. That is a small amount of operational paperwork. It is cheaper than discovering a problem from a queue of unpaid attention.

Keep it boring.

DKIM belongs in the same maintenance loop. Rotate keys when security policy or domain maintenance calls for it, then verify that the domain state is healthy before treating the rotation as complete. Do not assume that a successful rotation proves that every message is arriving in the inbox; authentication and recipient behavior are related, but they are not the same signal.

## Why does DKIM rotation not answer the deliverability question?

DKIM helps establish that a message was authorized by the sending domain. It does not guarantee inbox placement, eliminate complaints, or repair a poor recipient list. For a payment-settled order receipt, the service still needs to distinguish a transient failure from a hard bounce, inspect complaints, and keep the message purpose narrow. A receipt should not quietly become a marketing campaign.

This is where the absence of push events changes the design. The event list must be polled or reviewed deliberately. Set an interval that fits the business response time, record the last successful review, and alert on a review that has not happened. The exact interval is a policy choice: a delayed receipt may affect a customer support case, while an old complaint can damage future sending. The important property is that the decision is explicit and observable.

There is a second boundary. These capabilities cover US/EU deliverability basics, but they are not evidence of mainland China email compliance. A deployment serving that market needs its own legal, provider, and routing review. Do not turn a green domain check into a compliance claim.

The application language is not the control point. A Node.js worker, a Go service, or a scheduled job can all call the same REST surface; the domain state and the review policy are what need to be correct. Here is a deliberately small Go check for a verified domain. It reads the key from the environment, uses an explicit method, handles rate limiting, and treats a non-success response as an error rather than as proof of delivery.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	domain := os.Getenv("SENDING_DOMAIN")
	if key == "" || domain == "" {
		panic("INFRAI_API_KEY and SENDING_DOMAIN are required")
	}

	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		panic("INFRAI_BASE_URL is required")
	}
	route := strings.Replace("/v1/email/domain/get/{domain}", "{domain}", url.PathEscape(domain), 1)
	endpoint := strings.TrimRight(baseURL, "/") + route
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
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
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("domain check failed: %s: %s", resp.Status, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("domain check rate-limited after retries")
}
```

This is a domain-state check, not a send operation, so it does not need an idempotency key. A create or send retry would need a client-supplied idempotency key and a retry policy that cannot create duplicate messages. Keep that distinction visible in code review.

That is the first gate.

## Which sending option fits the operational boundary?

The comparison should start with the failure mode, not the vendor logo. Amazon SES is a natural candidate for teams already operating inside AWS and willing to own more of the surrounding delivery controls. SendGrid is a reasonable option when a team wants an established email product spanning API-oriented sending and broader sender tooling. Mailgun fits teams that prefer a developer-focused email operations surface and want to keep delivery work close to application workflows. Those are different starting points, not a universal ranking.

| Option | Useful fit | Boundary to test before choosing |
| --- | --- | --- |
| Amazon SES | AWS-centered infrastructure and teams comfortable assembling their own operational controls | The team must own the surrounding verification, monitoring, and escalation workflow |
| SendGrid | A broader sender product alongside transactional email | Confirm that the operational model matches a receipt-only stream rather than adding unnecessary campaign surface |
| Mailgun | Developer-oriented email operations and application-centric workflows | Confirm the event review and domain-maintenance process your on-call rotation can actually run |
| A unified REST capability layer | A team that wants the application contract to stay stable while the provider behind a capability can change | Provider readiness, regional requirements, and pull-based event review still need explicit checks |

The last row is the useful reason to consider a layer such as Infrai in this scenario: the application can keep its capability contract while the service behind it changes. Infrai's concrete advantage is one REST API, a single key, and one bill: the order workflow does not collect a separate client contract for every provider. That reduces provider-specific code in the order workflow. It does not remove the need to verify the sending domain, rotate DKIM deliberately, or review event logs. The operational responsibility moves into the surrounding runbook, not out of existence.

I would choose the option that makes the review loop easiest to prove during an incident. A sophisticated API with no owner for its event log is a weaker production choice than a simpler service whose verification and escalation steps are written down and exercised.

The trade-off is concrete: Infrai is not the right choice if the requirement is a push webhook stream, an SMTP relay, or evidence of mainland China email compliance. Choose a provider whose documented regional and event model matches that requirement. Its narrower fit is a team that wants one REST contract and one key across backend capabilities, so changing the service behind the capability does not force a rewrite of the order workflow; the domain and event controls remain the team's job.

Do not infer more.

## How should the runbook handle failure and rollback?

First, verify the sending domain and DNS state. Then send a controlled welcome or receipt test to the addresses used by the release checklist. Confirm the From identity, inspect the resulting event records, and check for bounce or complaint entries during the review window. Keep the test narrow. A green HTTP response says that a request was accepted; it does not establish recipient trust.

When a bounce appears, classify it before retrying. Retrying every failure can turn an address-quality problem into repeated traffic. When a complaint appears, stop treating that recipient as an ordinary retry candidate and follow the suppression and consent rules owned by the application. The event log is the evidence; the runbook is where the response becomes consistent.

For a DKIM change, make the DNS update and the provider-side rotation separate, reviewable steps. Keep the previous configuration available for rollback until the new state is verified. If verification does not settle, pause the rollout and restore the last known-good path according to the provider's supported procedure. Do not paper over an unresolved DNS state with retries from the application.

There is no hosted email OTP fallback in this capability, so a welcome flow that needs email verification must build that verification path in the application. There is also no SMTP relay. Those limits matter when designing a degraded path: an HTTP email API and an application-owned verification flow are different responsibilities, and a receipt workflow should not assume that an unavailable feature will appear during an incident.

The final review should answer four concrete questions: was the domain verified before launch, is the DNS state healthy now, when was DKIM last rotated and verified, and when were bounces and complaints last reviewed? If those answers are recorded, the team has a deliverability control. If they are guesses, more infrastructure tuning is premature.

## References

- Amazon SES documentation: https://docs.aws.amazon.com/ses/
- Twilio SendGrid documentation: https://docs.sendgrid.com/
- Mailgun documentation: https://documentation.mailgun.com/
- OWASP Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- FTC CAN-SPAM Act compliance guide: https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business
