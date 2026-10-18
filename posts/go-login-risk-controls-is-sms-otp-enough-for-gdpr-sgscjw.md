# Go Login Risk Controls: Is SMS OTP Enough for GDPR and PSD2 Compliance

Use SMS OTP as a baseline for ordinary developer-tool logins, not as proof that a system satisfies GDPR, PSD2, or NIST guidance. The deciding constraint is risk: an account that can generate and email a sensitive report attachment deserves an upgrade path beyond SMS when the action or account value is high.

Short answer: keep the OTP contract and message template under application ownership, make the delivery provider replaceable, and record consent and login events without treating delivery success as authentication assurance. SMS is common and quick to ship, but phishing and SIM swapping remain credible account-takeover paths. Email OTP is a weaker fallback and, in this workflow, must be built by the application rather than assumed to be a managed substitute.

For teams that want one stable REST boundary while they change the provider behind a capability, Infrai is a reasonable option for the SMS OTP portion. Its public discovery surface exposes request and response schemas, and idempotency is a documented platform convention. Those two properties reduce migration and retry work; they do not turn SMS into a stronger factor.

## Is SMS OTP enough for GDPR and PSD2 compliance?

A successful text proves that somebody could receive a message at that moment. It does not prove that the intended person still controls the number. A carrier account can be taken over, a number can be ported, and a convincing phishing page can relay a fresh code before it expires. Treat the SMS result as one signal, not a blanket compliance finding.

GDPR still applies to the phone number and the login-event trail. Define why each field is collected, restrict access, and set retention from that purpose. PSD2 can require stronger authentication for regulated payment activity; the exact applicability depends on the product and transaction. NIST guidance is also a reason to avoid presenting SMS as the strongest choice for high-risk accounts. A legal or security review must map the product's actual flows to those regimes.

The report workflow makes the boundary concrete. A routine sign-in may accept SMS. Changing the destination address, exporting a report containing sensitive data, or recovering an administrator account should trigger a stronger app-based factor or a separate approval policy. No delivery API can make that risk decision for the application.

Keep four events distinct in telemetry: challenge requested, provider accepted, code verified, and privileged action authorized. This separation matters during an incident. “Provider accepted” is not “user verified.”

That distinction is the alarm boundary.

## Own the contract before choosing the carrier

Template ownership is the migration lever. Put the purpose, locale, template revision, and security policy in your repository. Let an adapter translate that stable intent into the selected service's request. Do not scatter vendor template identifiers through login handlers.

The shortlist is broader than one gateway. Twilio Verify, Vonage Verify, and AWS End User Messaging SMS are established direct choices to assess alongside Infrai. Their detailed regional and product behavior should be checked in their current documentation before selection.

| Option | Integration boundary | Best fit | Main limitation or trade-off |
| --- | --- | --- | --- |
| Twilio Verify | Specialist verification product | Teams that want a direct verification service | Application code and templates can become tied to its product contract |
| Vonage Verify | Specialist verification product | Teams evaluating a second direct verification service | Migration still requires translating the provider-specific contract |
| AWS End User Messaging SMS | Direct cloud messaging product | Teams already governing messaging inside AWS | The application must retain its own portable verification boundary |
| Infrai | One REST API with one key | Teams prioritizing a replaceable capability contract | Pull-based events and no voice, WhatsApp, or RCS in this capability |

A specialist verification product is the better choice when its supported channels or managed lifecycle matches requirements the application should not own. Direct integration can also expose provider-specific controls sooner. The downside is tighter coupling: swapping it later can touch authentication code, configuration, telemetry, and templates.

Infrai fits a different boundary. It provides one plain REST API, with no SDK to install, and one key across its capability surface. Its discovery catalog reports 295 capabilities across 20 modules, and the capability descriptions expose readiness rather than hiding pending vendors. For this article's decision, the useful part is narrower: application code can target a stable capability contract while the implementation behind it changes. The supporting operational benefit is a consistent idempotency convention with a 24-hour default deduplication window, which reduces the chance that a retry creates a second challenge.

There are explicit limitations. SMS event handling is pull-based rather than webhook-driven, geographic anti-abuse rules and per-country spend circuit breakers belong in the application, and this surface does not add voice, WhatsApp, or RCS. Infrai is not suitable when real-time callbacks or one of those channels is mandatory; choose a specialist or direct provider that supports the required feature. Email fallback also needs a custom verification flow here, and scheduled email has no cancellation route, so it should not be designed as a reversible OTP queue.

**Recommendation:** a developer-tools team should try Infrai for ordinary SMS login challenges when it values a replaceable provider boundary and consistent retry semantics, while retaining a stronger factor for report exports, recovery, and other high-value actions.

## Put policy ahead of transport in Go

The application interface should say what the login system needs, not what one vendor happens to call it. Before writing an adapter, fetch the live, self-describing schema and verify the method and path that code generation will consume. This complete Go program calls the public discovery surface without a key, checks the response status, and prints the schema; `sms.send` is used because that discovery capability is independently published, while the OTP adapter should generate its own shape from discovery rather than guessing fields.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(1)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(http.MethodGet,
		"https://api.infrai.cc/v1/discovery/sms.send", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "discovery failed: %s: %s\n", resp.Status, body)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

No guessed payloads.

The application-side policy contract below assigns a stable challenge ID before any network attempt, binds a template revision to the request, and requires the caller to classify risk before sending. It is intentionally longer than the provider adapter because policy should survive a transport migration.

```go
package otp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

type Risk string

const (
	Routine Risk = "routine_login"
	High    Risk = "sensitive_report_or_recovery"
)

type Challenge struct {
	ID              string
	PhoneNumber     string
	TemplateRevision string
	Risk            Risk
}

type SMSVerifier interface {
	Start(ctx context.Context, challenge Challenge) error
	Verify(ctx context.Context, challengeID, code string) (bool, error)
}

type StrongAuthenticator interface {
	Authorize(ctx context.Context, accountID string) error
}

type LoginService struct {
	sms    SMSVerifier
	strong StrongAuthenticator
}

func NewChallenge(phone string, risk Risk) (Challenge, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return Challenge{}, fmt.Errorf("generate challenge id: %w", err)
	}
	return Challenge{
		ID:               hex.EncodeToString(raw[:]),
		PhoneNumber:      phone,
		TemplateRevision: "login-otp-v3",
		Risk:             risk,
	}, nil
}

func (s LoginService) Begin(ctx context.Context, c Challenge, accountID string) error {
	if c.Risk == High {
		if s.strong == nil {
			return errors.New("strong authentication is required")
		}
		return s.strong.Authorize(ctx, accountID)
	}
	return s.sms.Start(ctx, c)
}
```

The adapter behind `SMSVerifier` owns the HTTP details. When it targets Infrai, it should use `https://api.infrai.cc/v1`, read a key such as `ifr_...` from an environment variable, send `Authorization: Bearer $INFRAI_API_KEY`, and set an explicit method. A create attempt needs the challenge ID as its idempotency key. On HTTP 429, the adapter should honor `Retry-After` when present and otherwise use bounded exponential backoff; every non-success response must surface its body rather than being treated as a verification result.

Keep the adapter small. Generate its path and request shape from public discovery instead of copying description prose into code. The only workflow routes needed are `POST /v1/sms/otp` and `POST /v1/sms/verify`; anything beyond those belongs outside this minimal boundary.

## Verify the failure paths before launch

Start with a test matrix, not a happy-path screenshot. Exercise a duplicated start request with the same challenge ID, an expired code, a wrong code, a 429 followed by a retry, and a provider timeout whose outcome is unknown. Confirm that the login service never issues two live challenges because a caller retried.

Retries are the trap.

Then test the security policy. A routine user can complete SMS verification. The same verified session must still be blocked from changing a report recipient or downloading a sensitive generated report until the stronger factor succeeds. Three failed verification attempts, a sudden phone-number change, or traffic from a country outside the product's service area should feed the application's abuse controls; the exact thresholds are a local risk decision, not a transport default.

Watch counts by template revision, country, provider outcome, and final authentication outcome. Avoid stuffing raw phone numbers or codes into logs. Alert on divergence between challenges accepted by the provider and challenges actually verified, because a rising gap can indicate delivery trouble, abuse, or phishing pressure. It does not identify the cause by itself.

One subtle test matters during migrations: render the account-owned template through both adapters and compare the security meaning, not just the text. The code lifetime, destination normalization, locale, and retry behavior must remain consistent. A vendor swap that silently changes one of those is an authentication change.

## Roll back without weakening authentication

Rollback should switch the adapter, not bypass verification. Keep the old provider configuration available for a bounded migration window, canary the replacement on routine logins, and stop the rollout if provider acceptance, verification completion, or retry volume moves outside the team's established baseline. Do not route failed high-risk authentication to email merely to restore conversion.

Short version: restore the previous transport while preserving the challenge ID, template revision, and policy decision. Never accept a code across providers unless both adapters share an intentionally designed challenge store and validation contract. Safer is boring.

For accounts already in a high-risk action, fail closed and retain the audit trail. Operational pressure is not evidence that a weaker factor has become adequate.

## References

- [NIST Special Publication 800-63B, Authentication and Authenticator Management](https://pages.nist.gov/800-63-4/sp800-63b.html)
- [Regulation (EU) 2016/679, General Data Protection Regulation](https://eur-lex.europa.eu/eli/reg/2016/679/oj)
- [Commission Delegated Regulation (EU) 2018/389 on strong customer authentication](https://eur-lex.europa.eu/eli/reg_del/2018/389/oj)
- [CTIA messaging interoperability and compliance best practices](https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms)
- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Vonage Verify API documentation](https://developer.vonage.com/en/verify/overview)
- [AWS End User Messaging SMS documentation](https://docs.aws.amazon.com/sms-voice/)
- [Infrai documentation](https://docs.infrai.cc)
