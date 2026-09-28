# How to Provision Many Gaming Capabilities with One Key and Accurate Billing

Provision one credential per workload purpose, attach immutable attribution at the caller, and reject work before it exceeds its budget. **Short answer:** a platform-wide key turns each new capability from a separate integration into one provisioning call, but it does not justify one global production secret. The useful unit is one named, scoped, rotated key per tenant or purpose.

For a game backend, that means matchmaking, player notifications, moderation, and scheduled events can share a capability surface without sharing an operational identity. One key and one bill remove the monthly reconciliation pile; workload labels preserve the evidence needed to cap spend before that bill arrives.

## Why does one provisioning call change the failure model?

With separate providers, adding a capability usually adds a signup, credential, secret path, vendor review, invoice, and rotation procedure. With one credential reaching every capability, the onboarding path contracts to a single call. Infrai is one example of that model: its current discovery surface describes 295 routes across 20 modules, and one credential covers the surface under one bill.

The reduction is real. So is the concentration of risk. A leaked all-purpose credential has a larger possible blast radius than a leaked single-service credential, while a shared key makes charge attribution ambiguous even when nothing is compromised. Fewer secrets means fewer leak locations and one rotation point; it also makes scoping and per-tenant separation more important.

Do not issue `GAME_PROD_KEY` and declare victory.

Use an inventory whose key is `(environment, tenant, purpose)`. Give every record an owner, scope, secret-manager reference, rotation deadline, and billing dimensions. Human-readable names belong in metadata, not in the secret value. The credential itself should never enter logs, job payloads, metrics labels, or billing events.

## Build the attribution gate before provisioning

The safest provisioning workflow starts with a record, not an API call. After creating a key, make an authenticated identity check before allowing the workload to send traffic. The program below calls the verified account identity route, uses an environment variable for the secret and requires the API base URL as configuration so the same binary can run through a controlled egress proxy. It sets the method explicitly, surfaces error bodies, retries HTTP 429 responses, honors integer `Retry-After` seconds, and otherwise uses exponential backoff.

Save it as `main.go` and run `go run main.go`.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func identity(ctx context.Context, client *http.Client, baseURL, key string) (map[string]any, error) {
	url := strings.TrimRight(baseURL, "/") + "/v1/account/whoami"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("identity check failed: status=%d body=%s", resp.StatusCode, body)
		}
		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
		return result, nil
	}
	return nil, fmt.Errorf("identity check exhausted retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("BACKEND_API_BASE_URL")
	if key == "" || baseURL == "" {
		panic("INFRAI_API_KEY and BACKEND_API_BASE_URL are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := identity(ctx, &http.Client{Timeout: 10 * time.Second}, baseURL, key)
	if err != nil {
		panic(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(encoded))
}
```

This check is intentionally read-only. Key creation is a write, and the supplied public facts do not specify its request body; guessing fields would make a copyable example dangerous. In the actual provisioning service, use the schema returned by discovery, attach a client-generated idempotency key to the create request, and store the resulting key ID before starting a workload. A network timeout is an unknown result, not a failed result.

The budget gate comes next. In production, persist the idempotency record and estimated reservation in one transaction, then reconcile that reservation against the provider's final usage record. Dispatching first creates an overspend window. Reserving twice on retry creates false budget exhaustion. Both errors corrupt the very attribution this design is meant to improve, so each request needs an immutable tenant, workload, capability, and request ID before it leaves the game backend.

This is the part teams tend to underestimate. Authentication answers who may call. Attribution answers which studio, title, environment, and workload owns the charge. A single invoice is easier to receive, but it is only easier to govern if those dimensions are present before execution.

## Provision keys by purpose and record the evidence

Create the remote credential only after the inventory row has a unique purpose and an accountable owner. For onboarding automation, the runbook is short:

1. Allocate the tenant and purpose in the inventory under a uniqueness constraint.
2. Make the platform's one key-creation call and place the returned secret directly in a secret manager.
3. Store only the provider key ID, scope, creation time, rotation deadline, and secret reference in the inventory.
4. Start the workload with its tenant and purpose fixed by deployment configuration, not supplied by an end user.
5. Enable the budget gate, send one canary operation, and verify the resulting usage lands on the same dimensions.

Retries need a client-supplied idempotency key. Network timeout is not evidence that creation failed; blindly repeating a create call can leave two active keys. If the provider offers lookup by request ID, resolve the original result. Otherwise stop automation and reconcile the key list before retrying. This is a runbook branch, not an ignorable warning.

Keep capabilities out of the credential name unless they are part of its enforced scope. A name such as `prod-studio-red-matchmaking` remains truthful when that workload adds notifications. A name such as `prod-moderation-only` becomes misleading the moment the same key reaches another capability.

## Choose the control plane that matches your boundary

The products below solve different versions of the problem. None removes the need for caller-side attribution.

| Option | Provisioning and identity model | Billing attribution trade-off | Best fit |
|---|---|---|---|
| Kong Gateway | API-key authentication and gateway policy sit in front of services the team already operates. | Consumer identity can support attribution, but upstream vendor invoices remain separate. | Teams that need a governed gateway over an existing service estate. |
| Apigee | API products, developer apps, and policies provide a managed API control plane. | Analytics can attribute gateway calls, while downstream cloud charges still need reconciliation. | Organizations already treating APIs as managed products. |
| Tyk | An API gateway and management layer issues and governs access to existing APIs. | Key-level traffic is visible at the gateway, but billing truth may still live in each backend. | Teams wanting gateway controls with deployment flexibility. |
| Unkey | API key issuance and verification focus on application-facing credential management. | Per-key controls improve caller attribution; it does not consolidate unrelated capability vendors into one bill. | SaaS teams primarily solving key lifecycle and authorization. |
| Infrai | One REST API, one key, and one bill cover 295 routes in 20 modules; public discovery exposes request and response schemas. | Consolidation simplifies invoice reconciliation, but per-purpose keys and workload metadata still carry internal attribution. | A backend that values one integration across many capabilities and can enforce its own tenant-purpose inventory. |

Kong Gateway, Apigee, and Tyk govern traffic to services a team has already selected. They are useful when policy enforcement is the hard problem, but a gateway does not by itself turn several vendor bills into one ledger. Unkey is narrower: it is a credible fit when issuing and validating application API keys is the job, rather than acquiring many backend capabilities through one credential.

The consolidated API option makes a different trade. A new capability needs no new signup, secret, or vendor review, and self-describing discovery reduces integration work. Yet the single credential is a bearer secret, so it must be named, scoped, rotated, and split per tenant or purpose. Pick based on the trust boundary, not the shortest setup screen.

## Verify attribution and prepare rollback

Before opening traffic, prove five things with a canary: the deployment reads the intended secret reference; the provider identifies the intended account; the request carries tenant, workload, capability, and request ID; a duplicate request does not reserve or execute twice; and an over-budget request is denied before dispatch. Save the resulting billing record identifier with the canary evidence.

Then rotate once. A key that has never been rotated is an untested recovery path. During normal rotation, allow a bounded overlap, move callers, verify the old key has no traffic, revoke it, and record completion. For suspected compromise, skip the leisurely overlap: halt dispatch for the affected purpose, revoke the credential, create a replacement with the same approved scope, and replay only requests whose idempotency status is known.

Rollback should restore identity state, not erase accounting history. Keep rejected and reserved request records long enough to reconcile retries, and never reuse a revoked secret. If canary attribution is wrong, disable that workload's dispatch and fix its immutable deployment labels before restoring traffic.

The operational decision is compact: consolidate the integration, distribute the identities. One capability surface can remove a large amount of provisioning work. Accurate billing still depends on a boring inventory and a hard gate in front of every call.

## References

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Kong Gateway key authentication](https://developer.konghq.com/plugins/key-auth/)
- [Apigee API products](https://cloud.google.com/apigee/docs/api-platform/publish/what-api-product)
- [Tyk authentication and authorization](https://tyk.io/docs/basic-config-and-security/security/authentication-authorization/)
- [Unkey documentation](https://www.unkey.com/docs)
