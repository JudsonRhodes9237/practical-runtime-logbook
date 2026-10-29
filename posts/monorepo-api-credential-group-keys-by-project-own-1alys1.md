# Monorepo API Credential Group Keys by Project Ownership Rotation Explained for Property Portfolios

TL;DR: Group service credentials by property project, give each group one accountable team, and schedule review from the credential's expiry and traffic signals. Set the spend ceiling only after deciding which requests may be refused. A review that nobody can sign is an alerting failure, not a documentation problem.

The least complex design is a small, explicit inventory: project, environment, credential class, accountable team, reviewer, expiry, last use, and refusal policy. Developer names may appear in the audit trail, but they should not define the credential boundary. In a property portfolio, a leasing integration and a maintenance integration can outlive several engineers and still need different access and traffic decisions.

Keep it boring.

## The page that arrives after the review failed

The on-call page usually arrives as a symptom: a scheduled rent statement export is retrying, a vendor-facing request is being refused, or a queue has started delivering the same work twice. The first useful question is not “who owned this key?” It is “which project was allowed to make this request, and what should have warned us before the boundary was reached?”

Work backward from that page. A project credential should have a known reviewer, a next review time, an expiry event, and a signal that distinguishes normal refusal from an outage. The inventory entry is the control plane; request logs and usage counters are the evidence. If the two disagree, pause the rotation and resolve the identity mapping rather than guessing.

Here is a deliberately small record for three property-management projects. The values are examples, not policy defaults:

```json
{
  "projects": [
    {"name": "lease-import", "environment": "prod", "reviewer": "leasing-ops", "refusal": "fail-closed"},
    {"name": "maintenance-dispatch", "environment": "prod", "reviewer": "field-ops", "refusal": "queue"},
    {"name": "resident-portal", "environment": "prod", "reviewer": "resident-services", "refusal": "fail-closed"}
  ]
}
```

The important field is not the label. It is the action attached to refusal. A lease import can stop and page; a maintenance dispatch may need to preserve work in a queue while access is corrected. Treating both as “retry” makes a later postmortem harder because the retry policy hides the business decision.

## How should a monorepo group API credentials and keys by project?

A useful schedule combines time and change. Time alone misses a revoked person still using a live credential. Change alone misses a quiet credential that has quietly outlived its purpose. I would create a review task when any of these events occurs: a project changes environment, its accountable team changes, the credential scope changes, its observed caller changes, or its expiry window is reached. The task should contain the proposed action and evidence, so the reviewer can sign it without opening six dashboards. The tradeoff is deliberate: more review events create more work, while fewer events leave a wider window for stale access. For a small property team, that extra task is preferable to asking an engineer to reconstruct ownership during a refused export.

This is where a scheduler becomes an SRE control rather than a calendar. It must be idempotent. The same event may be delivered twice, and a deployment may replay an event after a timeout. Use a stable review key such as project, environment, credential version, and review window. Store the decision separately from the notification attempt. A failed email must not create a second rotation task, and a successful notification must not mark the review complete.

```go
package review

import (
    "crypto/sha256"
    "encoding/hex"
    "fmt"
)

type Credential struct {
    Project     string
    Environment string
    Version     string
    Window      string
}

func ReviewKey(c Credential) string {
    raw := fmt.Sprintf("%s|%s|%s|%s", c.Project, c.Environment, c.Version, c.Window)
    sum := sha256.Sum256([]byte(raw))
    return hex.EncodeToString(sum[:])
}

// CreateReview must be backed by a unique key in durable storage.
func CreateReview(store Store, c Credential) error {
    key := ReviewKey(c)
    return store.InsertIfAbsent(key, c)
}

type Store interface {
    InsertIfAbsent(key string, c Credential) error
}
```

The hash is not a security boundary and it does not replace secret storage. It only gives the scheduler a deterministic idempotency key. The durable store still needs a uniqueness constraint, and the worker still needs a retry policy with an observable terminal state. That small distinction prevents a common incident pattern: the alert is fixed, but the automation quietly creates duplicate review work.

The limitation matters. A project boundary cannot correct an upstream service that exposes only one shared credential or produces no usable caller identity. In that case, schedule the review around the shared credential, record the lost attribution, and treat the missing boundary as a risk to remove. Splitting labels in a monorepo will not create isolation that the API does not provide.

## How do spend ceilings change the refusal decision?

A spend ceiling is a traffic policy, not an ownership model. For each project, record the maximum tolerated usage and the requests that may be refused when that limit is approached. Then record what must continue for safety or legal operations. Property software often has an uneven risk profile: a resident-facing access check may deserve a different refusal action from a bulk report export, even when both use the same upstream service.

Keep the rule legible. For example, a project may allow ordinary reads, queue non-urgent exports, and refuse new bulk work once the ceiling signal is active. The review asks three concrete questions: is the current usage expected, is the credential still scoped to this project, and is the refusal action safe for the pending work? A reviewer who cannot answer all three should return the task, not approve it with a vague comment.

Do not turn a threshold into a single magic number. Measure request count, rejected requests, retry volume, and the age of queued work. A threshold that pages on every short burst trains people to ignore the page. A threshold that waits for widespread refusal makes rotation an emergency. The right boundary is the one that gives the accountable team time to review while preserving the refusal behavior chosen for that project.

That is the boundary.

## The instrumentation change that makes the page actionable

Emit one event for the review decision and separate events for runtime traffic. Include project and environment identifiers, a credential version, the policy outcome, and a correlation identifier. Never put the secret itself in a log. A dashboard can then answer whether the page came from an expired credential, an unexpected caller, a refusal policy, or a scheduler delivery problem. Those are different repairs.

The review record should show last observed use, but “used recently” is not proof that access is appropriate. Compare the observed caller with the project boundary and require an explicit exception with an owner and end time. This is the point where developer ownership is useful as evidence and dangerous as the primary grouping. People rotate; project responsibilities and refusal rules are the durable objects.

Use staged rotation where the protocol permits it: issue the replacement, verify the expected project traffic, switch consumers, and revoke the old version. The audit entry should capture each state transition. If a consumer cannot reload credentials without a restart, that operational fact belongs in the review because it changes the refusal window and the rollback plan.

## What a reviewer can actually sign

A signable review is short enough to read and specific enough to reject. It names the project, the environment, the access purpose, the credential version, the accountable team, the next review event, the observed traffic summary, and the refusal action. It also says what happens to queued work if access is denied. That last sentence is where many otherwise tidy controls become operationally unsafe.

The reviewer can approve, rotate, narrow, or revoke. Each outcome needs an audit event and a follow-up owner. Approval should not reset the expiry silently; rotation should not close the review until the new version is observed; revocation should not delete the evidence needed to explain the refusal. These are ordinary state-machine rules, but writing them down is cheaper than reconstructing them during a page.

My decision rule is plain: group by the smallest project boundary that has its own reviewer and refusal behavior. If two projects share a credential only because deployment is convenient, split them before adding more scheduling logic. If they genuinely share the same purpose, team, environment, and refusal policy, document why the shared boundary is acceptable and test its rotation path. A centralized control service also has a boundary: it becomes the wrong fit when the team cannot export its audit trail, test refusal behavior locally, or recover when the scheduler is unavailable. Keep an offline review record for that case, and make the recovery step part of the runbook rather than assuming the control plane is always reachable.

That produces a review someone can sign, a refusal someone can explain, and a page that points to a repair. The spend ceiling remains a guardrail. It never becomes the owner.

## Further reading

- OWASP Secrets Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- NIST Digital Identity Guidelines, Authentication and Lifecycle Management: https://pages.nist.gov/800-63-4/sp800-63b.html
- RFC 6749, The OAuth 2.0 Authorization Framework: https://www.rfc-editor.org/rfc/rfc6749
- OpenTelemetry Specification: https://opentelemetry.io/docs/specs/otel/
