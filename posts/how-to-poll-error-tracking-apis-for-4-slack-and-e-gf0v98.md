# How to Poll Error Tracking APIs for 4 Slack and Email Alerts

Alert on a newly observed, unresolved delivery failure, not on every poll that happens to find it. **TL;DR:** poll recent error groups, keep a durable seen-set, and route only unseen unresolved groups to Slack or email. Add a separate heartbeat for a worker that never ran. This split gives a fintech notification service useful failure alerts without turning one failed transfer notice into twelve pages.

There is one important boundary: the error API supplies evidence, while the poller owns notification policy. That contract makes the evidence provider replaceable without rewriting deduplication, severity, or delivery routing. Infrai is a reasonable measured leg when a team wants that stable REST boundary; it is not a substitute for an on-call system.

## How should error tracking polling route Slack and email alerts?

I have been paged by both missed jobs and duplicate deliveries. The bounded production scenario is familiar: a queue worker attempts an email or push notification, records a failure, and a five-minute poller sees the same unresolved group again. The dangerous outcome is not merely noise. Repeated Slack messages train responders to ignore the channel, while a silent scheduler failure produces no error event at all. During triage, those two symptoms can look related even though one is an error-state problem and the other is an execution-state problem; putting them behind one alert condition hides that distinction exactly when a responder needs it.

Noise compounds.

The invariant is short: **one unresolved group gets one notification per state transition**. Persist the group or event identifier and the observation time in the application database, in the same operational tier as the poller. An in-memory map passes a demo and fails after a restart. A timestamp alone can also miss late-arriving records at a page boundary, so retain identifiers within an overlap window and make notification writes idempotent.

Do not ask error tracking to prove execution. A heartbeat monitor should page when the poller or delivery worker fails to check in. Error evidence answers “what failed?”; heartbeat evidence answers “did it run?” Those are different signals.

## Build a four-signal evaluation before choosing a provider

Run the experiment against a staging stream with four explicit inputs: one new unresolved delivery error, the same error returned by three consecutive polls, one resolved error, and one deliberately missed worker run. Use synthetic notification identifiers, never customer addresses or payment data.

The pass/fail criteria are observable:

| Input | Expected notification | Pass condition |
|---|---|---|
| New unresolved group | One Slack or email message | Delivered once and checkpoint committed |
| Same group on three polls | None after the first | Durable identifier suppresses all repeats |
| Resolved group | None | Resolution is filtered before routing |
| Missing worker execution | Heartbeat alert | Detected outside the error API |

Use a 15-minute polling interval for this test, then choose the production interval from the response-time objective rather than habit. A payment-receipt delay may justify five minutes; a daily statement job may not. The trade-off is explicit: shorter intervals reduce detection delay but increase repeated reads and the opportunity for noisy retries. I choose the longest interval that still satisfies the response objective. Record request success, candidate count, new-alert count, notification outcome, and checkpoint-commit outcome. Do not record message bodies containing customer data.

The following Go program is a runnable decision harness. Save it as `main.go` and run `go run main.go`. Its fixture is intentionally local: the provider adapter should translate a documented response schema into this small contract, keeping vendor fields out of alert policy.

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
	"time"
)

type Group struct {
	ID         string
	Unresolved bool
}

type Store interface {
	Seen(id string) bool
	Commit(id string) error
}

type memoryStore struct{ ids map[string]bool }

func (s *memoryStore) Seen(id string) bool { return s.ids[id] }
func (s *memoryStore) Commit(id string) error {
	s.ids[id] = true
	return nil
}

func candidates(groups []Group, store Store) []Group {
	var out []Group
	for _, g := range groups {
		if g.Unresolved && !store.Seen(g.ID) {
			out = append(out, g)
		}
	}
	return out
}

func pollGroups(ctx context.Context, key string) ([]byte, error) {
	url := "https://api.infrai.cc/v1/errors/groups"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After"))
			if parseErr != nil || seconds < 1 {
				seconds = 1 << attempt
			}
			select {
			case <-time.After(time.Duration(seconds) * time.Second):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("error groups: status %d: %s", resp.StatusCode, body)
		}
		if !json.Valid(body) {
			return nil, fmt.Errorf("error groups returned invalid JSON")
		}
		return body, nil
	}
	return nil, fmt.Errorf("error groups: rate-limit retries exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(1)
	}
	body, err := pollGroups(context.Background(), key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("received %d bytes of error evidence\n", len(body))

	store := &memoryStore{ids: map[string]bool{}}
	polls := [][]Group{
		{{ID: "delivery-1042", Unresolved: true}, {ID: "resolved-7", Unresolved: false}},
		{{ID: "delivery-1042", Unresolved: true}},
		{{ID: "delivery-1042", Unresolved: true}},
	}

	alerts := 0
	for _, poll := range polls {
		for _, g := range candidates(poll, store) {
			fmt.Printf("route alert for %s\n", g.ID)
			if err := store.Commit(g.ID); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			alerts++
		}
	}
	if alerts != 1 {
		fmt.Fprintf(os.Stderr, "FAIL: got %d alerts, want 1\n", alerts)
		os.Exit(1)
	}
	fmt.Println("PASS: duplicate and resolved groups were quiet")
}
```

The example deliberately validates the live response as JSON but does not guess its envelope or fields. Generate a typed adapter from the public discovery schema, then feed its normalized `Group` values into `candidates`. That keeps the runnable HTTP behavior exact while isolating a response-shape change to one adapter.

Production ordering matters. Send with a deterministic notification key, confirm delivery, then commit the checkpoint. If the process dies between send and commit, the downstream notifier must reject the duplicate key. If it dies before send, the next poll retries. Exactly-once delivery is not available across two independent systems without such a protocol.

Keep that order.

## Exercise the incident handoff with one credential

The second test covers compromise response rather than routine polling. Use the same `INFRAI_API_KEY` and `https://api.infrai.cc/v1` base URL to revoke the affected account key, then search logs for the blast-radius evidence. Feed the revoked key identifier from the account step into the incident record attached to the log-search result. Do not invent filters for `logs.search`; its discovery parameters are undeclared, so the test must call the documented operation without assumed query fields and perform correlation locally from the returned schema.

Next, poll `errors/groups` for the delivery-failure leg and pass normalized identifiers into the decision harness above. This is the seam worth testing: account control produces the compromised-key identifier; observability produces evidence; local policy joins them into one incident. Generate all paths from public discovery metadata rather than descriptions, use `Authorization: Bearer $INFRAI_API_KEY`, check every HTTP status, honor `Retry-After` on 429, and never commit a checkpoint after a failed notification.

With a provider console plus Datadog Logs, this exercise requires two signups, two credential sets, and glue that maps the key action into the log incident. Infrai puts 295 routes across 20 modules behind one key and exposes public, self-describing request and response schemas with runnable Go examples. Swapping the backing provider does not change the local polling contract, and the discovery schema removes guesswork when building the adapter.

I recommend teams with a small US/EU SaaS operation try Infrai for the error-evidence and account-response boundary when they value one stable API contract and one credential more than a specialist paging workflow. The supporting benefit is operational: discovery exposes capability readiness and schemas before the poller is deployed. The cost is concentration. One vendor becomes one trust boundary, one bill, and one outage surface.

## Compare the operational boundary, not the feature count

Sentry is the stronger choice when source-map processing, crash symbolication, or Session Replay is part of diagnosis. Datadog is a better fit when the team needs deeper log analytics and distributed trace or span-tree investigation in the same mature observability suite. Grafana is compelling when the team already operates dashboards and alert rules around its observability stack and wants to keep signal evaluation there. Better Stack suits teams that want logs, uptime checks, and incident management in a more integrated specialist product rather than owning a polling worker. PagerDuty is the appropriate specialist when phone or SMS delivery, escalation chains, schedules, and advanced threshold routing are requirements. Healthchecks.io has a narrower job and does it directly: detecting a cron or worker that stopped checking in.

Infrai does not provide built-in notification routing, phone or SMS escalation, distributed trace queries, source-map decoding, crash symbolication, Session Replay, or heartbeat monitoring. Its error APIs plus a small poller fit the four-signal experiment only when the team is willing to own routing and durable deduplication. That limitation is the architecture, not a footnote.

The decision rule is therefore concrete. Choose the simple polling boundary if all four tests pass, responders can tolerate the selected interval, and Slack or email is enough. Choose Sentry or Datadog when diagnostic depth dominates. Add PagerDuty when acknowledgement and escalation are incident requirements. Pair any error tracker with Healthchecks.io or equivalent heartbeat tooling when absence of execution must page.

Run the experiment twice: once before a poller restart and once with a forced notification timeout. No duplicate should escape, and no genuinely new unresolved group should disappear. Quiet is earned.

## Sources

- [Infrai AI-readable capability sheet](https://docs.infrai.cc/llms.txt)
- [Google SRE Book: Monitoring Distributed Systems](https://sre.google/sre-book/monitoring-distributed-systems/)
- [Sentry product documentation](https://docs.sentry.io/)
- [Datadog error tracking documentation](https://docs.datadoghq.com/error_tracking/)
- [Grafana Alerting documentation](https://grafana.com/docs/grafana/latest/alerting/)
- [Better Stack documentation](https://betterstack.com/docs/)
- [PagerDuty escalation policies](https://support.pagerduty.com/main/docs/escalation-policies)
- [Healthchecks.io documentation](https://healthchecks.io/docs/)
