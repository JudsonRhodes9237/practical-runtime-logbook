# A Retry Queue for Failed Property Webhook Jobs Under a Rate Limit: Dedup or Idempotency?

You want a stuck outbound webhook backlog to drain without dispatching the same work order twice. Use a standard managed queue, a per-destination rate limit you control, and a delivery ledger keyed by the event id — in that order of importance. Queue-native deduplication (FIFO dedup windows, named tasks, custom job ids) is a narrow optimization for failed jobs: the retry that actually hurts happens hours after every one of those windows has closed.

The queue you can drain deliberately at 03:00 is worth more than the queue with the better feature matrix.

## The duplicate-dispatch incident that sets the rule

A property management platform fans out small, high-consequence events. A work order moves to dispatched. A tenant's card clears. An inspection gets rescheduled. Each event lands in somebody else's system — a contractor dispatch portal, an accounting package, an owner portal — and each of those enforces its own limit and its own opinion about what a repeated request means.

I've been paged for both halves of this problem: deliveries that never arrived, and deliveries that arrived twice. The second page is the worse one. A missed webhook leaves a portal stale until somebody refreshes it, and the fix is a replay; a duplicate dispatch sends a second technician to the same unit, and the argument about who pays for that visit outlives the incident channel by weeks. That asymmetry is the whole design brief. It's why I treat "did the effect happen exactly once" as the invariant and treat queue selection as a downstream detail, rather than the other way round — most of the retry machinery you can buy protects delivery attempts, not effects.

The mechanism is dull, which is why it keeps working. A downstream starts answering 429. Workers back off, the backlog grows, somebody drains it faster than the limit allows, and a good share of those retries are jobs whose first attempt already succeeded — the response just never made it back to the worker. At-least-once delivery is the documented contract of most managed queues, so a duplicate is normal operation, not an exception you get to be surprised by.

Delivery is not finished when the POST returns 202. It's finished when your side has durably recorded that this event id reached this destination.

## How should a retry queue handle failed jobs when the downstream keeps limiting you?

Three concerns get collapsed into one and then blamed on the queue. Admission control decides how fast you're allowed to call a given destination. Deduplication suppresses a repeated submission inside a short window. Idempotency protects the business effect no matter how many times the message shows up. Only the third one is still standing an hour into an incident.

Admission belongs per destination, not per worker fleet. A global concurrency cap starves the eleven vendors who are healthy in order to be polite to the one that isn't. Read `Retry-After` when the downstream sends it, and treat 429 as a scheduling signal rather than an error: pause that destination's bucket, leave everything else alone.

Failure classification is the part that gets skipped, and it's what makes a redrive safe. A 429 or a 503 is retryable with backoff. A 400 with a validation body is not — retrying it forever just burns your rate budget on a payload that will never be accepted, so park it and let a human decide. A transport timeout is neither: you don't know whether the effect landed, so you have to retry it, which is exactly why the ledger exists.

Ambiguity is the default case. Design for it.

## How four common setups differ once a backlog forms

I compare these on one axis — what the operator can actually do at the moment things are already broken — because steady-state throughput numbers stop mattering the second a backlog forms.

| Setup | Rate control during a drain | Built-in duplicate suppression | Recovery ergonomics |
|---|---|---|---|
| SQS standard + DLQ | Consumer-side: your own token bucket and concurrency cap | None; at-least-once is the stated contract | Redrive from the DLQ in bounded slices, ack only after the ledger write |
| SQS FIFO | Ordering per message group; throughput bounded per group | Content or explicit dedup id, 5-minute interval | Ordering helps live traffic; the window is long closed by the time a human replays |
| Cloud Tasks | Queue-level dispatch rate and max concurrent dispatches, plus retry config | Named tasks are refused again for about an hour after completion | Pause and resume the queue while you fix the downstream; naming tasks costs creation throughput |
| Redis with BullMQ | Per-queue limiter expressed as jobs per duration | Custom job id, only while that job still exists in the queue | Total control of the data structures, total ownership of Redis durability and failover |

The five-minute dedup interval on SQS FIFO is the number that misleads people. It's real, it's documented, and it is aimed at a producer that submits the same message twice in a burst. A failed job that gets replayed after an investigation is many multiples of that window away, so FIFO buys ordering and a burst guard, not protection against the duplicates that hurt.

Cloud Tasks moves the rate limit into the queue configuration, which is pleasant when the limit is a property of the destination rather than of your traffic, and its pause control is a genuinely good incident affordance. Named tasks give a longer suppression window than FIFO, still not one that spans a human investigation.

BullMQ's limiter is per queue, and its custom job id suppresses a duplicate only while the job is still present. Redis in this design is a database, and it needs the persistence, failover and memory-ceiling attention that implies. That's fine where Redis is already an operated dependency with a runbook; where it's an incidental cache someone added for sessions, a recovery procedure now includes a datastore nobody on the rotation has ever failed over.

None of the four removes the need for the ledger. RabbitMQ's dead-letter exchanges and Pub/Sub's exactly-once option on pull subscriptions are worth reading precisely because they show how narrowly each mechanism scopes its guarantee.

## The delivery ledger, in Go — the code that makes a redrive safe

The shape is claim, deliver, record. A unique constraint on `(event_id, destination)` does the real work, so a duplicate message can't even start a second HTTP call, and the terminal state survives a worker that dies at the wrong moment.

```go
package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"
)

// Limiter is a per-destination token bucket. One vendor's 429 must not
// throttle deliveries to the other eleven.
type Limiter interface {
	Wait(ctx context.Context, destination string) error
	Pause(destination string, d time.Duration)
}

type Delivery struct {
	EventID     string // stable across every retry of this event
	Destination string // "contractor-dispatch"
	Payload     string
}

var errSettled = errors.New("delivery already settled")

// claim reserves this (event, destination) pair. Rows affected == 0 means the
// pair is delivered or parked, so the caller acks the message and does nothing.
func claim(ctx context.Context, db *sql.DB, d Delivery) error {
	res, err := db.ExecContext(ctx, `
		INSERT INTO delivery_ledger (event_id, destination, state, updated_at)
		VALUES ($1, $2, 'in_flight', now())
		ON CONFLICT (event_id, destination) DO UPDATE
		   SET state = 'in_flight', updated_at = now()
		 WHERE delivery_ledger.state = 'retryable'`, d.EventID, d.Destination)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return errSettled
	}
	return nil
}

func mark(ctx context.Context, db *sql.DB, d Delivery, state string, code int) error {
	_, err := db.ExecContext(ctx, `
		UPDATE delivery_ledger SET state = $1, http_status = $2, updated_at = now()
		 WHERE event_id = $3 AND destination = $4`, state, code, d.EventID, d.Destination)
	return err
}

func deliver(ctx context.Context, db *sql.DB, hc *http.Client, tokens Limiter, d Delivery) error {
	if err := claim(ctx, db, d); err != nil {
		return err
	}
	if err := tokens.Wait(ctx, d.Destination); err != nil {
		return mark(ctx, db, d, "retryable", 0) // out of budget; next tick picks it up
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://dispatch.portal.example/work-orders", strings.NewReader(d.Payload))
	if err != nil {
		return mark(ctx, db, d, "parked", 0)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", d.EventID)

	resp, err := hc.Do(req)
	if err != nil {
		return mark(ctx, db, d, "retryable", 0) // ambiguous: it may well have landed
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode < 300:
		return mark(ctx, db, d, "delivered", resp.StatusCode)
	case resp.StatusCode == http.StatusTooManyRequests:
		// Retry-After may be seconds or an HTTP-date; fall back to a fixed pause.
		wait, perr := time.ParseDuration(resp.Header.Get("Retry-After") + "s")
		if perr != nil || wait <= 0 {
			wait = 60 * time.Second
		}
		tokens.Pause(d.Destination, wait)
		return mark(ctx, db, d, "retryable", resp.StatusCode)
	case resp.StatusCode < 500:
		return mark(ctx, db, d, "parked", resp.StatusCode) // payload problem, a human decides
	default:
		return mark(ctx, db, d, "retryable", resp.StatusCode)
	}
}
```

Two things make this testable in a way that a bare handler isn't. Call `deliver` twice with the same `Delivery` against a fake transport and assert exactly one outbound request — that test catches more real incidents than any load test I've written. Then set the queue's visibility timeout above the worst-case handler duration, or the queue itself becomes your duplicate generator while the first attempt is still in flight.

The metrics worth alerting on are per destination, not global: oldest unsettled ledger row, ratio of 429s to 2xxs, and the count of claims that returned `errSettled`. That last one is your duplicate rate. When it climbs during a drain, the ledger is doing its job and something upstream is replaying too aggressively.

## Where this stops being the right shape

The catch is a durable write per delivery per destination. For high-volume, low-consequence fan-out — analytics pings, cache invalidations, anything where a repeat costs nothing — that write is overhead you don't need, and a plain at-least-once consumer is the honest answer. Ledgers earn their cost where a duplicate spends money or dispatches a human.

This design is also not suitable when ordering across an aggregate is a business rule rather than a nicety; per-key ordering wants FIFO message groups or a partitioned log, and bolting sequence numbers onto an unordered queue is a control plane you'll regret. Stick with a workflow engine when the unit of work is a multi-step saga with compensation, not a single delivery. And if the destination offers no idempotency key at all, you can't push the guarantee across the wire — you keep it on your side, which means dedup on natural business keys and, honestly, a reconciliation job that compares your ledger against their state on a schedule.

I'm not sure any of these queues is meaningfully better than the others once the ledger exists. That's the point. Pick the one whose pause, replay and dead-letter controls your on-call rotation can already operate, and spend the saved argument on the receipts instead.

## Further reading

- https://www.rabbitmq.com/docs/dlx
- https://cloud.google.com/pubsub/docs/overview
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html
- https://cloud.google.com/tasks/docs/configuring-queues
- https://docs.bullmq.io/guide/rate-limiting
- https://www.rfc-editor.org/rfc/rfc6585#section-4
- https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-idempotency-key-header
