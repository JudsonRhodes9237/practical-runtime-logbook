# Scheduled Reservation Data Cleanup API: Node.js Files, Queue Retries, and a DLQ

Short answer: schedule discovery, enqueue one cleanup command per expired reservation, and let an idempotent worker perform the object deletion. Retries belong around transient work; deterministic failures should land in a dead-letter queue (DLQ) with enough context to fix and redrive them. This is the useful boundary for a Node.js API that expires stale B2B SaaS reservations without making one long cron request responsible for every file.

The hard part is not deleting an old object. It is deciding what a second delivery means.

## Reservation governance starts with the expiry record

A reservation is a hold on a resource: an export file, a generated report, or a temporary upload. After the hold window, the system may remove the object and mark the reservation expired. Those are two state changes, and they should not be treated as one indivisible operation unless the storage and database share a transaction boundary.

The scheduler should identify candidates and publish work. The worker should re-read the reservation state, check its expiry time, delete the object if deletion is still allowed, and record the outcome. The message should carry a stable `reservation_id`, a storage bucket, an object key, and a cleanup run identifier. A redelivery then points to one decision, rather than asking a worker to reconstruct an entire scan.

I've been paged by missed jobs and duplicate deliveries. The operational lesson is plain: a retry is normal control flow, not evidence that the original request never ran.

For a fixed hold window, make the expiry test explicit. If the worker receives a message late, it should not infer eligibility from the message's arrival time. It should compare the reservation's authoritative `expires_at` with the current clock, and it should avoid deleting an object whose reservation was renewed. Clock skew and a delayed queue are ordinary conditions; the state check is what makes them safe.

The scheduler also needs an overlap policy. A run that is still scanning when the next tick starts can publish the same reservation twice. A stable run ID and a uniqueness rule on `(reservation_id, cleanup_run_id)` make duplicate discovery observable and manageable. The worker still needs its own idempotency key, because queues commonly provide at-least-once delivery rather than exactly-once execution.

## How can a Node.js API schedule data cleanup for expired files?

Classify failures before choosing a retry action. A timeout, temporary connection failure, or throttling response is usually worth retrying with exponential backoff and jitter. A malformed object key, an invalid reservation, or a permission decision that will not change on its own should stop retrying after the configured attempt limit. Send the message to the DLQ with the reservation ID, bucket, key, attempt count, error class, and first-seen time.

Do not use the DLQ as a trash can. It is a holding area for work that needs a decision. Give it an owner, alert on age and count, and make redrive a deliberate operation after the underlying condition is corrected. A short error message is not enough for a postmortem.

The delete operation should be safe to repeat. If the object has already disappeared, the worker can record the cleanup as complete according to the storage contract. The database update, audit event, and notification are separate side effects, so each needs the same stable idempotency key or an equivalent uniqueness constraint. Otherwise a harmless duplicate delete can still create duplicate billing events or misleading audit rows.

Here is a deliberately small worker boundary. It does not depend on a commercial SDK or a particular queue; an HTTP or Node.js implementation can supply the same interfaces. The important behavior is the state re-check, the idempotency record, and the separation between retryable and permanent errors.

```go
package cleanup

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Reservation struct {
	ID        string
	Bucket    string
	Key       string
	ExpiresAt time.Time
	Renewed   bool
}

type ReservationStore interface {
	Get(ctx context.Context, id string) (Reservation, error)
	MarkExpired(ctx context.Context, id, idempotencyKey string) error
}

type ObjectStore interface {
	Delete(ctx context.Context, bucket, key string) error
}

type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

type Message struct {
	ReservationID string
	RunID         string
}

func Handle(ctx context.Context, msg Message, store ReservationStore, objects ObjectStore, now time.Time) error {
	reservation, err := store.Get(ctx, msg.ReservationID)
	if err != nil {
		return err
	}
	if reservation.Renewed || now.Before(reservation.ExpiresAt) {
		return nil
	}

	idempotencyKey := fmt.Sprintf("reservation-expiry:%s", reservation.ID)
	if err := objects.Delete(ctx, reservation.Bucket, reservation.Key); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return err // the queue should retry this delivery
		}
		return PermanentError{Err: err} // the queue should route this to its DLQ
	}
	return store.MarkExpired(ctx, reservation.ID, idempotencyKey)
}
```

The queue adapter should map a returned transient error to a delayed retry and a `PermanentError` to the DLQ. It should acknowledge only after the object operation and the idempotent state write have both reached their intended outcome. If the process dies between those steps, the next delivery repeats the delete and retries the state write; that is why the state write must be deduplicated.

## Keep the cleanup pipeline small enough to replace

There are several reasonable designs, but they solve different problems.

| Design | Retry and failure model | Good fit | Trade-off |
| --- | --- | --- | --- |
| Storage lifecycle rule | Storage-managed age or prefix expiration | Every object under a simple, unconditional retention rule | Cannot inspect reservation or tenant state before expiry |
| Cron plus queue workers | The scheduler discovers; workers retry and route poison messages | Independent objects with a fixed hold window | Requires idempotency, queue monitoring, and DLQ ownership |
| Redis-backed job worker | Application-managed attempts and backoff | A team already operating Redis and worker processes | Failure policy and redrive tooling remain application work |
| Workflow engine | Durable steps, timers, joins, and compensation | Cleanup that is a multi-step business process | More state and operational machinery than one delete command |

The comparison axis is retry and idempotency, not the number of knobs in a scheduler. A lifecycle rule is the cleanest choice when the database has no say. A queue is a good middle layer when each reservation can be handled independently. A workflow engine earns its complexity when deletion depends on several steps and a later action must wait for all of them.

The catch is important: this pattern is not suitable when the worker cannot authenticate its storage access, when the cleanup requires a cross-record transaction, or when the message must serve as a long-term event history. Stick with a lifecycle rule for unconditional expiration, and use a workflow-oriented design when fan-in, compensation, or human approval is part of the job. Your mileage may vary on the right backoff window; measure the storage service's behavior and the business hold window together.

## Test the redrive path before production rollout

A green cron invocation proves very little. Record the run ID, candidate count, published count, acknowledged count, retry count, DLQ count, and oldest outstanding message. Track the age of the oldest expired reservation as a business metric. A queue can be healthy while stale data accumulates because discovery is filtering incorrectly.

Test the boundaries with a small matrix: an unexpired reservation, a renewed reservation, an already-deleted object, a transient timeout, a permanent permission failure, a duplicate delivery, and a worker crash after deletion but before the state write. The duplicate case is the one teams often skip. It is also the one that turns a routine retry into a customer-visible accounting problem.

Keep the scheduler's job small. It should time out, authenticate its target, and publish bounded messages; it should not hold a database scan open while deleting thousands of objects. Bound the batch, record a continuation point, and make a retried trigger reuse its logical run ID. A run that is easy to resume is easier to operate than a run that claims to be atomic but cannot be.

Three words: retry the uncertain.

Do not retry a bad contract forever. Preserve the failed message, repair the cause, and redrive it with an audit trail. That is how a scheduled cleanup becomes an operational process rather than a cron expression that quietly erases evidence.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://www.rabbitmq.com/docs/priority
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html
