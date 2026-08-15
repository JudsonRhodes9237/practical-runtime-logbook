# Why I Scheduled Node.js Invoice JSON Extraction with Reranked Chunks

Short answer: schedule long supplier-invoice extraction as a bounded, checkpointed job: chunk the text, retrieve and rerank evidence for each field, fit only that evidence inside the model's token limit, and validate the assembled JSON before publishing it. Choose a larger single request only while measured end-to-end quality remains acceptable and its worst-case latency fits the job deadline.

I care about that last condition because a fast answer that silently drops the payment terms is wrong, while a careful answer that arrives after the media finance export has closed is operationally useless. I've been paged by missed jobs and duplicate deliveries. That changes the design: the unit of work needs an identity, every stage needs a budget, and publication must be idempotent.

Don't treat a timeout as permission to send the same long document again.

## A timeout is a scheduling signal, not a retry instruction

First separate four limits that often collapse into one vague “timeout.” The queue has a delivery or lease window. The worker has an execution deadline. The model endpoint has a request deadline. The selected model has a context limit shared by instructions, evidence, and output. Raising one limit doesn't enlarge the others, and a retry can consume the remaining job budget without improving evidence coverage. For invoice extraction, I schedule by field groups rather than by pages alone. Header fields such as supplier name and invoice number tend to need different evidence from line items or remittance instructions. Each field group gets a query, candidate chunks, an embedding retrieval pass, a rerank pass, and a reserved output allowance. The final extraction request contains the highest-ranked passages with stable chunk IDs. This makes missing evidence visible instead of letting truncation decide which end of a long document disappears. There is a catch. Chunking can split a label from its value, tables can cross page boundaries, and repeated totals can make a locally plausible chunk globally wrong. Structural overlap should preserve page and character offsets, with neighboring chunks retained after one of them ranks highly. For a scanned 40-page supplier packet, the useful clue may be a “Total due” label at the bottom of one page and the amount at the top of the next. A fixed-size splitter that discards those coordinates creates a quality defect that no longer timeout can repair.

Embeddings are a candidate generator, not the final judge. Reranking a small candidate set against a field-specific question helps distinguish “invoice total” from a repeated subtotal, but the assembled result still needs document-level checks. I'm not sure one retrieval recipe will win across native PDFs, OCR text, and exported spreadsheets; your mileage may vary. Resolve that uncertainty with a labeled evaluation set that preserves each input type, rather than tuning against a few tidy invoices.

The decision rule is plain: prefer the smallest evidence set that clears the quality threshold and still leaves deadline headroom. If recall falls when evidence is reduced, add candidates or route the document to a slower lane. If the deadline is already exhausted before extraction begins, mark the attempt as deferred and let the scheduler decide what happens next. Don't improvise inside the worker.

## What should a Node.js text to JSON extraction scheduler do at the token limit?

The safe shape is a state machine with persisted boundaries: normalize, chunk, retrieve, rerank, extract, validate, and publish. A stage records its input digest, configuration version, attempt, start time, end time, and outcome. The digest prevents an old chunk set from being paired with newly normalized text; the configuration version lets operators compare a rollback with the current release. Publication uses a key derived from the supplier document and schema version, so a redelivery cannot create a second finance record.

This also keeps retry policy honest. RFC 9110 distinguishes idempotent methods because automatic retry has different consequences depending on request semantics. The same reasoning belongs inside a queue consumer even when HTTP isn't visible: normalization and retrieval can usually be recomputed from immutable inputs, while a publish step needs an idempotency key or a read-before-write contract. A `408 Request Timeout` or a canceled local context tells us the attempt did not finish; it does not prove that a remote side effect never occurred.

The focused Go sketch below leaves the model and vector implementations behind interfaces. It deliberately passes evidence IDs through extraction and validation. Token counting is also injected, because a character count is not a model context count.

```go
package invoice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

type Chunk struct {
	ID     string
	Text   string
	Page   int
	Start  int
	End    int
}

type Invoice struct {
	Supplier      string   `json:"supplier"`
	InvoiceNumber string   `json:"invoice_number"`
	Currency      string   `json:"currency"`
	Total         string   `json:"total"`
	EvidenceIDs   []string `json:"evidence_ids"`
}

type Ranker interface {
	Candidates(ctx context.Context, query string, chunks []Chunk) ([]Chunk, error)
	Rerank(ctx context.Context, query string, chunks []Chunk) ([]Chunk, error)
}

type Extractor interface {
	Extract(ctx context.Context, evidence []Chunk, schemaVersion string) (Invoice, error)
}

type Validator interface {
	Check(invoice Invoice, evidence []Chunk) error
}

type TokenCounter interface {
	Count(text string) int
}

type Job struct {
	DocumentID    string
	SchemaVersion string
	Deadline      time.Time
	EvidenceLimit int
}

func Run(
	ctx context.Context,
	job Job,
	chunks []Chunk,
	ranker Ranker,
	extractor Extractor,
	validator Validator,
	tokens TokenCounter,
) (Invoice, string, error) {
	if time.Until(job.Deadline) <= 0 {
		return Invoice{}, "", errors.New("job deadline exhausted")
	}

	ctx, cancel := context.WithDeadline(ctx, job.Deadline)
	defer cancel()

	query := "supplier, invoice number, currency, total, and supporting labels"
	candidates, err := ranker.Candidates(ctx, query, chunks)
	if err != nil {
		return Invoice{}, "", err
	}
	ranked, err := ranker.Rerank(ctx, query, candidates)
	if err != nil {
		return Invoice{}, "", err
	}

	evidence := make([]Chunk, 0, len(ranked))
	used := 0
	for _, chunk := range ranked {
		n := tokens.Count(chunk.Text)
		if used+n > job.EvidenceLimit {
			continue
		}
		evidence = append(evidence, chunk)
		used += n
	}
	if len(evidence) == 0 {
		return Invoice{}, "", errors.New("no evidence fits the token budget")
	}

	result, err := extractor.Extract(ctx, evidence, job.SchemaVersion)
	if err != nil {
		return Invoice{}, "", err
	}
	if err := validator.Check(result, evidence); err != nil {
		return Invoice{}, "", err
	}

	sum := sha256.Sum256([]byte(job.DocumentID + ":" + job.SchemaVersion))
	publicationKey := hex.EncodeToString(sum[:])
	return result, publicationKey, nil
}
```

The worker should not publish from `Run` itself. Return the validated payload and publication key to a narrow commit step that can atomically record the key and outcome. If the queue redelivers while the first attempt is still running, both attempts may finish extraction, but only one key may cross that boundary. Duplicate compute is annoying. Duplicate accounting data is an incident.

Concurrency needs its own cap. A large invoice fans out into field queries and chunks, so worker count multiplied by per-job fan-out is the real pressure on downstream capacity. Put the semaphore around remote work, preserve enough deadline for validation and commit, and reject new fan-out before the process is saturated. A queue that looks healthy while every active job is waiting on the same dependency is not healthy.

## Choose a lane with a quality-versus-latency dispatch matrix

Build a replay set from representative supplier documents with reviewed JSON and evidence locations. Keep difficult layouts: multi-page tables, repeated subtotals, OCR noise, credit notes, and invoices containing more than one currency. Measure exact schema validity, field-level precision and recall, evidence coverage, and end-to-end latency by percentile. Averages hide the documents that page an operator.

| Observed condition | Dispatch lane | Required check |
| --- | --- | --- |
| Short input fits the context and deadline | Direct extraction | Schema and field validation |
| Long input has locally identifiable fields | Chunk, retrieve, and rerank | Evidence coverage plus field validation |
| Layout carries meaning that text loses | Layout-aware parsing or review | Visual correspondence to the source |
| Remaining budget cannot cover validation and commit | Deferred scheduling | No partial result is published |

Quality and latency belong in the same release decision, but they shouldn't be compressed into one magic score. A configuration can pass only when required fields meet their quality thresholds and the slow tail fits the scheduler deadline with room for validation and publication. Compare at least the current configuration, a smaller evidence budget, and a larger candidate set. Record the chunker version, retrieval query, reranker version, model configuration, prompt digest, and schema version for every replay so a regression can be reproduced.

Then test failure timing. Cancel during retrieval, after extraction, and immediately before commit. Redeliver the same job. Confirm that checkpoints resume only from compatible inputs, partial JSON never becomes visible, and the publication key admits one result. Force a chunk boundary through a table row and verify that structural overlap recovers the label-value pair. These tests matter more than a demo that succeeds on one short invoice.

Watch production with stage-level duration, queue age, attempt count, evidence token count, selected chunk count, validation outcome, and publication-key conflicts. Don't log raw invoice text by default; operational metadata and stable document IDs are usually enough to diagnose scheduling. Alert on user-visible outcomes such as deadline exhaustion, sustained queue age, missing required fields, and duplicate commit attempts. A single slow model call is a symptom, not the service objective.

The limitation is real: retrieval-first extraction is not suitable when correctness depends on the entire document being jointly visible, or when tables cannot be reconstructed from the available text and coordinates. Keep a whole-document lane when the input safely fits its context and deadline. Use a document parser or human review path when layout fidelity dominates language interpretation. Stick with a simpler direct extraction for short, consistent invoices; extra retrieval stages add latency, operational state, and more configurations to evaluate.

## Verification and rollback share one ledger

Rollback means restoring a known configuration tuple, not merely redeploying yesterday's worker image. Pin the normalizer, chunker, retrieval query, reranker, extraction prompt, model configuration, and JSON schema together. Stop admission to the suspect version, allow validated commits to finish, and quarantine unvalidated attempts. Resume those jobs from the last checkpoint whose input digest matches the restored tuple.

Do not mix evidence selected by one ranking configuration with an extractor from another release.

After rollback, replay the affected document IDs through the labeled checks before reopening normal concurrency. Compare field changes and evidence IDs, then inspect the few cases with the largest quality delta. If the old configuration meets quality but misses the latency objective, keep it in the slow lane while the new candidate is evaluated. That is an explicit capacity trade-off, not a reason to weaken validation or extend every timeout.

My default for long supplier invoices is therefore a scheduled, retrieval-first pipeline with an idempotent commit boundary. It earns that default only while replayed evidence shows that chunking and reranking preserve required fields within the deadline. Short documents stay on the direct path, and layout-dependent exceptions leave the automated lane. Clear lanes make the pager quieter because each failure has an owner and a next action.

## References

- RFC 9110, “HTTP Semantics”: https://www.rfc-editor.org/rfc/rfc9110

## Further reading

- LiteLLM, an open-source self-hosted LLM gateway: https://github.com/BerriAI/litellm
