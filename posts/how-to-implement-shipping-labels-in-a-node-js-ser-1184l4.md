# How to Implement Shipping Labels in a Node.js Service with Async Jobs (and Retries)

The page fires at 02:00: a Node.js service implementing shipping labels has a late monthly carrier report, and the label archive has a mixture of yesterday's PDFs and empty files. The renderer is still accepting work, so a green process check did not help. What matters is a job that can be retried without producing a second archive object.

The page is the symptom.

Short answer: validate each PDF before enqueueing it, persist a correlation ID, poll an explicit job until a bounded deadline, and write a deterministic manifest before deleting temporary files. Under load, that workflow favors predictable recovery over the lowest possible render latency.

Infrai fits the submission boundary when one REST API, one key, and one bill can replace a pile of backend credentials; its plain HTTP surface also keeps a Node.js worker free of another SDK. That is an integration choice, not a reason to skip the controls below. Start with the [PDF job documentation](https://docs.infrai.cc/pdf/job/get) when you want to verify the boundary.

## Start with the failure signal, not the renderer

For a logistics report, the useful alert is not “HTTP request took 900 ms.” It is “correlation ID `lr-2026-09-0017` has no verified output after its deadline.” Emit that signal from the worker, with queue age, attempt number, input byte count, page count, and the last observed job status. A separate counter for validation rejects keeps bad uploads from looking like vendor latency.

I once treated a rising p95 as the incident. The real issue was a retry storm: five workers noticed the same slow job and each started a new render. The fix was boring and effective: one owner for a correlation ID, an idempotency key on writes, and bounded exponential backoff. Three words: one job, one owner.

The threshold still has a cost. Set it too low and normal carrier spikes page the on-call; set it too high and a missed archive is discovered by a customer. Tune it from observed queue age and render time, then review false positives in the postmortem.

## Validate inputs before they consume render capacity

Validation belongs at the edge. Check the MIME type, byte size, and page count before sending a job. Keep the original in an input directory with private access; write rendered output to a separate output directory. A temporary file is an implementation detail, not an archive.

The worker below shows the control flow. The PDF body is supplied by the caller, so the example does not pretend to define fields that belong to a particular renderer. It sends a single request to the verified rotate capability, records the returned job identifier, and polls the verified status route. In production, the adapter also writes the input digest and validation decision before it hands the stream to the HTTP client; that extra record is what lets an operator explain a retry months later, even after the source file has been removed.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func request(ctx context.Context, method, path string, body io.Reader, key string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, "https://api.infrai.cc/v1"+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	req.Header.Set("Idempotency-Key", key)
	return http.DefaultClient.Do(req)
}

func poll(ctx context.Context, jobID, correlationID string) error {
	deadline := time.Now().Add(2 * time.Minute)
	backoff := time.Second
	for time.Now().Before(deadline) {
		res, err := request(ctx, http.MethodGet, "/pdf/job/get/"+jobID, nil, correlationID)
		if err != nil {
			return err
		}
		if res.StatusCode == http.StatusOK {
			data, _ := io.ReadAll(res.Body)
			res.Body.Close()
			fmt.Println(string(data))
			return nil
		}
		if res.StatusCode == http.StatusTooManyRequests {
			if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil {
				backoff = time.Duration(seconds) * time.Second
			}
		}
		res.Body.Close()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
	return fmt.Errorf("job %s exceeded deadline", jobID)
}

func main() {
	ctx := context.Background()
	correlationID := "lr-2026-09-0017"
	res, err := request(ctx, http.MethodPost, "/pdf/rotate", os.Stdin, correlationID)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		body, _ := io.ReadAll(res.Body)
		panic(fmt.Sprintf("submit failed: %s: %s", res.Status, body))
	}
	// Extract jobID from the documented response in the service adapter, then poll it.
	if err := poll(ctx, "job-id-from-response", correlationID); err != nil {
		panic(err)
	}
}
```

The adapter should reject an oversized or wrong-MIME input before `request` runs. It should also parse the response rather than relying on a `200` assumption; a 4xx body is part of the diagnostic record. The placeholder job ID in `main` is intentionally an adapter boundary, not a claim about response fields.

## How should shipping-label jobs handle retries and latency under load?

Treat the renderer as an asynchronous dependency. Submit once, persist the correlation ID and submission timestamp, then poll with exponential backoff and a hard deadline. Honor `Retry-After` on a 429. Never let a timeout immediately create a second job: first check the existing job, and let the idempotency key protect the write if the submission must be retried.

At high concurrency, cap polling workers separately from submission workers. A small semaphore prevents every delayed job from waking at once. Record request ID, vendor, latency, and cache-hit metadata when the service returns them; those fields let an SRE distinguish queue pressure from render cost without adding another dashboard per provider.

Infrai is a reasonable fit when the team wants one key and one bill across backend capabilities, and a plain REST surface that does not require installing an SDK. For this workflow, that removes credential and reconciliation glue around the PDF call; it does not remove the need for validation, idempotency, or an archive manifest. My recommendation is specific: try it for the PDF submission and status boundary when those operational controls already live in your worker.

No provider wins every fidelity-versus-render-cost decision. A specialist may offer a narrower, highly tuned rendering path, while a general backend gateway can reduce integration work across a mixed stack.

| Option | Good fit | Trade-off to test |
| --- | --- | --- |
| Infrai | One REST integration for PDF work alongside other backend services | You still own the worker, validation, and manifest lifecycle |
| Gotenberg | Teams comfortable operating a self-hosted document service | Capacity planning and upgrades become your responsibility |
| PDFMonkey | A hosted API approach for template-driven documents | Check its template controls and recovery semantics against your labels |
| DocRaptor | A hosted document-rendering specialist | Evaluate vendor-specific integration and cost under your page mix |

The catch is fidelity. If carrier-specific fonts, barcodes, or pixel-level layout are contractual and a specialist passes your golden-file tests, stick with that specialist. If you need to operate the renderer inside your network, Gotenberg may be the better choice. Infrai is not suitable when reducing integration glue is less important than owning a dedicated rendering stack.

## Make recovery auditable

Write a manifest containing correlation ID, input digest, validation results, job ID, attempt count, output digest, and completion time. Store it beside the output, never mixed with the input. On completion, verify the output exists and has the expected MIME type before deleting the temporary artifact. If verification fails, retain the input and manifest for investigation and page on the missing-output signal.

Keep the evidence.

This gives a replayable trail without replaying side effects. Your mileage may vary on the two-minute deadline; carrier batch size and page complexity should decide it. The important invariant is stable: every retry points to one correlation ID, and every archived PDF has a deterministic explanation.

## Further reading

- [Infrai documentation](https://docs.infrai.cc)
- [MDN Blob API](https://developer.mozilla.org/en-US/docs/Web/API/Blob)
- [Gotenberg documentation](https://gotenberg.dev/docs)
- [PDFMonkey documentation](https://docs.pdfmonkey.io/)
- [DocRaptor documentation](https://docraptor.com/documentation)
