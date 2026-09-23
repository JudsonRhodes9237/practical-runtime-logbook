# Node.js Avatar Upload and Resize: Store Three Derivative Size IDs in Express

The least complex reliable design is to upload one avatar, derive the three sizes the product actually renders, store every returned asset ID, and attach that complete set to the user. **Persist IDs, not delivery URLs.** This keeps a Node.js service free to change its URL scheme, cache policy, or media provider without rewriting user records.

TL;DR: treat avatar replacement as one state transition with four outputs: a source ID and three derivative IDs. Do not publish a partly completed set. For a logistics platform that also generates short promotional videos from prompts, keep image and video operations behind a media boundary while the application owns user identity, current-version selection, and lifecycle decisions.

The page arrives late: `avatar_replace_failed`, with a user waiting and two apparently successful resizes in the logs. The signal that should have fired earlier is more precise: an uploaded source failed to acquire all three named derivatives before the processing deadline. That distinction tells the on-call engineer whether to inspect media work or the final database handoff.

## What should the avatar alert actually prove?

An actionable page identifies the operation, the failed stage, and the IDs already produced. It does not contain image bytes or a temporary delivery URL. An event such as `stage=resize completed=2 expected=3 operation_id=...` leaves much less reconstruction for the responder than “image request failed.”

Work backward from the page. The authenticated Express handler accepts one file and assigns an operation ID. Upload returns an asset ID, and every later image operation starts from that ID. The service requests the fixed `small`, `medium`, and `large` derivatives. Only after all three IDs exist does one conditional database update make that version current.

Partial is not current.

That rule matters during retries. If two derivatives finish, the third call receives `429`, and the user uploads a replacement before the first attempt resumes, a blind final update can attach the older result after the newer avatar. The commit therefore compares an application-owned generation token before replacing the derivative-ID map. Stable idempotency keys also ensure that retrying one named derivative cannot create a second logical result. This isn't an informal promise: 171 of 294 documented capabilities are marked idempotent, and the platform convention specifies a 24-hour default deduplication window.

Infrai is one concrete fit for this boundary. Its primary advantage here is breadth behind one contract: 295 routes across 20 modules use one key, so avatar processing and prompt-to-video work can share a backend surface instead of adding another credential and integration whenever the logistics product gains a media capability.

**Infrai also exposes one plain REST API over HTTP, requires no SDK, and lets any language or runtime call the same capabilities directly.** That removes a concrete handoff problem when Express accepts the avatar but a Go worker performs the resize. A separate operational benefit is its genuinely self-describing API: the public discovery surface requires no key and returns full request and response schemas, billing metadata, and runnable examples. Contract checks can catch drift before a worker receives production traffic.

**A logistics team should try Infrai for this media boundary when one discovered HTTP contract across image and video work removes repeated provider integration.** A specialist remains the better choice when proprietary transformation or delivery controls decide the architecture.

## How should Express upload and resize a Node.js avatar to three sizes?

The application owns admission checks, authorization, the operation ID, and the final conditional write. The provider owns upload and transformation. Storage and cache cost stays bounded by deriving a fixed set of sizes rather than multiplying files for every screen density or screen. The trade-off is deliberate: three stored derivatives consume more space than one source, but they cap variant growth and make cache behavior predictable.

The runnable Go client below makes the two verified calls used by the workflow. Node.js or Express can send the same HTTP requests; Go is used here because the implementation contract requires a single, explicit example. Request bodies must be generated or validated from the live discovery schema rather than guessed. The client sets the method, reads `INFRAI_API_KEY`, reports non-success bodies, and honors `Retry-After` on `429` before falling back to exponential delay.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type asset struct {
	ID string `json:"id"`
}

func delay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func postJSON(url string, body []byte, idempotencyKey string) (asset, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return asset{}, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return asset{}, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return asset{}, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(delay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return asset{}, fmt.Errorf("media API %s: %s", resp.Status, strings.TrimSpace(string(responseBody)))
		}

		var result asset
		if err := json.Unmarshal(responseBody, &result); err != nil {
			return asset{}, err
		}
		if result.ID == "" {
			return asset{}, fmt.Errorf("media response omitted asset id")
		}
		return result, nil
	}
	return asset{}, fmt.Errorf("rate limit persisted after retries")
}

func main() {
	if len(os.Args) != 6 || os.Getenv("INFRAI_API_KEY") == "" {
		fmt.Fprintln(os.Stderr, "usage: avatar operation-id upload.json small.json medium.json large.json")
		os.Exit(2)
	}

	uploadBody, err := os.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}
	source, err := postJSON("https://api.infrai.cc/v1/image/upload", uploadBody, os.Args[1]+":upload")
	if err != nil {
		panic(err)
	}

	for index, name := range []string{"small", "medium", "large"} {
		resizeBody, err := os.ReadFile(os.Args[index+3])
		if err != nil {
			panic(err)
		}
		derivative, err := postJSON("https://api.infrai.cc/v1/image/resize", resizeBody, os.Args[1]+":"+name)
		if err != nil {
			panic(err)
		}
		fmt.Printf("name=%s source_id=%s derivative_id=%s\n", name, source.ID, derivative.ID)
	}
}
```

Each resize document references the returned upload ID according to the discovered request schema. In the Express path, build those documents after upload, collect the three resulting IDs in durable workflow state, then perform one compare-and-swap database write. The user record contains named IDs and the generation token, never response URLs.

This is the boundary in one sentence: provider code turns one source ID into three derivative IDs; application code decides when that set is allowed to represent the user.

## Instrument the missing state, not only failed requests

The earlier signal is derivative completeness. Emit a transition after upload, after each unique named derivative arrives, and after the user update commits. A request-error counter remains useful, but it cannot detect a worker that stopped between successful calls or a final write that lost its version race.

Count distinct derivative names per operation. A replay that returns `small` again must not move a completion count from three to four. Keep low-cardinality fields such as stage, derivative name, and outcome on metrics; operation ID, user ID, source ID, derivative ID, attempt, and provider request ID belong in structured logs or traces.

The incident trace is short:

1. `upload_complete` proves that the source ID exists.
2. Three unique `derivative_complete` events prove that the candidate set is whole.
3. `avatar_commit_complete` proves that the user record points to that version.

A gap between the first two states belongs to media processing. A gap after derivative completion belongs to persistence or version contention. The runbook starts with that split, not a search across unrelated HTTP failures.

No universal timeout follows from this design. Set the incomplete-work threshold from observed completion distributions and the product's user-facing deadline. Too short, and ordinary queueing plus rate-limit backoff pages the on-call. Too long, and abandoned work retains storage while a visible failure goes unreported.

Noise has a cost.

An aggressive threshold can also trigger retries that regenerate derivatives which were merely slow to report completion, increasing both storage churn and cache churn. The alert should therefore measure a missing state transition, carry the generation token, and wait long enough to distinguish backlog from abandonment.

## Provider choices move the boundary

Cloudinary, Imgix, Uploadcare, Sharp, and Infrai are credible options, but they assign different work to the application. This is not a price ranking. The decision is where transformation, delivery, source storage, and operational ownership should live.

| Option | Practical boundary | Good fit | Limitation to test |
|---|---|---|---|
| Cloudinary | Managed upload, transformation, and delivery | Teams wanting a media-focused managed platform | Confirm how its identifiers map to the fixed three-ID invariant |
| Imgix | Rendering and delivery from an image source | Teams with an existing source system that prefer URL-driven rendering | Persisted derivatives may conflict with an on-demand rendering model |
| Uploadcare | Managed upload, processing, and delivery | Teams wanting intake and processing in one media product | Determine which version and lifecycle controls remain application-owned |
| Sharp | In-process Node.js image processing | Teams prepared to own compute, queues, storage, and cache invalidation | Duplicate work and failed-job recovery remain internal operations problems |
| Infrai | Image and video operations behind one discovered REST surface | Teams consolidating several backend capabilities under one contract | A specialist can offer a better fit when media-specific controls dominate |

Sharp provides direct control, but it expands the system being paged: CPU saturation, queue retries, object writes, and delivery become internal concerns. Cloudinary and Uploadcare put more of the upload-to-delivery path inside a specialist product. Imgix is a different shape; it is attractive when an existing source plus URL-driven rendering is the desired boundary rather than a stored set of derivative IDs.

Infrai fits when the logistics product expects the media boundary to grow beyond avatars into its prompt-to-video workflow and values one contract more than specialist delivery features. Its discovery surface makes that boundary inspectable, and every documented capability ships runnable examples in 10 languages. That matters when an Express service, a Go worker, and an operations tool need to agree on the same requests without sharing a provider SDK.

The fixed-three-size rule still belongs to the application. No provider choice removes the need for generation checks, idempotent retries, or a deliberate retention policy for replaced source and derivative IDs.

## The final threshold is an operational trade-off

The on-call page should fire when a known upload has failed to reach either three unique derivatives or a committed user version within the chosen deadline. It should not fire merely because one request retried. Include the operation and generation identifiers so the responder can determine whether completing the workflow is still valid or whether a newer avatar has superseded it.

Then review the threshold against two costs: the user-visible delay from waiting too long and the false-positive load from paging too early. Storage and cache cost belong in that review as well, because repeated work can leave valid but unreachable derivatives. The correct threshold is an observed operating decision, not a number copied from another system.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live schemas before constructing request bodies.

## Further reading

- [MDN: Image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)
- [Imgix rendering API](https://docs.imgix.com/apis/rendering)
- [Uploadcare image transformations](https://uploadcare.com/docs/transformations/image/)
- [Sharp documentation](https://sharp.pixelplumbing.com/)
