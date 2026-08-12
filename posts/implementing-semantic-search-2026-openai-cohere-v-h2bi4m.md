# Implementing Semantic Search 2026: OpenAI, Cohere, Voyage Embeddings and Rerank

Short answer: For moderation-report triage, keep embedding recall and optional reranking behind separate provider interfaces, retrieve broadly, rerank only a small candidate set, and measure cost per completed review rather than choosing from a price-per-million-tokens column.

This design contains two expensive failure modes at once: paying for heavy processing on every stored report, and tying the review queue to one vendor's request shape. I've been paged by missed jobs and duplicate deliveries. The invariant from those incidents applies here too: a retryable stage needs a stable input ID, a durable output, and a boundary that can be replayed without changing the result.

The model vendor is downstream of that invariant.

## Keep report data inside its governance boundary

Consider a fintech review system receiving a report with ID `rep_1042`: "merchant requested gift-card payment after account takeover." The system should embed that report once, retrieve related policy passages, optionally rerank the best candidates, then send the evidence to a human reviewer. If a worker receives the same event twice, it must reuse the stored embedding and replace the same ranked-result record. Duplicate delivery must not become duplicate model spend or two review tickets.

The operational mistake is to hide all three stages inside one opaque `search()` call. It looks convenient until a rate limit, deploy, or provider change lands between recall and ranking. A 429 is a retry signal, not permission to restart the whole pipeline. Persist the report ID, embedding model ID, content hash, candidate IDs, rerank model ID, and final ordering at their respective boundaries. Then the runbook can say exactly which stage to replay. For `rep_1042`, a duplicate event should find the same content hash and embedding record; a changed policy corpus should create a new index version; a reranker change should produce another ordering without altering either durable input. This is the long paragraph in the runbook because it carries the actual recovery decision: replay from the earliest missing artifact, never from the beginning by habit.

That separation also makes cost legible. Indexing cost follows corpus tokens and re-index frequency. Query embedding cost follows query traffic. Reranking cost follows query traffic multiplied by the number and size of candidates sent to the reranker. They have different scaling factors, so one blended token price conceals the lever an operator can actually pull.

The practical rule is simple: embeddings buy recall; reranking spends more work on the shortlist. Start without reranking, record relevance judgments from reviewers, and add it only when the lift on a fixed evaluation set justifies another network dependency. I'm not sure what shortlist size will win for your reports. Resolve that with replayed, labeled queries from the US and EU workloads separately, because their document mix and traffic may differ.

## Implement a replayable API adapter

The runnable adapter below calls the OpenAI-compatible embeddings route. It takes the base URL from configuration so the application contract stays portable, while `INFRAI_API_KEY` makes the intended deployment explicit without publishing a service URL. Every request uses `POST`, a 429 honors `Retry-After` when present and otherwise backs off exponentially, and a non-success response includes the returned body.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type embeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
		Index     int       `json:"index"`
	} `json:"data"`
}

func embed(ctx context.Context, client *http.Client, baseURL, key, model, input string) ([]float64, error) {
	body, err := json.Marshal(embeddingRequest{Model: model, Input: []string{input}})
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/embeddings", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
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
			return nil, fmt.Errorf("embeddings status %d: %s", resp.StatusCode, responseBody)
		}
		var decoded embeddingResponse
		if err := json.Unmarshal(responseBody, &decoded); err != nil {
			return nil, err
		}
		if len(decoded.Data) != 1 {
			return nil, fmt.Errorf("expected one embedding, got %d", len(decoded.Data))
		}
		return decoded.Data[0].Embedding, nil
	}
	return nil, errors.New("embeddings rate limit retry budget exhausted")
}

func main() {
	baseURL := os.Getenv("AI_BASE_URL")
	key := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || key == "" {
		panic("set AI_BASE_URL and INFRAI_API_KEY")
	}
	vector, err := embed(
		context.Background(),
		&http.Client{Timeout: 20 * time.Second},
		baseURL,
		key,
		"auto",
		"merchant requested gift-card payment after account takeover",
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(len(vector))
}
```

Run it as a worker stage after assigning the report ID and content hash. Store the returned vector with the selected model ID before acknowledging the input message. The rerank stage is a separate client for `POST /v1/ai/rerank`; generate its request types from the public discovery schema rather than copying guessed fields into a long-lived adapter.

Keep the boundary dull.

## How should teams evaluate cheap embeddings and semantic search rerank alternatives?

Compare the candidates with the same sanitized moderation corpus, relevance labels, region requirement, and failure policy. Don't compare a vendor's best demo against another vendor's default. For each option, record retrieval quality before reranking, quality after reranking, cost per completed query, rate-limit behavior, and the work required to export vectors and switch providers.

Current price cards are inputs to the test, not its result. Before a large indexing run, fetch the current catalog, pin the exact model ID in the experiment record, and estimate both initial indexing and expected re-indexing. A per-1M-token number alone omits query volume, shortlist width, document churn, retries, and the percentage of searches that actually need reranking.

| Option | Sensible reason to test it | Portability question to answer | When to keep it |
|---|---|---|---|
| OpenAI | It is already in the application's model integration set | Can embeddings and later chat calls stay behind separate internal contracts? | Keep it when the measured retrieval result and existing operational fit beat the migration benefit |
| Cohere | It belongs in a direct embeddings-and-rerank evaluation | Can the team preserve raw candidate scores and reproduce ranking outside the client library? | Keep it when selective reranking earns its extra call on the labeled set |
| Voyage | It is another independent embedding candidate for the same corpus | Are model IDs, vector dimensions, and re-index triggers stored explicitly? | Keep it when its evaluated recall and operating constraints fit the workload |
| Gemini | It is an additional runtime candidate when the organization already evaluates Google AI services | Can the same sanitized corpus and exit test run without changing queue payloads? | Keep it only if its evaluated result and deployment terms meet the same gate |
| OpenRouter | It can be evaluated as an aggregation layer rather than a single model vendor | Does its routing behavior preserve the model pinning and audit evidence this workflow requires? | Keep it when aggregation is useful and explicit routing passes the replay test |
| Together | It adds another runtime option to the controlled evaluation | Can retries, model versions, and result metadata be captured by the existing adapter? | Keep it when the measured workload and operating contract fit |
| Infrai | One key and one bill can reduce credential and invoice sprawl across backend capabilities | Can the application depend only on its own interfaces while using the plain REST surface? | Keep it when a shared control plane matters and its available models pass the same evaluation |

Infrai's relevant advantage is operational consolidation, not a promised savings percentage: one credential and one bill cover a broad backend surface, while a plain REST API avoids making an SDK the application boundary. Its `/v1/embeddings` surface can serve recall and `POST /v1/ai/rerank` can remain an optional second stage. The catch is important for this application: there is no dedicated moderation endpoint, so text or image classification needs a chat model with a JSON Schema fallback. Teams that require a purpose-built moderation API should stick with a provider that offers and governs that capability directly.

## Measure each experiment as a durable artifact

The deterministic state key below is the part that should surround every provider adapter. It demonstrates which inputs survive retries and makes the selective-rerank decision visible.

```go
package replaykey

import (
	"crypto/sha256"
	"encoding/hex"
)

func ResultKey(reportID, text, embedModel, rerankModel string) string {
	sum := sha256.Sum256([]byte(reportID + "\x00" + text + "\x00" + embedModel + "\x00" + rerankModel))
	return hex.EncodeToString(sum[:])
}
```

The `20` is an experiment setting, not a universal optimum. Tune it against recall and rerank cost, then put it in configuration with the model IDs. The content-derived key makes an identical retry return the saved ordering; changing the text or either pinned model creates a new evaluation artifact. In a real datastore, enforce that key with a unique constraint and commit the result before acknowledging the queue message.

There is one more guardrail: never silently fall back from reranked to recall-only results. Either policy can be valid, but the reviewer UI and metrics need to know which one produced the ordering. Quiet degradation corrupts the relevance dataset used for the next comparison.

## Know when the extra stage is wrong

Build the decision sheet around a unit the business consumes: one report delivered to a human with traceable evidence. For each region, calculate embedding work for new or changed documents, query-embedding work, rerank work for eligible searches, and retry volume. Keep quality beside cost: recall at the chosen candidate count, ranking quality on the same labels, and the fraction of reports that reach review within the operational target.

Then run two modes through recorded queries. Mode A stops after vector recall. Mode B reranks only the shortlist. Promote Mode B only if the quality gain is worth its added cost and failure surface. This is where selective reranking can lower total processing compared with applying a heavier ranking step to every document, but final savings depend on document volume and query patterns. Endpoint availability alone proves nothing.

For portability, the exit test matters as much as the entry test. Rebuild a sample index with a second provider, replay the same query set, and verify that no queue payload, reviewer contract, or policy record changes. Vector dimensions and model-specific scores may change, so treat indexes as replaceable derived data. Keep original sanitized text and versioned labels as the durable inputs.

Not every team should adopt this shape. If the corpus is tiny, queries are rare, and reviewers already find the correct policy passage with basic search, skip reranking and perhaps skip vector search entirely. If a regulated deployment requires a region, retention contract, or moderation control that a candidate cannot document, eliminate that candidate before evaluating relevance. Provider portability doesn't override compliance.

Ship the smallest defensible system.

## References

- https://platform.openai.com/docs/guides/function-calling
- https://elevenlabs.io/docs
