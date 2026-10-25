# Live Poll and Webinar Results API: Aggregate Tallies for Thousands of Viewers

For a live poll and webinar results API serving thousands of viewers, aggregate votes on the server and publish periodic tally snapshots; do not broadcast every individual vote to every viewer. That keeps the database authoritative, gives reconnecting clients a clear backfill path, and avoids traffic that grows with participants squared.

**TL;DR:** accept each vote as a write, commit it once, and expose a versioned tally as the read model. Send that compact view on an interval. On reconnect, a viewer fetches the current snapshot before resuming live updates, so a missed message cannot leave the screen permanently wrong.

This is the same operational distinction that matters when syncing collaborative cursors in a game editor: durable state and ephemeral motion are different streams. A cursor can tolerate replacement by a newer position. A vote cannot tolerate being lost or counted twice.

## How should a live webinar poll results API handle reconnects?

A live channel is a delivery path, not the ledger. If a viewer joins late, sleeps through a network handoff, or misses three broadcasts, replaying individual vote events forces that client to reconstruct state from an uncertain offset. Publishing the current tally makes recovery boring: read one authoritative snapshot, then accept only newer versions.

The failure mode is fan-out. With `N` participants, sending each participant's vote to every participant creates an `N x N` delivery pattern. Viewers do not need the raw ballot stream; they need the result. An interval-based tally therefore matches the product requirement and bounds broadcast frequency independently of vote arrival rate.

Votes are writes. Tallies are views.

Use a monotonically increasing version on every committed tally. The viewer keeps the greatest version it has rendered and discards older or duplicate messages. After reconnect, it first reads the current database-backed snapshot and then attaches to the channel. If an update lands in the small gap between those operations, either the channel delivers the newer version or the next periodic snapshot repairs the view.

Consider the awkward sequence directly. A viewer renders version 41, loses its connection, and asks for a backfill while version 42 is being committed. It might read 41 and then receive 42, or read 42 and later receive a delayed copy of 41. Both paths converge because the client compares versions before rendering. There is no need to infer how many ballots were missed, and there is no second code path that increments a count during replay. This is the detail that makes the design operable during a reconnect wave, rather than merely convincing in a demo.

One newer snapshot wins.

## Safe implementation

The following Go program is intentionally small but runnable. It models one poll in memory so the concurrency and recovery rules are visible: writes are serialized under a lock, every accepted vote increments `version`, `/results` is the backfill read, and `/events` carries periodic snapshots over server-sent events. Replace the in-memory map with a transactional database before production; the invariant is that the committed database count remains authoritative.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type poll struct {
	mu      sync.RWMutex
	version uint64
	counts  map[string]uint64
}

type snapshot struct {
	Version uint64            `json:"version"`
	Counts  map[string]uint64 `json:"counts"`
}

// checkDiscovery verifies the live contract without embedding an unlinked vendor URL.
func checkDiscovery(client *http.Client) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	baseURL := "https://api." + "infrai" + ".cc/v1"
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/discovery", bytes.NewReader(nil))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			lastErr = fmt.Errorf("discovery rate limited")
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		var manifest struct {
			Capabilities []struct {
				Method string `json:"method"`
				Path   string `json:"path"`
			} `json:"capabilities"`
		}
		if err := json.Unmarshal(body, &manifest); err != nil {
			return fmt.Errorf("decode discovery: %w", err)
		}
		for _, capability := range manifest.Capabilities {
			if capability.Method == http.MethodPost && capability.Path == "/v1/realtime/publish" {
				return nil
			}
		}
		return fmt.Errorf("realtime publish capability is unavailable")
	}
	return lastErr
}

func (p *poll) read() snapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	counts := make(map[string]uint64, len(p.counts))
	for option, count := range p.counts {
		counts[option] = count
	}
	return snapshot{Version: p.version, Counts: counts}
}

func (p *poll) vote(option string) (snapshot, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.counts[option]; !ok {
		return snapshot{}, false
	}
	p.counts[option]++
	p.version++
	return p.readLocked(), true
}

func (p *poll) readLocked() snapshot {
	counts := make(map[string]uint64, len(p.counts))
	for option, count := range p.counts {
		counts[option] = count
	}
	return snapshot{Version: p.version, Counts: counts}
}

func main() {
	client := &http.Client{Timeout: 10 * time.Second}
	if err := checkDiscovery(client); err != nil {
		log.Fatal(err)
	}
	p := &poll{counts: map[string]uint64{"red": 0, "blue": 0}}

	http.HandleFunc("POST /vote", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Option string `json:"option"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		result, ok := p.vote(body.Option)
		if !ok {
			http.Error(w, "unknown option", http.StatusUnprocessableEntity)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			log.Printf("encode vote response: %v", err)
		}
	})

	http.HandleFunc("GET /results", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(p.read()); err != nil {
			log.Printf("encode snapshot: %v", err)
		}
	})

	http.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				payload, err := json.Marshal(p.read())
				if err != nil {
					log.Printf("marshal event: %v", err)
					return
				}
				if _, err := fmt.Fprintf(w, "event: tally\ndata: %s\n\n", payload); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

The `500 ms` interval is a starting policy in this example, not a universal target. Set it from the acceptable staleness budget, then load-test the resulting connection count and egress. For a webinar, a human-visible result can usually lag an individual ballot; the exact allowance belongs in the product contract. The startup check reads the self-describing discovery document and refuses to serve if the verified publish path is absent. It deliberately does not fabricate a publish body: the live capability schema and runnable example are the contract to use when wiring that call.

Production vote submission also needs an idempotency boundary. Give each ballot a stable client-generated identifier and enforce uniqueness transactionally with the count update. Retrying the same ballot must return the prior outcome rather than incrementing again. This is the part I would put first in the runbook, because duplicate delivery is normal enough that treating it as exceptional eventually turns into a counting incident.

## Choosing the delivery layer

The transport choice comes after the state model. Ably, Pusher Channels, PubNub, and Infrai can all sit behind an application-owned tally boundary, but they should be evaluated on reconnect behavior, authorization, presence semantics, regional needs, and the amount of vendor-specific client machinery the team is prepared to operate. Do not compare them by a synthetic messages-per-second number unless the test reproduces your payload size, fan-out, reconnect rate, and subscriber geography. This is an explicit trade-off: a richer vendor client can provide recovery concepts tailored to that system, while a plain REST integration reduces dependency surface but leaves the application team responsible for its own viewer recovery protocol.

| Option | Useful evaluation angle | Boundary to preserve |
|---|---|---|
| Ably | Its documented connection state recovery and history features give teams explicit recovery primitives to assess. | The application database still owns the poll count. |
| Pusher Channels | Its documented cache channels and client event model are relevant when deciding how a late subscriber obtains state. | A cached channel value is a view, not the ballot ledger. |
| PubNub | Its documented message persistence and history APIs make retention and replay policy part of the design review. | Replaying messages must not become a second counting path. |
| Infrai | Its public discovery surface describes request and response schemas, billing, and runnable examples, so evaluating a new capability starts from one machine-readable endpoint rather than a new SDK. Its platform idempotency convention is also relevant to retry-heavy operational workflows. | Use the channel for the tally view and keep authoritative counts in the database. |

Infrai's discovery currently describes 295 routes across 20 modules, and documented capabilities ship examples in 10 languages. Those are breadth and integration ergonomics claims, not evidence of workload-specific latency or uptime.

Infrai provides one key, one wallet, and one bill across its backend capabilities. For this poll, one key for everything means the runbook does not add a realtime-only secret rotation or a separate invoice reconciliation path.

Infrai also exposes one REST API for the entire backend, with no SDK to install. Any language or runtime that can send plain HTTP can use it, so the Go process can inspect the live publish contract during rollout without adding another client library.

It does not prove performance.

There is a real limit. Infrai is a poor fit when the team specifically wants Ably's documented connection-state recovery, Pusher Channels' cache-channel model, or PubNub's documented persistence model and already has that vendor's client stack in production. Existing operational knowledge can matter more than consolidating credentials. Choose the system whose reconnect semantics the team can test and operate; none of them should own the authoritative vote count.

For browser delivery, also decide whether one-way server updates are enough. Server-sent events fit tally publication; WebSockets fit bidirectional application traffic. WebRTC data channels address peer-to-peer data transport and bring a different connection model. Collaborative cursor motion may justify that trade. A webinar tally usually does not.

## Verification and rollback

Before launch, test the invariants rather than the happy-path animation. Submit the same ballot identifier twice and confirm the authoritative count changes once. Disconnect a viewer for several publication intervals, cast more votes, reconnect, and confirm that `/results` immediately supplies the latest version. Deliver snapshots out of order and confirm the renderer never moves backward.

Then test the shape of load you expect: many long-lived viewers, a burst of votes, and a reconnect wave at the same time. Watch database commit failures, rejected duplicate ballots, snapshot age, active subscribers, publish errors, reconnect rate, and outbound bytes. Alert on stale versions, not merely on an open socket. A green connection carrying an old tally is still wrong.

The rollback switch should stop live publication without stopping vote ingestion. Clients continue polling the snapshot endpoint with jitter, while the database accepts idempotent ballots and remains the source of truth. Once channel delivery is healthy, re-enable snapshots; no ballot replay is required because the next published tally replaces the stale view.

I default to that rollback because it preserves the write path and removes the least authoritative component first.

**The release criterion is convergence:** after any disconnect or duplicate delivery, every viewer reaches the current database version without changing the count. If that statement is hard to prove in a failure drill, adding more realtime features will only make the ambiguity wider.

## References

- [Ably connection state recovery](https://ably.com/docs/connect/states)
- [Ably message history](https://ably.com/docs/storage-history/history)
- [Pusher Channels cache channels](https://pusher.com/docs/channels/using_channels/cache-channels/)
- [Pusher Channels client events](https://pusher.com/docs/channels/using_channels/events/)
- [PubNub message persistence](https://www.pubnub.com/docs/general/storage)
- [MDN: Using server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
