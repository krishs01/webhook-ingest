package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	var eventCount int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("expected 1 event row, got %d", eventCount)
	}

	var gotAccount string
	row = st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	// Only one event row should exist.
	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}

	// Account stats must reflect exactly one call, not three.
	var callCount int64
	row = st.Pool().QueryRow(ctx,
		`SELECT call_count FROM account_stats WHERE account_id = $1`, accountID)
	if err := row.Scan(&callCount); err != nil {
		t.Fatalf("account_stats scan: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("account_stats.call_count = %d, want 1", callCount)
	}
}

// TestConcurrentDuplicateDelivery fires 20 goroutines that all POST the same
// event_id at the same time. This exercises the TOCTOU race that the old
// check-then-insert pattern (EventExists → InsertEvent) was vulnerable to.
// Without the UNIQUE constraint + ON CONFLICT DO NOTHING fix, multiple
// concurrent deliveries would slip through the existence check, insert
// duplicate rows, and inflate account_stats.
func TestConcurrentDuplicateDelivery(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)

	// All goroutines start at the same instant to maximise contention.
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start
			resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
			if err != nil {
				t.Errorf("post: %v", err)
				return
			}
			_ = resp.Body.Close()
			// Every delivery must be accepted — the provider expects 200.
			if resp.StatusCode != http.StatusOK {
				t.Errorf("got %d, want 200", resp.StatusCode)
			}
		}()
	}
	close(start) // release all goroutines at once
	wg.Wait()

	// Exactly one event row must exist.
	var eventCount int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("events: stored %d copies, want 1", eventCount)
	}

	// Account stats must reflect exactly one call, not 20.
	var callCount int64
	row = st.Pool().QueryRow(ctx,
		`SELECT call_count FROM account_stats WHERE account_id = $1`, accountID)
	if err := row.Scan(&callCount); err != nil {
		t.Fatalf("account_stats scan: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("account_stats.call_count = %d, want 1", callCount)
	}
}
