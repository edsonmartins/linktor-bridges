package eventstore

import (
	"path/filepath"
	"testing"
)

func TestEnqueuePendingAck(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	for _, p := range []string{`{"id":"1"}`, `{"id":"2"}`, `{"id":"3"}`} {
		if err := store.Enqueue("inbound", []byte(p)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	items, err := store.Pending(100)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("Pending len = %d, want 3", len(items))
	}
	// FIFO order.
	if string(items[0].Payload) != `{"id":"1"}` || string(items[2].Payload) != `{"id":"3"}` {
		t.Fatalf("FIFO order violated: %+v", items)
	}

	if err := store.Acknowledge(items[0].Seq); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	remaining, _ := store.Pending(100)
	if len(remaining) != 2 || string(remaining[0].Payload) != `{"id":"2"}` {
		t.Fatalf("after ack, want [2,3], got %+v", remaining)
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = store.Enqueue("inbound", []byte(`{"id":"kept"}`))
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	again, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer again.Close()
	items, _ := again.Pending(10)
	if len(items) != 1 || string(items[0].Payload) != `{"id":"kept"}` {
		t.Fatalf("event lost across reopen: %+v", items)
	}
}

func TestLenAndLimit(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "limits.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	for i := range 5 {
		_ = store.Enqueue("s", []byte{byte('a' + i)})
	}
	if n, _ := store.Len(); n != 5 {
		t.Fatalf("Len = %d, want 5", n)
	}
	items, _ := store.Pending(2)
	if len(items) != 2 {
		t.Fatalf("Pending(2) = %d items, want 2", len(items))
	}
}
