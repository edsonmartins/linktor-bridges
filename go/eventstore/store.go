// Package eventstore provides a small durable queue for bridge events (inbound
// messages, status) so that an outage of the WebSocket link to Linktor does not
// lose events that arrived while the device was offline.
//
// It is deliberately tiny: one SQLite table holding JSON payloads with a
// monotonic sequence. The bridge drains the queue and clears acknowledged rows.
package eventstore

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Store is a durable FIFO queue backed by SQLite.
type Store struct {
	mu *sync.Mutex
	db *sql.DB
}

// Open opens (or creates) the store at the given path.
func Open(path string) (*Store, error) {
	dsn := "file:" + path + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open event store: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping event store: %w", err)
	}
	schema := `CREATE TABLE IF NOT EXISTS events (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		kind TEXT NOT NULL,
		payload TEXT NOT NULL,
		created_at INTEGER NOT NULL
	)`
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create event store schema: %w", err)
	}
	return &Store{mu: &sync.Mutex{}, db: db}, nil
}

// Enqueue appends an event. kind is a short label ("inbound"); payload is the
// raw JSON to forward.
func (s *Store) Enqueue(kind string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT INTO events (kind, payload, created_at) VALUES (?, ?, ?)`,
		kind, string(payload), time.Now().Unix(),
	)
	return err
}

// Pending returns up to limit queued events in FIFO order, oldest first.
func (s *Store) Pending(limit int) ([]Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(
		`SELECT seq, kind, payload FROM events ORDER BY seq ASC LIMIT ?`, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []Item
	for rows.Next() {
		var it Item
		var payload string
		if err := rows.Scan(&it.Seq, &it.Kind, &payload); err != nil {
			return nil, err
		}
		it.Payload = []byte(payload)
		items = append(items, it)
	}
	return items, rows.Err()
}

// Acknowledge removes events with seq <= upto (they were delivered).
func (s *Store) Acknowledge(upto int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM events WHERE seq <= ?`, upto)
	return err
}

// Len reports the number of queued events.
func (s *Store) Len() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n)
	return n, err
}

// Close closes the underlying database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

// Item is one queued event.
type Item struct {
	Seq     int64
	Kind    string
	Payload []byte
}
