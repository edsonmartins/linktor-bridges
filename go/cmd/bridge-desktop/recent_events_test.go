package main

import (
	"encoding/json"
	"fmt"
	"testing"
)

// O painel lê uma cópia limitada aos últimos eventos, sem consumir nada.
func TestRecentEventsKeepsLatest(t *testing.T) {
	var r recentEvents
	for i := 0; i < recentEventsMax+5; i++ {
		r.add(json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)))
	}
	got := r.snapshot()
	if len(got) != recentEventsMax {
		t.Fatalf("len = %d, want %d", len(got), recentEventsMax)
	}
	if string(got[0]) != `{"n":5}` || string(got[len(got)-1]) != fmt.Sprintf(`{"n":%d}`, recentEventsMax+4) {
		t.Fatalf("janela errada: primeiro %s, último %s", got[0], got[len(got)-1])
	}
	if again := r.snapshot(); len(again) != recentEventsMax {
		t.Fatal("ler não pode consumir")
	}
}
