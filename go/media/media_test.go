package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFetch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok.jpg":
			w.Header().Set("Content-Type", "image/jpeg; charset=binary")
			_, _ = w.Write([]byte("jpegbytes"))
		case "/empty":
			w.WriteHeader(http.StatusOK)
		case "/big":
			w.Header().Set("Content-Type", "video/mp4")
			_, _ = w.Write(make([]byte, MaxOutboundBytes+1))
		default:
			http.Error(w, "nope", http.StatusForbidden)
		}
	}))
	defer srv.Close()
	dir := t.TempDir()

	path, mimeType, err := Fetch(context.Background(), srv.URL+"/ok.jpg", dir)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if mimeType != "image/jpeg" {
		t.Fatalf("mime = %q", mimeType)
	}
	if data, _ := os.ReadFile(path); string(data) != "jpegbytes" {
		t.Fatalf("conteúdo = %q", data)
	}

	for _, tc := range []struct{ name, url string }{
		{"403", srv.URL + "/nope"},
		{"vazio", srv.URL + "/empty"},
		{"acima do limite", srv.URL + "/big"},
		{"esquema", "ftp://x/y"},
	} {
		if _, _, err := Fetch(context.Background(), tc.url, dir); err == nil {
			t.Errorf("%s: esperava erro", tc.name)
		}
	}
	// Nenhum arquivo parcial fica para trás nos casos de erro.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("sobraram %d arquivos em %s", len(entries), dir)
	}
}

func TestInlineAttachments(t *testing.T) {
	dir := t.TempDir()
	okPath := filepath.Join(dir, "a.jpg")
	if err := os.WriteFile(okPath, []byte("jpegbytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"id":"M1","text":"oi","attachments":[` +
		`{"type":"image","mime_type":"image/jpeg","path":"` + okPath + `"},` +
		`{"type":"document","filename":"f.pdf","path":"` + filepath.Join(dir, "sumiu.pdf") + `"},` +
		`{"type":"video","error":"attachment exceeds 32 MB"}]}`)

	prepared, cleanup, err := InlineAttachments(payload)
	if err != nil {
		t.Fatalf("InlineAttachments: %v", err)
	}
	var event struct {
		Text        string              `json:"text"`
		Attachments []map[string]string `json:"attachments"`
	}
	if err := json.Unmarshal(prepared, &event); err != nil {
		t.Fatal(err)
	}
	if event.Text != "oi" || len(event.Attachments) != 3 {
		t.Fatalf("evento alterado: %s", prepared)
	}
	if event.Attachments[0]["data"] != base64.StdEncoding.EncodeToString([]byte("jpegbytes")) || event.Attachments[0]["path"] != "" {
		t.Fatalf("anexo 0: %v", event.Attachments[0])
	}
	if !strings.Contains(event.Attachments[1]["error"], "unavailable") || event.Attachments[1]["data"] != "" {
		t.Fatalf("anexo sumido: %v", event.Attachments[1])
	}
	if event.Attachments[2]["error"] != "attachment exceeds 32 MB" {
		t.Fatalf("anexo com erro: %v", event.Attachments[2])
	}

	cleanup()
	if _, err := os.Stat(okPath); !os.IsNotExist(err) {
		t.Fatal("arquivo enviado deveria ter sido apagado")
	}

	// Sem anexos o payload volta intacto e sem limpeza.
	same, clean, err := InlineAttachments([]byte(`{"id":"M2","text":"só texto"}`))
	if err != nil || clean != nil || string(same) != `{"id":"M2","text":"só texto"}` {
		t.Fatalf("payload sem anexo alterado: %s cleanup=%t err=%v", same, clean != nil, err)
	}
}
