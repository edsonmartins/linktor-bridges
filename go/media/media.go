// Package media move arquivos de mídia entre o Linktor e o wacore: baixa a
// URL de um outbound para um arquivo e troca os caminhos de um inbound por
// base64 na hora de enviar. Fica fora do wacore para o pacote que fala com o
// whatsmeow não conhecer HTTP nem o formato do frame.
package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// MaxOutboundBytes é o maior arquivo baixado do Linktor para envio, o
	// mesmo limite do adaptador WhatsApp embutido.
	MaxOutboundBytes = 25 << 20
	fetchTimeout     = 60 * time.Second
)

// Fetch baixa a URL para um arquivo novo em dir e devolve o caminho e o MIME
// informado pelo servidor. Só http e https; o corpo é cortado em MaxOutboundBytes.
func Fetch(ctx context.Context, rawURL, dir string) (path, mimeType string, err error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", "", fmt.Errorf("invalid media url %q", rawURL)
	}
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("fetch media: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("fetch media: HTTP %d", resp.StatusCode)
	}

	f, err := os.CreateTemp(dir, "out-*")
	if err != nil {
		return "", "", err
	}
	n, err := io.Copy(f, io.LimitReader(resp.Body, MaxOutboundBytes+1))
	closeErr := f.Close()
	switch {
	case err != nil:
		_ = os.Remove(f.Name())
		return "", "", fmt.Errorf("fetch media: %w", err)
	case closeErr != nil:
		_ = os.Remove(f.Name())
		return "", "", closeErr
	case n > MaxOutboundBytes:
		_ = os.Remove(f.Name())
		return "", "", fmt.Errorf("media exceeds %d MB", MaxOutboundBytes>>20)
	case n == 0:
		_ = os.Remove(f.Name())
		return "", "", errors.New("media is empty")
	}
	mimeType = strings.TrimSpace(strings.Split(resp.Header.Get("Content-Type"), ";")[0])
	return f.Name(), mimeType, nil
}

// InlineAttachments troca, num payload de inbound, o campo "path" de cada
// anexo pelos bytes do arquivo em "data" (base64). Devolve o payload novo e a
// limpeza que apaga os arquivos, para rodar depois do envio. Anexo cujo
// arquivo sumiu segue só com o registro e um "error".
func InlineAttachments(payload []byte) ([]byte, func(), error) {
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		return nil, nil, fmt.Errorf("inbound payload: %w", err)
	}
	raw, ok := event["attachments"].([]any)
	if !ok || len(raw) == 0 {
		return payload, nil, nil
	}

	var files []string
	for _, item := range raw {
		att, ok := item.(map[string]any)
		if !ok {
			continue
		}
		path, _ := att["path"].(string)
		if path == "" {
			continue
		}
		delete(att, "path")
		data, err := os.ReadFile(path)
		if err != nil {
			att["error"] = "media file unavailable: " + filepath.Base(path)
			continue
		}
		att["data"] = base64.StdEncoding.EncodeToString(data)
		files = append(files, path)
	}
	prepared, err := json.Marshal(event)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		for _, p := range files {
			_ = os.Remove(p)
		}
	}
	return prepared, cleanup, nil
}
