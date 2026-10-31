// Command bridge-desktop is the WhatsApp Linktor bridge for Windows/Linux/macOS.
//
// It runs in the system tray, keeps the WhatsApp Multi-Device session alive on
// the user's machine and exposes a local web dashboard for pairing and status.
// Receiving commands (outbound) and publishing events (inbound) to Linktor is
// the next milestone and will ride the same localhost HTTP surface.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
	"whatsbridge/agent"
	"whatsbridge/media"

	"github.com/skip2/go-qrcode"

	"whatsbridge/wacore"
)

//go:embed webui/index.html
var webUI embed.FS

var (
	gatewayServer  = flag.String("server", "", "Linktor gateway WS URL (ws(s)://host/api/v1/gateways/ws); grava em config.json")
	gatewayChannel = flag.String("channel-id", "", "Linktor channel ID; grava em config.json")
	gatewayToken   = flag.String("token", "", "Linktor bridge token; grava em config.json")
	listenAddr     = flag.String("listen", "", "endereço do painel local (padrão 127.0.0.1:7788, ou o de config.json)")
)

// defaultListen é fixo de propósito: a URL do painel não pode mudar a cada
// execução. Se a porta estiver ocupada, cai para uma aleatória e avisa.
const defaultListen = "127.0.0.1:7788"

const dataDirName = "linktor-bridge"

var gateway *gatewayRuntime

func main() {
	flag.Parse()

	dir, err := defaultDataDir()
	if err != nil {
		fatal("resolve data dir: %v", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		fatal("create data dir: %v", err)
	}
	fmt.Printf("store: %s\n", dir)

	if err := wacore.Initialize(dir); err != nil {
		fatal("initialize core: %v", err)
	}
	defer wacore.Close()

	// Sessão já pareada em execução anterior: reconecta sozinha na subida, sem
	// esperar um clique no painel. Falha aqui não é fatal (o painel permite
	// tentar de novo) e o auto-reconnect do whatsmeow cuida das quedas.
	if wacore.IsPaired() {
		go func() {
			if err := wacore.Connect(); err != nil {
				fmt.Printf("connect on startup: %v\n", err)
			}
		}()
	}

	// Outbound handler used by the agent: sends via the local WhatsApp session.
	outbound := outboundHandler{mediaDir: wacore.MediaDir()}
	wacore.PurgeMedia()

	rt, err := newGatewayRuntime(dir, outbound)
	if err != nil {
		fatal("%v", err)
	}
	gateway = rt

	// Configuração do Linktor: config.json, com os flags por cima (e gravados).
	cfg, err := loadConfig(dir)
	if err != nil {
		fatal("%v", err)
	}
	if *gatewayServer != "" {
		cfg.Server = *gatewayServer
	}
	if *gatewayChannel != "" {
		cfg.ChannelID = *gatewayChannel
	}
	if *gatewayToken != "" {
		cfg.Token = *gatewayToken
	}
	if *listenAddr != "" {
		cfg.Listen = *listenAddr
	}
	rt.cfg.Listen = cfg.Listen
	if cfg.Server != "" || cfg.ChannelID != "" || cfg.Token != "" {
		if err := rt.start(cfg); err != nil {
			// Sem ligação com o Linktor o painel ainda serve para configurar.
			fmt.Printf("gateway não ligado: %v (configure no painel)\n", err)
		}
	} else {
		fmt.Println("gateway não configurado: informe servidor, canal e token no painel")
	}

	listen := cfg.Listen
	if listen == "" {
		listen = defaultListen
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		fmt.Printf("porta %s ocupada (%v); usando uma porta livre\n", listen, err)
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fatal("listen: %v", err)
		}
	}
	addr := fmt.Sprintf("http://%s", ln.Addr().String())

	go func() {
		srv := &http.Server{Handler: newRouter(outbound)}
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			fatal("http server: %v", err)
		}
	}()

	fmt.Printf("dashboard: %s\n", addr)

	runUI(addr)
}

// shutdown para o agente e desconecta a sessão do WhatsApp (sem desparear).
func shutdown() {
	if gateway != nil {
		gateway.stop()
	}
	wacore.Disconnect()
}

// outboundHandler adapts wacore to agent.Handler. mediaDir recebe os
// downloads temporários de mídia de saída.
type outboundHandler struct {
	mediaDir string
}

func (outboundHandler) SendText(to, text, messageID string) (string, error) {
	if messageID == "" {
		return wacore.SendText(to, text)
	}
	return wacore.SendTextWithID(to, text, messageID)
}

// SendMedia baixa a URL assinada do Linktor para um arquivo temporário, envia
// pelo wacore e apaga o arquivo.
func (h outboundHandler) SendMedia(to string, m agent.OutboundMedia, caption, messageID string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	path, fetchedMime, err := media.Fetch(ctx, m.URL, h.mediaDir)
	if err != nil {
		return "", err
	}
	defer os.Remove(path)
	mimeType := m.MimeType
	if mimeType == "" {
		mimeType = fetchedMime
	}
	return wacore.SendMedia(to, m.Type, path, mimeType, m.Filename, caption, m.PTT, messageID)
}

func (outboundHandler) RejectCall(callFrom, callID string) error {
	return wacore.RejectCall(callFrom, callID)
}

func (outboundHandler) Status() (string, error) {
	return wacore.GetStatus()
}

func newRouter(outbound outboundHandler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		index, err := webUI.ReadFile("webui/index.html")
		if err != nil {
			http.Error(w, "ui not found", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(index)
	})
	for _, pattern := range []string{"GET /api/gateway", "POST /api/gateway", "DELETE /api/gateway"} {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			gateway.handleGateway(w, r)
		})
	}
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mustStatus())
	})
	mux.HandleFunc("GET /api/qr.png", func(w http.ResponseWriter, r *http.Request) {
		// Sempre o código atual: o WhatsApp troca o QR a cada ~20s enquanto o
		// pareamento está aberto, e o painel recarrega a imagem a cada 4s.
		payload := wacore.CurrentQR()
		if payload == "" {
			http.Error(w, "no active QR", http.StatusNotFound)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		pngBytes, err := qrcode.Encode(payload, qrcode.Medium, 256)
		if err != nil {
			http.Error(w, "encode QR: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngBytes)
	})
	mux.HandleFunc("POST /api/pair/qr", func(w http.ResponseWriter, r *http.Request) {
		if _, err := wacore.BeginPairing(); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]any{"qr": true, "expires_at": time.Now().Add(3 * time.Minute).Unix()})
	})
	mux.HandleFunc("POST /api/pair/code", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Phone string `json:"phone"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		code, err := wacore.BeginCodePairing(req.Phone)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]any{"code": code})
	})
	mux.HandleFunc("POST /api/pair/cancel", func(w http.ResponseWriter, r *http.Request) {
		wacore.CancelPairing()
		writeJSON(w, map[string]any{"cancelled": true})
	})
	mux.HandleFunc("POST /api/connect", func(w http.ResponseWriter, r *http.Request) {
		if err := wacore.Connect(); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]any{"connected": true})
	})
	mux.HandleFunc("POST /api/disconnect", func(w http.ResponseWriter, r *http.Request) {
		wacore.Disconnect()
		writeJSON(w, map[string]any{"disconnected": true})
	})
	mux.HandleFunc("POST /api/send", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			To   string `json:"to"`
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		messageID, err := wacore.SendText(req.To, req.Text)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeJSON(w, map[string]any{"message_id": messageID})
	})
	// Só leitura: os últimos eventos que o bridge já pegou para o Linktor.
	mux.HandleFunc("GET /api/inbound", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, gateway.recent.snapshot())
	})
	return mux
}

func mustStatus() map[string]any {
	status, err := wacore.GetStatus()
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	var parsed map[string]any
	_ = json.Unmarshal([]byte(status), &parsed)
	parsed["qr_active"] = wacore.CurrentQR() != ""
	if pairErr := wacore.PairingError(); pairErr != "" {
		parsed["pair_error"] = pairErr
		parsed["last_error"] = pairErr
	}
	return parsed
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func defaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".linktor", dataDirName), nil
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "linktor-bridge: "+format+"\n", args...)
	os.Exit(1)
}
