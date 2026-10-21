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

	"github.com/skip2/go-qrcode"

	"whatsbridge/agent"
	"whatsbridge/eventstore"
	"whatsbridge/wacore"
)

//go:embed webui/index.html
var webUI embed.FS

var (
	gatewayServer  = flag.String("server", "", "Linktor gateway WS URL (ws(s)://host/api/v1/gateways/ws)")
	gatewayChannel = flag.String("channel-id", "", "Linktor channel ID")
	gatewayToken   = flag.String("token", "", "Linktor bridge token")
)

const dataDirName = "linktor-bridge"

var lastQR struct {
	payload string
	at      time.Time
}

var agentCancel context.CancelFunc

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

	// Outbound handler used by the agent: sends via the local WhatsApp session.
	outbound := outboundHandler{}

	if *gatewayServer != "" {
		if *gatewayChannel == "" || *gatewayToken == "" {
			fatal("--channel-id and --token are required when using --server")
		}
		startAgent(*gatewayServer, *gatewayChannel, *gatewayToken, outbound)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fatal("listen: %v", err)
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
	if agentCancel != nil {
		agentCancel()
	}
	wacore.Disconnect()
}

// outboundHandler adapts wacore to agent.Handler.
type outboundHandler struct{}

func (outboundHandler) SendText(to, text, messageID string) (string, error) {
	if messageID == "" {
		return wacore.SendText(to, text)
	}
	return wacore.SendTextWithID(to, text, messageID)
}

func (outboundHandler) RejectCall(callFrom, callID string) error {
	return wacore.RejectCall(callFrom, callID)
}

func (outboundHandler) Status() (string, error) {
	return wacore.GetStatus()
}

// startAgent runs the Linktor bridge agent in the background. Inbound events
// drained from wacore are persisted to the local event store and forwarded to
// the gateway as it connects (durable, so an outage does not lose events).
func startAgent(serverURL, channelID, token string, outbound outboundHandler) *agent.Agent {
	events := make(chan agent.OutboundEvent, 256)

	// Durable event store: events remain queued until the gateway confirms.
	storeDir := filepath.Join(mustDataDir(), "events.db")
	store, err := eventstore.Open(storeDir)
	if err != nil {
		fatal("open event store: %v", err)
	}

	a := agent.New(agent.Config{
		ServerURL: serverURL,
		ChannelID: channelID,
		Token:     token,
		Platform:  "desktop",
		Version:   "0.1.0",
		Outbound:  events,
	}, outbound)

	ctx, cancel := context.WithCancel(context.Background())
	go a.Run(ctx)
	agentCancel = cancel

	// Periodic heartbeat: persist drained inbound events, then replay the store
	// to the connected agent and refresh status. Call signaling events ride a
	// separate kind so the gateway routes them to the call handler.
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			raw, err := wacore.DrainInbound()
			if err == nil {
				var events []wacore.InboundEvent
				if json.Unmarshal([]byte(raw), &events) == nil {
					for _, evt := range events {
						data, err := json.Marshal(evt)
						if err != nil {
							continue
						}
						kind := "inbound"
						if evt.Type == "call" {
							kind = "call_event"
						}
						if err := store.Enqueue(kind, data); err != nil {
							fmt.Printf("eventstore: enqueue failed: %v\n", err)
						}
					}
				}
			}
			pending, err := store.Pending(256)
			if err == nil && len(pending) > 0 {
				maxSeq := pending[len(pending)-1].Seq
				flushed := true
				for _, item := range pending {
					select {
					case events <- agent.OutboundEvent{Kind: item.Kind, Payload: item.Payload}:
					default:
						flushed = false // gateway buffer full; retry later, keep queued
					}
				}
				if flushed {
					if err := store.Acknowledge(maxSeq); err != nil {
						fmt.Printf("eventstore: acknowledge failed: %v\n", err)
					}
				}
			}
			if status, err := wacore.GetStatus(); err == nil {
				a.SetStatus([]byte(status))
			}
		}
	}()

	fmt.Printf("gateway: %s (channel %s) · event store: %s\n", serverURL, channelID, storeDir)
	return a
}

func mustDataDir() string {
	dir, err := defaultDataDir()
	if err != nil {
		fatal("resolve data dir: %v", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		fatal("create data dir: %v", err)
	}
	return dir
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
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mustStatus())
	})
	mux.HandleFunc("GET /api/qr.png", func(w http.ResponseWriter, r *http.Request) {
		if lastQR.payload == "" {
			http.Error(w, "no active QR", http.StatusNotFound)
			return
		}
		pngBytes, err := qrcode.Encode(lastQR.payload, qrcode.Medium, 256)
		if err != nil {
			http.Error(w, "encode QR: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngBytes)
	})
	mux.HandleFunc("POST /api/pair/qr", func(w http.ResponseWriter, r *http.Request) {
		qr, err := wacore.BeginPairing()
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		lastQR.payload, lastQR.at = qr, time.Now()
		writeJSON(w, map[string]any{"qr": true, "expires_at": lastQR.at.Add(3 * time.Minute).Unix()})
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
	mux.HandleFunc("GET /api/inbound", func(w http.ResponseWriter, r *http.Request) {
		events, err := wacore.DrainInbound()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(events))
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
	parsed["qr_active"] = lastQR.payload != ""
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
