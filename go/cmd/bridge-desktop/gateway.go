package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"whatsbridge/agent"
	"whatsbridge/eventstore"
	"whatsbridge/media"
	"whatsbridge/wacore"
)

// Config é o que o bridge precisa para falar com o Linktor. Fica em
// <dataDir>/config.json (0600) e pode ser preenchida pelo painel ou pelos
// flags; os flags, quando passados, são gravados para a próxima execução.
type Config struct {
	Server    string `json:"server"`     // ws(s)://host/api/v1/gateways/ws
	ChannelID string `json:"channel_id"` // canal whatsapp_remote no Linktor
	Token     string `json:"token"`      // bridge token do canal
	Listen    string `json:"listen,omitempty"`
}

const configFileName = "config.json"

func configPath(dataDir string) string { return filepath.Join(dataDir, configFileName) }

func loadConfig(dataDir string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(configPath(dataDir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cfg, nil
		}
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("config.json inválido: %w", err)
	}
	return cfg, nil
}

func saveConfig(dataDir string, cfg Config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp := configPath(dataDir) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, configPath(dataDir))
}

// validateGateway confere os três campos antes de guardar.
func validateGateway(cfg Config) error {
	u, err := url.Parse(strings.TrimSpace(cfg.Server))
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
		return errors.New("servidor inválido: use ws://host/api/v1/gateways/ws ou wss://…")
	}
	if strings.TrimSpace(cfg.ChannelID) == "" {
		return errors.New("informe o ID do canal")
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return errors.New("informe o token do bridge")
	}
	return nil
}

// gatewayRuntime é dono do agente: liga, religa com outra configuração e
// desliga. O event store e o canal de eventos são únicos; o agente atual é o
// que os consome.
type gatewayRuntime struct {
	mu      sync.Mutex
	dataDir string
	cfg     Config
	agent   *agent.Agent
	cancel  context.CancelFunc
	events  chan agent.OutboundEvent
	store   *eventstore.Store
	handler outboundHandler
	// recent é a cópia dos últimos eventos para o painel mostrar. O painel
	// nunca lê a fila do wacore: DrainInbound consome, e um segundo leitor
	// roubava eventos do Linktor (mensagens e sinais de chamada sumiam).
	recent recentEvents
}

// recentEventsMax é quantos eventos o painel mostra.
const recentEventsMax = 50

// recentEvents guarda os últimos eventos drenados, mais novos no fim.
type recentEvents struct {
	mu    sync.Mutex
	items []json.RawMessage
}

func (r *recentEvents) add(event json.RawMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, event)
	if over := len(r.items) - recentEventsMax; over > 0 {
		r.items = append([]json.RawMessage(nil), r.items[over:]...)
	}
}

func (r *recentEvents) snapshot() []json.RawMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]json.RawMessage{}, r.items...)
}

func newGatewayRuntime(dataDir string, handler outboundHandler) (*gatewayRuntime, error) {
	store, err := eventstore.Open(filepath.Join(dataDir, "events.db"))
	if err != nil {
		return nil, fmt.Errorf("open event store: %w", err)
	}
	rt := &gatewayRuntime{
		dataDir: dataDir,
		events:  make(chan agent.OutboundEvent, 256),
		store:   store,
		handler: handler,
	}
	go rt.drainLoop()
	return rt, nil
}

// start grava a configuração e (re)liga o agente com ela.
func (rt *gatewayRuntime) start(cfg Config) error {
	if err := validateGateway(cfg); err != nil {
		return err
	}
	cfg.Server = strings.TrimSpace(cfg.Server)
	cfg.ChannelID = strings.TrimSpace(cfg.ChannelID)
	cfg.Token = strings.TrimSpace(cfg.Token)

	rt.mu.Lock()
	defer rt.mu.Unlock()
	cfg.Listen = rt.cfg.Listen
	if err := saveConfig(rt.dataDir, cfg); err != nil {
		return fmt.Errorf("gravar config: %w", err)
	}
	rt.stopLocked()
	rt.cfg = cfg

	a := agent.New(agent.Config{
		ServerURL: cfg.Server,
		ChannelID: cfg.ChannelID,
		Token:     cfg.Token,
		Platform:  "desktop",
		Version:   "0.1.0",
		Outbound:  rt.events,
		// Anexos saem do disco para o frame só na hora de enviar.
		PrepareInbound: media.InlineAttachments,
	}, rt.handler)
	ctx, cancel := context.WithCancel(context.Background())
	rt.agent = a
	rt.cancel = cancel
	go a.Run(ctx)
	fmt.Printf("gateway: %s (channel %s)\n", cfg.Server, cfg.ChannelID)
	return nil
}

// forget desliga o agente e apaga a configuração do Linktor (mantém a sessão
// do WhatsApp).
func (rt *gatewayRuntime) forget() error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.stopLocked()
	rt.cfg.Server, rt.cfg.ChannelID, rt.cfg.Token = "", "", ""
	return saveConfig(rt.dataDir, rt.cfg)
}

func (rt *gatewayRuntime) stop() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.stopLocked()
}

func (rt *gatewayRuntime) stopLocked() {
	if rt.cancel != nil {
		rt.cancel()
		rt.cancel = nil
	}
	if rt.agent != nil {
		rt.agent.Stop()
		rt.agent = nil
	}
}

func (rt *gatewayRuntime) current() *agent.Agent {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.agent
}

// status é o que o painel mostra no bloco "Linktor".
func (rt *gatewayRuntime) status() map[string]any {
	rt.mu.Lock()
	cfg := rt.cfg
	a := rt.agent
	rt.mu.Unlock()

	out := map[string]any{
		"configured": cfg.Server != "" && cfg.ChannelID != "" && cfg.Token != "",
		"server":     cfg.Server,
		"channel_id": cfg.ChannelID,
		"token_hint": maskToken(cfg.Token),
		"connected":  false,
	}
	if a != nil {
		st := a.State()
		out["connected"] = st.Connected
		if !st.ConnectedAt.IsZero() {
			out["connected_at"] = st.ConnectedAt
		}
		if st.LastError != "" {
			out["last_error"] = st.LastError
		}
	}
	pending, err := rt.store.Len()
	if err == nil {
		out["pending_events"] = pending
	}
	return out
}

// drainLoop move os eventos do wacore para o event store e, quando há agente
// ligado, do store para o agente. Sem agente os eventos ficam no store. É o
// único leitor da fila do wacore (ver recentEvents).
func (rt *gatewayRuntime) drainLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if raw, err := wacore.DrainInbound(); err == nil {
			var events []wacore.InboundEvent
			if json.Unmarshal([]byte(raw), &events) == nil {
				for _, evt := range events {
					data, err := json.Marshal(evt)
					if err != nil {
						continue
					}
					rt.recent.add(data)
					kind := "inbound"
					if evt.Type == "call" {
						kind = "call_event"
					}
					if err := rt.store.Enqueue(kind, data); err != nil {
						fmt.Printf("eventstore: enqueue failed: %v\n", err)
					}
				}
			}
		}

		a := rt.current()
		if a == nil {
			continue
		}
		pending, err := rt.store.Pending(256)
		if err == nil && len(pending) > 0 {
			maxSeq := pending[len(pending)-1].Seq
			flushed := true
			for _, item := range pending {
				select {
				case rt.events <- agent.OutboundEvent{Kind: item.Kind, Payload: item.Payload}:
				default:
					flushed = false // buffer cheio; tenta de novo no próximo tick
				}
			}
			if flushed {
				if err := rt.store.Acknowledge(maxSeq); err != nil {
					fmt.Printf("eventstore: acknowledge failed: %v\n", err)
				}
			}
		}
		if status, err := wacore.GetStatus(); err == nil {
			a.SetStatus([]byte(status))
		}
	}
}

func maskToken(token string) string {
	if token == "" {
		return ""
	}
	if len(token) <= 8 {
		return "••••"
	}
	return token[:4] + "…" + token[len(token)-4:]
}

// handleGateway serve GET (status), POST (salvar e ligar) e DELETE (esquecer)
// em /api/gateway.
func (rt *gatewayRuntime) handleGateway(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, rt.status())
	case http.MethodPost:
		var req Config
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		// Token em branco no formulário = manter o já gravado.
		if strings.TrimSpace(req.Token) == "" {
			rt.mu.Lock()
			req.Token = rt.cfg.Token
			rt.mu.Unlock()
		}
		if err := rt.start(req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, rt.status())
	case http.MethodDelete:
		if err := rt.forget(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, rt.status())
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
