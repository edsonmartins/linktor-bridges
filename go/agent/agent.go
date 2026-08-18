// Package agent implements the bridge side of the Linktor Bridge Protocol
// (see docs/PROTOCOL.md). It connects to the Linktor gateway over WebSocket,
// forwards inbound events, executes outbound send commands and reports status.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	pingInterval             = 30 * time.Second
	heartbeatIntervalDefault = 20 * time.Second
	writeTimeout             = 10 * time.Second
)

// Handler is the interface the bridge host implements to act on outbound
// commands coming from Linktor. The Agent never touches whatsmeow directly.
type Handler interface {
	// SendText sends a text message and returns the WhatsApp message ID.
	// messageID may be empty or a caller-persisted ID to reuse for idempotent retry.
	SendText(to, text, messageID string) (string, error)
	// RejectCall rejects an incoming WhatsApp call (call signaling).
	RejectCall(callFrom, callID string) error
	// Status returns the current session snapshot as JSON (wacore.Status).
	Status() (string, error)
}

// OutboundEvent is an event the bridge host wants forwarded to the gateway.
// Kind selects the protocol frame type ("inbound" for messages, "call_event"
// for call signaling); Payload is the raw JSON event from wacore.
type OutboundEvent struct {
	Kind    string
	Payload []byte
}

// Config configures the Agent connection.
type Config struct {
	ServerURL string // ws(s)://host/api/v1/gateways/ws
	ChannelID string
	Token     string
	Platform  string // "desktop" | "android"
	Version   string

	// HeartbeatInterval controls how often the application-level ping frame is
	// sent to the gateway. Zero means the default (20s). Exposed so tests can
	// exercise liveness without waiting a full interval.
	HeartbeatInterval time.Duration

	// Outbound is a channel on which events (inbound messages, call signaling)
	// are published so the host forwards them to the gateway.
	Outbound chan OutboundEvent
}

// Agent maintains the WebSocket connection to the Linktor gateway.
type Agent struct {
	cfg    Config
	server Handler
	client *Conn

	mu         sync.Mutex
	lastStatus []byte
}

// New creates an Agent. server must be non-nil.
func New(cfg Config, server Handler) *Agent {
	if cfg.Platform == "" {
		cfg.Platform = "desktop"
	}
	if cfg.Version == "" {
		cfg.Version = "0.1.0"
	}
	return &Agent{cfg: cfg, server: server}
}

// Conn wraps a live websocket with the currently active connection state.
type Conn struct {
	ws   *websocket.Conn
	done chan struct{}
}

// Run connects (with backoff) and serves the protocol until ctx is cancelled.
// It is blocking; call it in its own goroutine.
func (a *Agent) Run(ctx context.Context) {
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := a.runOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("agent: connection dropped: %v (retry in %s)", err, backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

// SetStatus stores the latest session status to be sent after reconnection.
func (a *Agent) SetStatus(statusJSON []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastStatus = append([]byte(nil), statusJSON...)
}

// Stop closes the active connection, causing Run to exit its loop.
func (a *Agent) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.client != nil {
		_ = a.client.ws.Close(websocket.StatusNormalClosure, "bye")
	}
}

func (a *Agent) runOnce(ctx context.Context) error {
	heartbeatInterval := a.cfg.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = heartbeatIntervalDefault
	}

	u, err := url.Parse(a.cfg.ServerURL)
	if err != nil {
		return fmt.Errorf("parse server url: %w", err)
	}
	q := u.Query()
	q.Set("channel_id", a.cfg.ChannelID)
	q.Set("token", a.cfg.Token)
	u.RawQuery = q.Encode()

	ws, _, err := websocket.Dial(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close(websocket.StatusInternalError, "")

	conn := &Conn{ws: ws, done: make(chan struct{})}
	a.mu.Lock()
	a.client = conn
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		if a.client == conn {
			a.client = nil
		}
		a.mu.Unlock()
	}()

	// Send hello and cached status.
	if err := a.send(conn, frame{Type: "hello", ChannelID: a.cfg.ChannelID, Platform: a.cfg.Platform, Version: a.cfg.Version}); err != nil {
		return err
	}
	a.mu.Lock()
	status := append([]byte(nil), a.lastStatus...)
	a.mu.Unlock()
	if status != nil {
		if err := a.send(conn, frame{Type: "status", ChannelID: a.cfg.ChannelID, Status: json.RawMessage(status)}); err != nil {
			return err
		}
	}

	// Heartbeat. Two layers:
	//  1. websocket-level ping (transport keep-alive) every pingInterval;
	//  2. application-level heartbeat frame every heartbeatInterval, carrying the
	//     current session status + timestamp, so the Linktor gateway can report
	//     bridge liveness ("last seen") in health telemetry.
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		heartbeat := time.NewTicker(heartbeatInterval)
		defer heartbeat.Stop()
		for {
			select {
			case <-ticker.C:
				ctxPing, cancel := context.WithTimeout(ctx, writeTimeout)
				err := conn.ws.Ping(ctxPing)
				cancel()
				if err != nil {
					_ = conn.ws.Close(websocket.StatusPolicyViolation, "ping timeout")
					return
				}
			case <-heartbeat.C:
				a.mu.Lock()
				status := append([]byte(nil), a.lastStatus...)
				a.mu.Unlock()
				hb := frame{
					Type:      "ping",
					ChannelID: a.cfg.ChannelID,
					TS:        time.Now().Unix(),
				}
				if status != nil {
					hb.Status = json.RawMessage(status)
				}
				if err := a.send(conn, hb); err != nil {
					_ = conn.ws.Close(websocket.StatusPolicyViolation, "heartbeat write failed")
					return
				}
			case <-conn.done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		_, data, err := conn.ws.Read(ctx)
		if err != nil {
			close(conn.done)
			return fmt.Errorf("read: %w", err)
		}
		if err := a.handleFrame(ctx, conn, data); err != nil {
			close(conn.done)
			return err
		}
	}
}

func (a *Agent) handleFrame(ctx context.Context, conn *Conn, data []byte) error {
	var f frame
	if err := json.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("invalid frame: %w", err)
	}
	switch f.Type {
	case "welcome":
		if !f.OK {
			return fmt.Errorf("welcome rejected: %s", f.Error)
		}
		log.Printf("agent: welcome ok")
		if a.cfg.Outbound != nil {
			go a.serveEvents(ctx, conn)
		}
	case "pong":
		// liveness echo; nothing to do
	case "outbound":
		resp := a.execOutbound(f)
		return a.send(conn, resp)
	case "call_command":
		resp := a.execCallCommand(f)
		return a.send(conn, resp)
	case "close":
		_ = conn.ws.Close(websocket.StatusNormalClosure, "server closed: "+f.Reason)
		return errors.New("closed by server: " + f.Reason)
	}
	return nil
}

func (a *Agent) execOutbound(f frame) frame {
	messageID, err := a.server.SendText(f.To, f.Text, f.MessageID)
	if err != nil {
		return frame{
			Type:          "ack",
			ChannelID:     a.cfg.ChannelID,
			CorrelationID: f.CorrelationID,
			OK:            false,
			Error:         sanitizeError(err),
		}
	}
	return frame{
		Type:          "ack",
		ChannelID:     a.cfg.ChannelID,
		CorrelationID: f.CorrelationID,
		OK:            true,
		MessageID:     messageID,
	}
}

// execCallCommand handles a call signaling command from Linktor. The frame
// carries the command in Text ("reject" for now; media/accept/end are future
// milestones that need the voip engine on the bridge) plus From/CallID.
func (a *Agent) execCallCommand(f frame) frame {
	if a.server == nil {
		return frame{Type: "call_ack", ChannelID: a.cfg.ChannelID, CorrelationID: f.CorrelationID, OK: false, Error: "bridge handler unavailable"}
	}
	if f.Text != "reject" {
		return frame{Type: "call_ack", ChannelID: a.cfg.ChannelID, CorrelationID: f.CorrelationID, OK: false, Error: "unsupported call command: " + f.Text}
	}
	if err := a.server.RejectCall(f.From, f.CallID); err != nil {
		return frame{Type: "call_ack", ChannelID: a.cfg.ChannelID, CorrelationID: f.CorrelationID, OK: false, Error: sanitizeError(err)}
	}
	return frame{Type: "call_ack", ChannelID: a.cfg.ChannelID, CorrelationID: f.CorrelationID, OK: true}
}

// serveEvents forwards events (messages + call signaling) to the gateway. The
// frame type is chosen by the event kind, and payloads are piped raw.
func (a *Agent) serveEvents(ctx context.Context, conn *Conn) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-conn.done:
			return
		case evt, ok := <-a.cfg.Outbound:
			if !ok {
				return
			}
			frameType := "inbound"
			if evt.Kind == "call_event" {
				frameType = "call_event"
			}
			_ = a.send(conn, frame{Type: frameType, ChannelID: a.cfg.ChannelID, Message: json.RawMessage(evt.Payload)})
		}
	}
}

func (a *Agent) send(conn *Conn, f frame) error {
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return conn.ws.Write(ctx, websocket.MessageText, data)
}

// frame is the JSON envelope used by the protocol.
type frame struct {
	Type          string          `json:"type"`
	ChannelID     string          `json:"channel_id,omitempty"`
	Platform      string          `json:"platform,omitempty"`
	Version       string          `json:"version,omitempty"`
	OK            bool            `json:"ok,omitempty"`
	Error         string          `json:"error,omitempty"`
	Reason        string          `json:"reason,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	To            string          `json:"to,omitempty"`
	Text          string          `json:"text,omitempty"`
	From          string          `json:"from,omitempty"`
	CallID        string          `json:"call_id,omitempty"`
	MessageID     string          `json:"message_id,omitempty"`
	Message       json.RawMessage `json:"message,omitempty"`
	Status        json.RawMessage `json:"status,omitempty"`
	TS            int64           `json:"ts,omitempty"`
}

func sanitizeError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) > 200 {
		msg = msg[:200] + "..."
	}
	return msg
}
