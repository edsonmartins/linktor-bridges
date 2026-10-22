package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// mockServer acts as the Linktor gateway for tests.
type mockServer struct {
	t          *testing.T
	url        string
	handler    http.Handler
	authQuery  string
	onFrame    func(f frame)
	outboundCh chan frame
	acks       chan frame
}

func newMockServer(t *testing.T, onFrame func(f frame)) *mockServer {
	m := &mockServer{
		t:          t,
		onFrame:    onFrame,
		outboundCh: make(chan frame, 16),
		acks:       make(chan frame, 16),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/gateways/ws", m.handleWS)
	m.handler = mux
	srv := httptest.NewServer(mux)
	m.url = "ws" + strings.TrimPrefix(srv.URL, "http") + "/gateways/ws"
	t.Cleanup(srv.Close)
	return m
}

func (m *mockServer) handleWS(w http.ResponseWriter, r *http.Request) {
	if m.authQuery != "" && r.URL.RawQuery != m.authQuery {
		http.Error(w, "bad auth", http.StatusUnauthorized)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	// Writer goroutine: pushes queued outbound commands onto the socket.
	go func() {
		for f := range m.outboundCh {
			_ = m.write(conn, f)
		}
	}()

	for {
		_, data, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		var f frame
		if err := json.Unmarshal(data, &f); err != nil {
			continue
		}
		switch f.Type {
		case "hello":
			_ = m.write(conn, frame{Type: "welcome", OK: true})
		case "inbound", "status":
			// record only
		case "ack", "call_ack":
			m.acks <- f
		}
		if m.onFrame != nil {
			m.onFrame(f)
		}
	}
}

func (m *mockServer) write(conn *websocket.Conn, f frame) error {
	data, _ := json.Marshal(f)
	return conn.Write(context.Background(), websocket.MessageText, data)
}

func (m *mockServer) pushOutbound(correlationID, to, text, messageID string) {
	m.outboundCh <- frame{
		Type:          "outbound",
		CorrelationID: correlationID,
		To:            to,
		Text:          text,
		MessageID:     messageID,
	}
}

func (m *mockServer) pushCallCommand(correlationID, from, callID, command string) {
	m.outboundCh <- frame{
		Type:          "call_command",
		CorrelationID: correlationID,
		From:          from,
		CallID:        callID,
		Text:          command,
	}
}

// mockHandler implements agent.Handler.
type mockHandler struct {
	sendErr  error
	sent     chan sendCall
	rejected chan rejectCall
}

type sendCall struct {
	to, text, messageID string
}

type rejectCall struct {
	from, callID string
}

func (h *mockHandler) SendText(to, text, messageID string) (string, error) {
	h.sent <- sendCall{to, text, messageID}
	if h.sendErr != nil {
		return "", h.sendErr
	}
	return "MOCKID", nil
}

func (h *mockHandler) SendMedia(to string, m OutboundMedia, caption, messageID string) (string, error) {
	h.sent <- sendCall{to, caption + " [" + m.Type + ":" + m.URL + "]", messageID}
	if h.sendErr != nil {
		return "", h.sendErr
	}
	return "MOCKMEDIA", nil
}

func (h *mockHandler) RejectCall(callFrom, callID string) error {
	if h.rejected != nil {
		h.rejected <- rejectCall{callFrom, callID}
	}
	return nil
}

func (h *mockHandler) Status() (string, error) {
	return `{"paired":true,"connected":true}`, nil
}

func TestHelloAndOutboundAck(t *testing.T) {
	server := newMockServer(t, nil)
	handler := &mockHandler{sent: make(chan sendCall, 1)}
	agent := New(Config{
		ServerURL: server.url,
		ChannelID: "wh-abc",
		Token:     "secret",
		Platform:  "desktop",
		Version:   "test",
	}, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go agent.Run(ctx)

	// Wait for hello/welcome exchange.
	time.Sleep(300 * time.Millisecond)

	// Server pushes an outbound command.
	server.pushOutbound("c_1", "5511999999999", "olá", "RETRY_ID")

	select {
	case ack := <-server.acks:
		if ack.CorrelationID != "c_1" || !ack.OK || ack.MessageID != "MOCKID" {
			t.Fatalf("unexpected ack: %+v", ack)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ack")
	}

	select {
	case call := <-handler.sent:
		if call.to != "5511999999999" || call.text != "olá" || call.messageID != "RETRY_ID" {
			t.Fatalf("unexpected send call: %+v", call)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for SendText")
	}
}

func TestOutboundErrorAck(t *testing.T) {
	server := newMockServer(t, nil)
	handler := &mockHandler{sendErr: context.DeadlineExceeded, sent: make(chan sendCall, 1)}
	agent := New(Config{ServerURL: server.url, ChannelID: "wh-abc", Token: "secret"}, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go agent.Run(ctx)

	time.Sleep(300 * time.Millisecond)
	server.pushOutbound("c_2", "5511999999999", "oi", "")

	select {
	case ack := <-server.acks:
		if ack.CorrelationID != "c_2" || ack.OK || ack.Error == "" {
			t.Fatalf("unexpected ack: %+v", ack)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for error ack")
	}
}

// TestHeartbeatPing verifies the agent publishes an application-level ping
// frame at the heartbeat interval after connecting.
func TestHeartbeatPing(t *testing.T) {
	var got frame
	server := newMockServer(t, func(f frame) {
		if f.Type == "ping" {
			got = f
		}
	})
	handler := &mockHandler{sent: make(chan sendCall, 1)}
	agent := New(Config{
		ServerURL:         server.url,
		ChannelID:         "wh-abc",
		Token:             "secret",
		HeartbeatInterval: 300 * time.Millisecond,
	}, handler)
	agent.SetStatus([]byte(`{"paired":true,"connected":true}`))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go agent.Run(ctx)

	// Heartbeat interval is configured short (300ms); wait with a small deadline.
	deadline := time.Now().Add(5 * time.Second)
	for got.Type != "ping" && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
	}
	if got.Type != "ping" {
		t.Fatal("agent did not publish a ping heartbeat in time")
	}
	if got.ChannelID != "wh-abc" {
		t.Fatalf("ping channel = %q, want wh-abc", got.ChannelID)
	}
	if len(got.Status) == 0 {
		t.Fatal("ping heartbeat should carry the cached status")
	}
	if got.TS == 0 {
		t.Fatal("ping heartbeat should carry a timestamp")
	}
}

// TestCallCommandReject verifies the bridge handles a call signaling command
// (reject) from the gateway and acks it with the call_ack frame.
func TestCallCommandReject(t *testing.T) {
	server := newMockServer(t, nil)
	handler := &mockHandler{rejected: make(chan rejectCall, 1)}
	agent := New(Config{
		ServerURL: server.url,
		ChannelID: "wh-abc",
		Token:     "secret",
	}, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go agent.Run(ctx)

	time.Sleep(300 * time.Millisecond)
	server.pushCallCommand("c_call_1", "5511999999999@s.whatsapp.net", "CALL-1", "reject")

	select {
	case rc := <-handler.rejected:
		if rc.callID != "CALL-1" || rc.from != "5511999999999@s.whatsapp.net" {
			t.Fatalf("unexpected reject args: %+v", rc)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for RejectCall")
	}

	select {
	case ack := <-server.acks:
		if ack.Type != "call_ack" || ack.CorrelationID != "c_call_1" || !ack.OK {
			t.Fatalf("unexpected call ack: %+v", ack)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for call_ack")
	}
}

// TestCallCommandUnsupported verifies unknown call commands are rejected with a
// clear error ack instead of being executed.
func TestCallCommandUnsupported(t *testing.T) {
	server := newMockServer(t, nil)
	handler := &mockHandler{sent: make(chan sendCall, 1)}
	agent := New(Config{ServerURL: server.url, ChannelID: "wh-abc", Token: "secret"}, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go agent.Run(ctx)

	time.Sleep(300 * time.Millisecond)
	server.pushCallCommand("c_9", "5511999999999@s.whatsapp.net", "CALL-9", "accept")

	select {
	case ack := <-server.acks:
		if ack.Type != "call_ack" || ack.OK || ack.Error == "" {
			t.Fatalf("expected failed call ack, got %+v", ack)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for call_ack")
	}
}

func TestRejectsBadAuth(t *testing.T) {
	server := newMockServer(t, nil)
	server.authQuery = "channel_id=wh-abc&token=good"
	handler := &mockHandler{sent: make(chan sendCall, 1)}
	agent := New(Config{ServerURL: server.url, ChannelID: "wh-abc", Token: "bad"}, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Run should keep retrying; we just assert no panic and it keeps running.
	done := make(chan struct{})
	go func() {
		agent.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("agent exited early on auth failure (should backoff-retry)")
	case <-time.After(2 * time.Second):
		// expected: still retrying
	}
}

// Um outbound com media vai para SendMedia, com a legenda em text, e o ack
// devolve o ID da mídia enviada.
func TestOutboundMediaAck(t *testing.T) {
	server := newMockServer(t, nil)
	handler := &mockHandler{sent: make(chan sendCall, 1)}
	agent := New(Config{ServerURL: server.url, ChannelID: "ch-1", Token: "tok"}, handler)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go agent.Run(ctx)

	server.outboundCh <- frame{
		Type: "outbound", CorrelationID: "c_m", To: "5511999999999", Text: "legenda",
		Media: &OutboundMedia{Type: "image", URL: "https://linktor.test/api/v1/media/x.jpg?sig=1", MimeType: "image/jpeg", Filename: "x.jpg"},
	}
	select {
	case ack := <-server.acks:
		if ack.CorrelationID != "c_m" || !ack.OK || ack.MessageID != "MOCKMEDIA" {
			t.Fatalf("unexpected ack: %+v", ack)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ack")
	}
	select {
	case call := <-handler.sent:
		if call.to != "5511999999999" || call.text != "legenda [image:https://linktor.test/api/v1/media/x.jpg?sig=1]" {
			t.Fatalf("unexpected SendMedia call: %+v", call)
		}
	default:
		t.Fatal("SendMedia was not called")
	}
}

// PrepareInbound roda na hora de enviar e a limpeza roda depois da escrita.
func TestPrepareInboundRunsBeforeSend(t *testing.T) {
	var got frame
	received := make(chan struct{}, 1)
	server := newMockServer(t, func(f frame) {
		if f.Type == "inbound" {
			got = f
			received <- struct{}{}
		}
	})
	cleaned := make(chan struct{}, 1)
	events := make(chan OutboundEvent, 1)
	agent := New(Config{
		ServerURL: server.url, ChannelID: "ch-1", Token: "tok", Outbound: events,
		PrepareInbound: func(payload []byte) ([]byte, func(), error) {
			return []byte(`{"id":"M1","text":"preparado"}`), func() { cleaned <- struct{}{} }, nil
		},
	}, &mockHandler{sent: make(chan sendCall, 1)})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go agent.Run(ctx)

	events <- OutboundEvent{Kind: "inbound", Payload: []byte(`{"id":"M1","attachments":[{"path":"/x"}]}`)}
	select {
	case <-received:
		if string(got.Message) != `{"id":"M1","text":"preparado"}` {
			t.Fatalf("inbound não foi preparado: %s", got.Message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for inbound")
	}
	select {
	case <-cleaned:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup não rodou após o envio")
	}
}
