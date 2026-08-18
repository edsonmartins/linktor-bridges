package wacore

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestLinkedDeviceIdentity(t *testing.T) {
	configureDeviceIdentity()
	if got := store.DeviceProps.GetOs(); got != linkedDeviceName {
		t.Fatalf("device name = %q, want %q", got, linkedDeviceName)
	}
	if got := store.DeviceProps.GetPlatformType(); got != waCompanionReg.DeviceProps_CHROME {
		t.Fatalf("platform = %v, want CHROME", got)
	}
}

func TestNormalizeDestination(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "digits", input: "5511999999999", want: "5511999999999@s.whatsapp.net"},
		{name: "formatted", input: "+55 (11) 99999-9999", want: "5511999999999@s.whatsapp.net"},
		{name: "jid", input: "5511999999999@s.whatsapp.net", want: "5511999999999@s.whatsapp.net"},
		{name: "group rejected", input: "123@g.us", wantErr: true},
		{name: "letters rejected", input: "abc", wantErr: true},
		{name: "short rejected", input: "1234", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeDestination(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("normalizeDestination(%q) expected error", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeDestination(%q): %v", tt.input, err)
			}
			if got.String() != tt.want {
				t.Fatalf("normalizeDestination(%q) = %q, want %q", tt.input, got.String(), tt.want)
			}
		})
	}
}

func TestLifecycleWithoutNetwork(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "session")
	if err := Initialize(dataDir); err != nil {
		t.Fatalf("Initialize(): %v", err)
	}
	t.Cleanup(func() { _ = Close() })
	if IsPaired() {
		t.Fatal("new store unexpectedly paired")
	}
	if got := Ping(); got != "wacore:ok" {
		t.Fatalf("Ping() = %q", got)
	}
	if messageID, err := GenerateMessageID(); err != nil || messageID == "" {
		t.Fatalf("GenerateMessageID() = %q, %v", messageID, err)
	}
	if _, err := SendText("5511999999999", "test"); err == nil {
		t.Fatal("SendText() without connection expected error")
	}
	if status, err := GetStatus(); err != nil {
		t.Fatalf("GetStatus(): %v", err)
	} else if !json.Valid([]byte(status)) {
		t.Fatalf("GetStatus() returned invalid JSON: %q", status)
	}
	if events, err := DrainInbound(); err != nil {
		t.Fatalf("DrainInbound(): %v", err)
	} else if !json.Valid([]byte(events)) {
		t.Fatalf("DrainInbound() returned invalid JSON: %q", events)
	}
}

func TestInboundQueueBounded(t *testing.T) {
	c := &core{}
	for range 300 {
		c.handleEvent(&events.Message{
			Message: &waProto.Message{Conversation: proto.String("x")},
			Info:    types.MessageInfo{ID: "id", Timestamp: time.Unix(1, 0)},
		})
	}
	c.mu.Lock()
	length := len(c.inbound)
	c.mu.Unlock()
	if length > 256 {
		t.Fatalf("inbound queue length = %d, want <= 256", length)
	}
	if got, err := DrainInbound(); err != nil {
		t.Fatalf("DrainInbound(): %v", err)
	} else if !json.Valid([]byte(got)) {
		t.Fatalf("DrainInbound() returned invalid JSON: %q", got)
	}
}

func TestNormalizePhone(t *testing.T) {
	got, err := normalizePhone("+55 (44) 8812-2990")
	if err != nil {
		t.Fatalf("normalizePhone(): %v", err)
	}
	if got != "554488122990" {
		t.Fatalf("normalizePhone() = %q", got)
	}
	for _, invalid := range []string{"1234", "abc", "04488122990"} {
		if _, err := normalizePhone(invalid); err == nil {
			t.Fatalf("normalizePhone(%q) expected error", invalid)
		}
	}
}
