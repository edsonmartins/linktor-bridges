package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gen2brain/beeep"
	"github.com/getlantern/systray"

	"whatsbridge/wacore"
)

const (
	statusPollInterval = 5 * time.Second
	// offlineNotifyGrace is how long the bridge must stay disconnected before a
	// native notification fires. It distinguishes a momentary blip (auto-reconnect
	// usually recovers fast) from a real outage that the user should see.
	offlineNotifyGrace = 30 * time.Second
)

// statusMonitor tracks the WhatsApp connection state, keeps the tray tooltip in
// sync and alerts the user when the bridge drops and stays disconnected. It never
// spams: a native notification fires only after offlineNotifyGrace of continuous
// disconnection, or on an unpairing/logout.
type statusMonitor struct {
	wasConnected bool
	wasPaired    bool

	// offlineSince is set when we first notice the drop; nil while connected.
	offlineSince time.Time
	notified     bool
}

// run is the monitoring loop; blocking until the app quits.
func (m *statusMonitor) run(quit <-chan struct{}) {
	ticker := time.NewTicker(statusPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-quit:
			return
		case <-ticker.C:
			m.check()
		}
	}
}

func (m *statusMonitor) check() {
	raw, err := wacore.GetStatus()
	if err != nil {
		return
	}
	var st struct {
		Paired    bool   `json:"paired"`
		Connected bool   `json:"connected"`
		Number    string `json:"number"`
	}
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		return
	}

	tooltip := "Linktor WhatsApp Bridge — "
	switch {
	case !st.Paired:
		tooltip += "não pareado. Abra o painel para conectar."
	case st.Connected:
		tooltip += "conectado" + maskNumber(st.Number)
	default:
		tooltip += "DESCONECTADO"
	}
	systray.SetTooltip(tooltip)
	if st.Connected {
		systray.SetTitle("Linktor Bridge ✓")
	} else {
		systray.SetTitle("Linktor Bridge ⚠")
	}

	// Alert only when the bridge stays disconnected past the grace period.
	now := time.Now()
	if !st.Connected && m.wasConnected {
		if m.offlineSince.IsZero() {
			m.offlineSince = now
		}
		if !m.notified && now.Sub(m.offlineSince) >= offlineNotifyGrace {
			m.notified = true
			notifyOffline()
		}
	} else if st.Connected {
		// Recovered (or never dropped): reset the offline bookkeeping.
		m.offlineSince = time.Time{}
		m.notified = false
	}

	// Also alert loudly when the device gets unpaired (logout/revocation).
	if !st.Paired && m.wasPaired {
		m.wasConnected = false
		m.wasPaired = false
		m.offlineSince = time.Time{}
		m.notified = true
		notify("Sessão revogada", "O WhatsApp foi desvinculado desta bridge. Abra o painel e pareie novamente.")
		return
	}

	m.wasConnected = st.Connected
	m.wasPaired = st.Paired
}

// notifyOffline tells the user the bridge lost connection.
func notifyOffline() {
	notify("Linktor Bridge desconectada",
		"A conexão com o WhatsApp caiu. O app continua tentando reconectar automaticamente.")
}

// notify sends a native OS notification, falling back to a log line when the
// platform backend is unavailable (e.g. headless Linux without notify-send).
func notify(title, message string) {
	if err := beeep.Notify(title, message, ""); err != nil {
		log.Printf("notification unavailable: %v (title=%s)", err, title)
		return
	}
	log.Printf("notification sent: %s", title)
}

func maskNumber(number string) string {
	if number == "" {
		return ""
	}
	if len(number) <= 4 {
		return " (" + number + ")"
	}
	return " (" + string(number[:1]) + "············" + string(number[len(number)-4:]) + ")"
}
