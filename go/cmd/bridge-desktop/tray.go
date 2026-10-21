//go:build !notray

package main

import (
	"fmt"
	"image"
	"image/color"
	"image/png"
	"time"

	"github.com/getlantern/systray"

	"whatsbridge/wacore"
)

// runUI roda a bandeja do sistema (precisa de cgo no macOS e no Linux). A
// variante sem bandeja está em headless.go (build tag notray).
func runUI(dashboard string) {
	systray.Run(
		func() { onReady(dashboard) },
		func() {},
	)
}

// trayStatus reflete o estado da conexão no tooltip e no título da bandeja.
func trayStatus(tooltip string, connected bool) {
	systray.SetTooltip(tooltip)
	if connected {
		systray.SetTitle("Linktor Bridge ✓")
	} else {
		systray.SetTitle("Linktor Bridge ⚠")
	}
}

func onReady(dashboard string) {
	systray.SetTitle("Linktor Bridge")
	systray.SetTooltip("Linktor WhatsApp Bridge")
	systray.SetIcon(trayIcon())

	mOpen := systray.AddMenuItem("Abrir painel", "Abrir o dashboard local")
	mPair := systray.AddMenuItem("Conectar WhatsApp", "Iniciar pareamento QR")
	mQuit := systray.AddMenuItem("Sair", "Encerrar a bridge")

	// Track connection state: tray tooltip + native OS notification when the
	// bridge drops and stays disconnected (or the session is revoked).
	quitStatus := make(chan struct{})
	defer close(quitStatus)
	go new(statusMonitor).run(quitStatus)

	go func() {
		for {
			select {
			case <-mOpen.ClickedCh:
				openBrowser(dashboard)
			case <-mPair.ClickedCh:
				qr, err := wacore.BeginPairing()
				if err != nil {
					fmt.Printf("pairing not started: %v\n", err)
					continue
				}
				lastQR.payload, lastQR.at = qr, time.Now()
				fmt.Printf("pairing QR ready: %s\n", dashboard)
			case <-mQuit.ClickedCh:
				shutdown()
				systray.Quit()
				return
			}
		}
	}()
}

func trayIcon() []byte {
	img := image.NewRGBA(image.Rect(0, 0, 22, 22))
	blue := color.RGBA{R: 46, G: 120, B: 210, A: 255}
	for y := 0; y < 22; y++ {
		for x := 0; x < 22; x++ {
			img.Set(x, y, blue)
		}
	}
	var buf []byte
	var w = &byteWriter{buf: &buf}
	if err := png.Encode(w, img); err != nil {
		return nil
	}
	// png.Encode writes via Write; collect raw bytes.
	return w.collected()
}

type byteWriter struct {
	buf *[]byte
}

func (w *byteWriter) Write(p []byte) (int, error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}

func (w *byteWriter) collected() []byte {
	return *w.buf
}
