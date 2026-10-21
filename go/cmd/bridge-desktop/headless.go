//go:build notray

package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// runUI é a variante sem bandeja (go build -tags notray): serve para Linux sem
// GTK/cgo, servidores e CI. Imprime o painel, segue monitorando o estado e
// espera Ctrl+C ou SIGTERM para encerrar.
func runUI(dashboard string) {
	fmt.Printf("modo sem bandeja: abra o painel em %s (Ctrl+C encerra)\n", dashboard)

	quitStatus := make(chan struct{})
	defer close(quitStatus)
	go new(statusMonitor).run(quitStatus)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	fmt.Println("encerrando…")
	shutdown()
}

// trayStatus não tem bandeja para atualizar; o estado vai para o log.
func trayStatus(tooltip string, connected bool) {
	_ = connected
	if tooltip != lastTooltip {
		lastTooltip = tooltip
		fmt.Println(tooltip)
	}
}

var lastTooltip string
