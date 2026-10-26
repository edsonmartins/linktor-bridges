package wacore

import (
	"sync"
	"time"
)

// sentTTL é por quanto tempo um ID fica lembrado como "nós enviamos". O eco
// chega em segundos; dez minutos cobrem uma reconexão que reenvie um lote
// recente e mantêm o registro pequeno.
const sentTTL = 10 * time.Minute

// sentRegistry lembra os IDs das mensagens que este bridge enviou. O WhatsApp
// marca com IsFromMe tanto o eco de um envio do Linktor (que o Linktor já
// guardou; repetir duplicaria) quanto o que o atendente digita no aparelho
// pareado (que o Linktor nunca viu e é metade da conversa). Só o primeiro é
// descartado, e só o ID que distribuímos o identifica. Mesma regra do
// adaptador WhatsApp embutido do Linktor.
type sentRegistry struct {
	mu  sync.Mutex
	ids map[string]time.Time
}

func newSentRegistry() *sentRegistry {
	return &sentRegistry{ids: make(map[string]time.Time)}
}

func (r *sentRegistry) remember(id string, now time.Time) {
	if r == nil || id == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids[id] = now
	if len(r.ids) > 2048 {
		for k, at := range r.ids {
			if now.Sub(at) > sentTTL {
				delete(r.ids, k)
			}
		}
	}
}

// isOurs diz se o ID foi enviado por nós dentro da janela. Fora dela a resposta
// é "não": deixar passar gera um duplicado visível; descartar some com a
// mensagem.
func (r *sentRegistry) isOurs(id string, now time.Time) bool {
	if r == nil || id == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	at, ok := r.ids[id]
	return ok && now.Sub(at) <= sentTTL
}
