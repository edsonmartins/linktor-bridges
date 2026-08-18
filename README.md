# Linktor Bridge

Bridges de WhatsApp Multi-Device que rodam **no device do usuário** (Android ou
PC Windows/Linux/macOS), onde o WhatsApp Web já é usado, e conectam de volta ao
[Linktor](https://github.com/msgfy/linktor) via WebSocket.

## Por quê

O WhatsApp correlaciona o dispositivo vinculado pelo IP/localização. Sessões
whatsmeow rodando em datacenter podem ser interpretadas como anomalia e
bloqueadas. A bridge resolve isso executando a sessão no mesmo lugar em que o
usuário normalmente usa WhatsApp Web.

## Estrutura

```text
bridges/
├── go/                              # módulo Go compartilhado
│   ├── wacore/                      # core único autorizado a importar whatsmeow
│   │   ├── core.go                  #   pairing QR/código, connect, send, inbound
│   │   └── core_test.go
│   ├── agent/                       # cliente WS do protocolo bridge↔Linktor
│   │   ├── agent.go                 #   hello/outbound/ack/inbound/status/ping
│   │   └── agent_test.go            #   testes contra servidor WS mockado
│   ├── eventstore/                  # fila durável (SQLite) para eventos inbound
│   │   └── store.go
│   └── cmd/bridge-desktop/          # app desktop (system tray + web local)
│       └── webui/index.html         #   dashboard embutido (go:embed)
├── android/                         # app Android (compõe o AAR via gomobile)
│   ├── app/src/main/java/dev/whatsbridge/
│   │   ├── MainActivity.kt          #   UI de status/pareamento/envio
│   │   └── wa/WaGateway.kt          #   wrapper JNI sobre wacore
│   └── settings.gradle.kts
├── docs/PROTOCOL.md                 # contrato WS bridge↔Linktor
├── scripts/build-aar.sh             # gera android/app/libs/wacore.aar
└── README.md
```

## Arquitetura da integração com o Linktor

```text
[ Device do usuário ]                          [ Servidor Linktor ]
┌────────────────────────────┐                 ┌─────────────────────────────┐
│ bridge (Android ou PC)     │                 │ plataforma (inbox, bots, IA)│
│  whatsmeow.Client ◄───────┐ │  WS + token    │ gateway endpoint (WS)       │
│  store SQLite (local)     │ │◄──────────────►│  RemoteSender → outbound    │
│  UI pareamento QR/código  │ │   inbound/     │  NATS linktor.messages.*    │
└────────────────────────────┘ │   status      └─────────────────────────────┘
                               └──────────────►
```

- **Inbound** (WhatsApp → Linktor): bridge publica no subject
  `linktor.messages.inbound.whatsapp` (consumidor já existente no servidor).
- **Outbound** (Linktor → WhatsApp): novo `RemoteSender` no servidor envia
  comando pelo WS e aguarda ack com `message_id`; a bridge executa
  `wacore.SendText*`.
- **Status**: bridge reporta `connected/disconnected/logged_out` → servidor
  atualiza `channels.connection_status` + evento `channel.connection`.

### Protocolo

Contrato completo (frames hello/outbound/ack/inbound/status/bye, autenticação,
heartbeat e reconexão) em `docs/PROTOCOL.md`.

```bash
cd go
go test ./agent/   # valida hello+outbound+ack e falha de auth contra mock WS
```

## Como rodar

### Desktop (Windows/Linux/macOS)

```bash
cd go
go run ./cmd/bridge-desktop
# store: ~/.linktor/linktor-bridge/
# dashboard: http://127.0.0.1:<porta>
```

Conectando a um Linktor:

```bash
go run ./cmd/bridge-desktop \
  --server wss://linktor.example.com/api/v1/gateways/ws \
  --channel-id wh-abc123 \
  --token <bridge-token>
```

**Entrega durável de eventos**: mensagens recebidas (inbound) são persistidas
num store SQLite local (`~/.linktor/linktor-bridge/events.db`) antes de serem
enviadas ao gateway. Se a conexão cair, os eventos ficam enfileirados no device
e são reenviados quando a conexão volta — sem perda por outage longo do link.

Builds:

```bash
# macOS (exige cgo para a bandeja nativa — rodar na própria máquina ou CI macOS)
CGO_ENABLED=1 go build -o bin/bridge-darwin-arm64 ./cmd/bridge-desktop

# Windows (pure-Go)
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o bin/bridge-windows-amd64.exe ./cmd/bridge-desktop

# Linux (exige cgo/X11 para a bandeja — buildar no alvo ou container)
GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -o bin/bridge-linux-amd64 ./cmd/bridge-desktop
```

> A bandeja usa `getlantern/systray` (AppKit no macOS, X11 no Linux, nativo no
> Windows), por isso só o Windows tem build 100% pure-Go. O restante do binário
> (whatsmeow, sqlite modernc) é pure-Go — o cgo é apenas da bandeja.

### Android

```bash
export ANDROID_HOME=/caminho/para/Android/sdk
./scripts/build-aar.sh          # gera android/app/libs/wacore.aar
./gradlew :app:assembleDebug    # na pasta android/
```

Instale `android/app/build/outputs/apk/debug/app-debug.apk` no aparelho.

## Validação

```bash
cd go
gofmt -w .
go test ./...
go vet ./...
go build ./cmd/bridge-desktop
```

## Próximos passos

### Feito

- [x] Protocolo bridge↔Linktor (`docs/PROTOCOL.md`) + cliente `agent` com testes
- [x] Endpoint WS no Linktor (`/api/v1/gateways/ws`) + `RemoteAdapter`
- [x] Heartbeat de aplicação (`ping`/`pong`) + saúde da bridge no Linktor
  (`GET /api/v1/channels/:id/bridge-health` e `bridge_health` no Channel)
- [x] `sender_name` (pushName) propagado no inbound
- [x] Store durável de eventos na bridge (`eventstore`, SQLite local)
- [x] Notificação nativa + tooltip no tray quando a bridge desconecta

### Roadmap

- [ ] **Mídia**: imagem/documento/vídeo/áudio outbound e inbound pelo protocolo
  (hoje texto-only; media retorna erro permanente explícito)
- [ ] **Interativos**: botões/listas native-flow sobre a bridge
- [ ] **Registrar usuário + canal**: UX no Linktor para criar canal bridge e
  gerar token (ex.: flag `whatsapp_remote` ou campo `transport: bridge`)
- [ ] **Mídia inbound com rehost**: baixar/decifrar no device e subir no store
  do Linktor (`rehostInboundMedia`)
- [ ] **Teste E2E ao vivo**: parear device real → enviar por
  `POST /api/v1/conversations/:id/messages` → confirmar no Inbox
- [ ] Bridge Android: agent completo via AAR (inbound/outbound/heartbeat)
- [ ] Instaladores desktop (MSI/dmg/deb) + autostart na bandeja
- [ ] Reconexão do embedded whatsmeow não sobrescrever `RemoteAdapter` (race)

> **Aviso:** `whatsmeow` implementa o protocolo WhatsApp Web/Multi-Device e não é
> API oficial da Meta. Use contas/números de teste. Risco operacional existente.
