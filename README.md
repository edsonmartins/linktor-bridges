# Linktor Bridge

Bridges de WhatsApp Multi-Device que rodam **no device do usuário** (Android ou
PC Windows/Linux/macOS) e conectam o [Linktor](https://github.com/msgfy/linktor)
ao WhatsApp por WebSocket — mantendo a sessão onde o WhatsApp Web já é usado.

![Arquitetura Linktor Bridge](docs/linktor_bridges_diagrama.png)

## A ideia

O Linktor é uma plataforma omnichannel: inbox unificado, bots/IA, automações e
agentes humanos conversando com clientes por múltiplos canais (Cloud API, SMS,
e-mail, Telegram, RCS). O WhatsApp é, de longe, o canal mais crítico — mas é
também o mais restritivo para integrações não-oficiais.

O WhatsApp correlaciona o dispositivo vinculado pelo **IP e localização**.
Sessões `whatsmeow` rodando em datacenter nascem em um IP "estranho" e são
frequentemente interpretadas como anomalia e bloqueadas. A bridge resolve isso
com uma inversão de arquitetura:

> **A sessão WhatsApp não vive no servidor. Vive no device do usuário.**
> A ponte é o canal: o Linktor fala com a bridge, e a bridge fala com o WhatsApp.

O resultado:

- **Mesma localização física** do uso legítimo do WhatsApp Web → menor risco de
  bloqueio/ban.
- **Sessão privada**: identidade e chaves ficam no armazenamento local do
  usuário (SQLite), nunca no servidor.
- **O servidor nunca toca o protocolo WhatsApp** — apenas troca mensagens
  estruturadas com a bridge por WebSocket autenticado.

## Onde se aplica

- **Empresas que já usam o Linktor** e precisam de WhatsApp sem Cloud API
  (custo, indisponibilidade ou política da Meta).
- **Operações com atendimento humano via WhatsApp** onde o dono da linha (CNPJ
  ou pessoa) mantém o número no celular/PC e quer centralizar no Inbox.
- **Hospedagem on-premise / VPS própria** (o device da bridge pode ser um
  mini-PC ou máquina virtual no mesmo datacenter regional).
- **Ambientes de teste/staging**: linhas de teste pareadas em dispositivos
  descartáveis, sem depender de número de produção.

## Fluxo de mensagens

```text
[ Device do usuário ]                          [ Servidor Linktor ]
┌────────────────────────────┐                 ┌─────────────────────────────┐
│ bridge (Android ou PC)     │                 │ plataforma (inbox, bots, IA)│
│  whatsmeow.Client ◄───────┐ │  WS + token    │ gateway endpoint (WS)       │
│  store SQLite (local)     │ │◄──────────────►│  RemoteAdapter → outbound   │
│  UI pareamento QR/código  │ │   inbound/     │  NATS linktor.messages.*    │
└────────────────────────────┘ │   status      └─────────────────────────────┘
                               └──────────────►
```

- **Inbound** (WhatsApp → Linktor): a bridge recebe a mensagem, persiste no
  store local (entrega durável) e publica no subject
  `linktor.messages.inbound.whatsapp` — consumido pelo pipeline já existente.
- **Outbound** (Linktor → WhatsApp): o `RemoteAdapter` no servidor envia um
  comando pelo WS e aguarda ack com `message_id`; a bridge executa
  `wacore.SendText*` e confirma.
- **Voz**: sinalização de chamada trafega como `call_event`/`call_command`
  (incluindo rejeição); a engine de chamada (mlow/SRTP) roda na bridge.
- **Health**: `ping`/`pong` de aplicação + `bridge_health` no Channel
  (online/stale) para o Linktor saber se o canal está operante.

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
├── docs/
│   ├── PROTOCOL.md                  # contrato WS bridge↔Linktor
│   └── linktor_bridges_diagrama.png # diagrama de arquitetura
├── scripts/build-aar.sh             # gera android/app/libs/wacore.aar
└── README.md
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

### Sem bandeja (Linux sem GTK/cgo, servidores, CI)

```bash
CGO_ENABLED=0 go build -tags notray -o linktor-bridge ./cmd/bridge-desktop
./linktor-bridge --server wss://host/api/v1/gateways/ws --channel-id <id> --token <token>
```

Com `-tags notray` a bandeja fica de fora e o binário é 100% Go: o painel
continua em `127.0.0.1:<porta>` (a URL sai no terminal), o monitor de status
loga as mudanças e Ctrl+C encerra. Notificações nativas seguem funcionando onde
houver `notify-send`/D-Bus.


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
- [x] Voz: `call_event`/`call_command` + rejeição de chamada pela bridge

### Roadmap

- [ ] **Mídia**: imagem/documento/vídeo/áudio outbound e inbound pelo protocolo
  (hoje texto-only; media retorna erro permanente explícito)
- [ ] **Interativos**: botões/listas native-flow sobre a bridge
- [ ] **Registrar usuário + canal**: UX no Linktor para criar canal bridge e
  gerar token (ex.: flag `whatsapp_remote` ou campo `transport: bridge`)
- [ ] **Mídia inbound com rehost**: baixar/decifrar no device e subir no store
  do Linktor (`rehostInboundMedia`)
- [ ] **Voz com mídia**: streaming PCM 16k bridge↔Linktor (engine na bridge)
- [ ] **Teste E2E ao vivo**: parear device real → enviar por
  `POST /api/v1/conversations/:id/messages` → confirmar no Inbox
- [ ] Bridge Android: agent completo via AAR (inbound/outbound/heartbeat)
- [ ] Instaladores desktop (MSI/dmg/deb) + autostart na bandeja
- [ ] Reconexão do embedded whatsmeow não sobrescrever `RemoteAdapter` (race)

> **Aviso:** `whatsmeow` implementa o protocolo WhatsApp Web/Multi-Device e não é
> API oficial da Meta. Use contas/números de teste. Risco operacional existente.
