# Protocolo Bridge ↔ Linktor

Transporte escolhido: **WebSocket**, iniciado pela **bridge** (o device do usuário)
e autenticado por `channel_id` + token. A bridge nunca expõe porta; ela conecta de
volta ao servidor Linktor e mantém a conexão viva.

## Visão geral

```text
bridge ──(WS wss://host/api/v1/gateways/ws?channel_id=...&token=...)──► linktor
  │  frames JSON bidirecionais:
  │    bridge → linktor: hello, inbound, status, ack, bye
  │    linktor → bridge: welcome, outbound, close
```

- Sessão WhatsApp (whatsmeow) **vive na bridge**; o Linktor nunca toca no protocolo.
- Mensagens recebidas no WhatsApp → bridge envia `inbound` → Linktor publica no
  subject NATS `linktor.messages.inbound.<tipo-do-canal>` (reaproveitando o
  pipeline atual, consumidor `linktor.messages.inbound.>`).
- Comandos de envio → Linktor envia `outbound` → bridge executa `wacore.SendText*`
  → responde `ack` com `message_id` (o ack do NATS do servidor aguarda esse ack).

## Implementação no servidor (Linktor)

O lado servidor vive em `internal/gateway/`:

| Peça | Papel |
|---|---|
| `Hub` | mantém `channelID → bridge` conectada; roteia `SendToChannel` com ack correlacionado |
| `Bridge` | conexão WS + fila de `ack` por `correlation_id` (timeout 60s) |
| `RemoteAdapter` | implementa `plugin.ChannelAdapter`; registrado por canal no registry enquanto a bridge está online, então o `outbound.Worker` resolve o envio pelo device |
| `Handler` | `GET /api/v1/gateways/ws` — valida token, faz upgrade, relay de `inbound`/`status` para NATS/status do canal |

Rotas:
- `POST /api/v1/channels/:id/bridge-token` (admin/owner) — gera/rotaciona o token
  do canal (armazenado cifrado em `credentials.bridge_token`).
- `GET /api/v1/gateways/ws?channel_id=..&token=..` — conexão da bridge
  (autenticada pelo token do canal, não por JWT).

## Autenticação

`GET /api/v1/gateways/ws?channel_id=<id>&token=<token>`

- `channel_id`: ID do canal no Linktor (ex. `wh-...`).
- `token`: token de bridge gerado/rotacionado via API do servidor.
- Após o handshake (`hello`/`welcome`), frames com `channel_id` divergente são
  fechados com `close {reason: "channel_mismatch"}`.

## Frames

Todos os frames são JSON. Envelope base:

```json
{ "type": "<tipo>", "...": "..." }
```

### bridge → linktor

**hello** — primeiro frame após abrir o socket:

```json
{
  "type": "hello",
  "channel_id": "wh-abc123",
  "platform": "desktop",          // "desktop" | "android"
  "version": "0.1.0"
}
```

**inbound** — evento recebido do WhatsApp:

```json
{
  "type": "inbound",
  "channel_id": "wh-abc123",
  "message": {
    "id": "3EB0....",
    "from": "5511999999999@s.whatsapp.net",
    "chat": "5511999999999@s.whatsapp.net",
    "text": "olá",
    "ts": 1724000000
  }
}
```

**status** — mudança de estado da sessão:

```json
{
  "type": "status",
  "channel_id": "wh-abc123",
  "status": {
    "paired": true,
    "connected": true,
    "number": "5511999999999",
    "last_error": ""
  }
}
```

**ack** — resposta a um `outbound` (indispensável para idempotência):

```json
{
  "type": "ack",
  "channel_id": "wh-abc123",
  "correlation_id": "c_123",
  "ok": true,
  "message_id": "3EB0....",
  "error": ""
}
```

Em caso de falha: `"ok": false` e `"error"` com o motivo sanitizado (sem dados
sensíveis). `correlation_id` ecoa o do `outbound`.

**bye** — encerramento limpo:

```json
{ "type": "bye", "channel_id": "wh-abc123", "reason": "user_quit" }
```

### linktor → bridge

**welcome** — resposta ao `hello`:

```json
{ "type": "welcome", "ok": true, "error": "" }
```

**outbound** — comando de envio:

```json
{
  "type": "outbound",
  "correlation_id": "c_123",
  "channel_id": "wh-abc123",
  "to": "5511999999999",
  "text": "mensagem",
  "message_id": "3EB0...."       // opcional; se presente, a bridge reusa (retry idempotente)
}
```

**close** — servidor pedindo fechamento:

```json
{ "type": "close", "reason": "token_expired" }
```

## Heartbeat e reconexão

- **Ping/Pong de protocolo**: a bridge envia pings a cada 30s; o servidor responde
  pong; o servidor considera morto um socket sem tráfego por 90s.
- **Heartbeat de aplicação** (`ping`/`pong`): além do ping de transporte, a bridge
  envia a cada 20s um frame JSON `ping` carregando o status da sessão + timestamp.
  O Linktor responde `pong` e registra `last_seen` — é isso que alimenta a
  telemetria de saúde da bridge (bridge online mas sem heartbeat = `stale`).

```json
{ "type": "ping", "channel_id": "wh-abc", "ts": 1724000000, "status": {"paired":true,"connected":true,"number":"5511..."} }
{ "type": "pong", "channel_id": "wh-abc", "ts": 1724000030 }
```

- **Reconexão**: a bridge reconecta com backoff exponencial (1s → 5s → 30s, teto).
  `status` e `inbound` pendentes são enfileirados e enviados após o `welcome`.
- **Mudança de canal/token**: re-pareamento força novo `hello`.

## Saúde da bridge (no Linktor)

- `GET /api/v1/channels/:id/bridge-health` (autenticado) — snapshot da bridge do
  canal: `online`, `platform`, `version`, `connected_at`, `last_seen_ping`,
  `stale` (sem heartbeat > 90s) e `last_session` (status do device).
- `online=false` significa bridge desconectada; `stale=true` significa socket
  aberto mas sem heartbeat (bridge travada/hung).

## Regras de segurança

- Nunca logar: payload de QR, chaves de sessão, conteúdo integral do store.
- `text` e `to` podem ser mascarados nos logs (`5511****9999`).
- O servidor só aceita token válido e não expirado; token expirado → `close`.
