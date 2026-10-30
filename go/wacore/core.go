// Package wacore is the shared WhatsApp bridge core used by the Android and
// desktop gateway apps, and the only place allowed to import whatsmeow.
//
// It owns the WhatsApp Multi-Device session (pairing, connection, sending and
// receiving) and exposes a small, gomobile-friendly API. Complex protocol types
// never cross this boundary: events are returned as JSON strings and sending is
// text/media oriented.
package wacore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow"
	_ "modernc.org/sqlite"
)

const (
	readyTimeout     = 30 * time.Second
	linkedDeviceName = "Linktor Bridge"
)

var singleton = &core{}

type core struct {
	mu sync.Mutex

	container *sqlstore.Container
	client    *whatsmeow.Client

	pairCtx   context.Context
	pairStop  context.CancelFunc
	pairError string
	// currentQR é o último código emitido pelo WhatsApp. O servidor troca o
	// código a cada ~20s; quem mostra o QR precisa ler sempre o atual.
	currentQR string

	inbound []InboundEvent

	// mediaDir recebe as mídias baixadas; download é trocável nos testes e
	// downloads limita quantos rodam ao mesmo tempo.
	mediaDir  string
	download  downloader
	downloads chan struct{}

	// sent lembra o que este bridge enviou, para separar eco de mensagem
	// digitada no aparelho pareado.
	sent *sentRegistry
}

// InboundEvent is a sanitized message received from WhatsApp, JSON-encoded for
// the host app (serialized by DrainInbound).
type InboundEvent struct {
	Type       string `json:"type"` // "message" | "call" | "receipt" | "connection" | "logout"
	MessageID  string `json:"id,omitempty"`
	From       string `json:"from,omitempty"`
	SenderName string `json:"sender_name,omitempty"`
	Chat       string `json:"chat,omitempty"`
	// SenderLID é o endereço LID do remetente quando o WhatsApp o usou no
	// lugar do número; From traz o número (PN) sempre que for conhecido.
	SenderLID string `json:"sender_lid,omitempty"`
	// IsFromMe marca mensagem digitada no aparelho pareado: From passa a ser
	// o cliente (o chat) e OperatorName o nome de quem digitou.
	IsFromMe     bool   `json:"is_from_me,omitempty"`
	OperatorName string `json:"operator_name,omitempty"`
	Text         string `json:"text,omitempty"`
	Timestamp    int64  `json:"ts,omitempty"`
	// Chamada (Type "call"): MessageID é o call_id, Summary a sinalização
	// (offer, accept, preaccept, transport, reject, terminate) e Video marca
	// oferta de videochamada. O áudio não passa pelo bridge.
	Summary string `json:"summary,omitempty"`
	Video   bool   `json:"video,omitempty"`
	// ContentType é text ou o tipo da mídia; MsgType guarda a variante do
	// WhatsApp (ptt, sticker). Attachments vem com Path no disco.
	ContentType string              `json:"content_type,omitempty"`
	MsgType     string              `json:"msg_type,omitempty"`
	Attachments []InboundAttachment `json:"attachments,omitempty"`
	// Location vem com content_type "location"; não há arquivo a baixar.
	Location *Location `json:"location,omitempty"`
	// Recibo (Type "receipt"): Status delivered|read para cada MessageIDs,
	// que são IDs de mensagens enviadas por este número.
	Status     string   `json:"status,omitempty"`
	MessageIDs []string `json:"message_ids,omitempty"`
}

// Location é um ponto compartilhado no WhatsApp (fixo ou ao vivo).
type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Name      string  `json:"name,omitempty"`
	Address   string  `json:"address,omitempty"`
}

// CallEventKind enumerates the call signaling events surfaced by the bridge.
type CallEventKind string

const (
	CallOffer     CallEventKind = "offer"
	CallAccept    CallEventKind = "accept"
	CallPreAccept CallEventKind = "preaccept"
	CallTransport CallEventKind = "transport"
	CallTerminate CallEventKind = "terminate"
	CallReject    CallEventKind = "reject"
)

// Status reports the current session snapshot.
type Status struct {
	Paired    bool   `json:"paired"`
	Connected bool   `json:"connected"`
	Number    string `json:"number,omitempty"`
	LastError string `json:"last_error,omitempty"`
}

// Initialize opens or creates the local session store in dataDir. Calling it
// again replaces the previous client and store cleanly.
func Initialize(dataDir string) error {
	return singleton.initialize(dataDir)
}

// IsPaired reports whether the store contains a linked WhatsApp device.
func IsPaired() bool {
	singleton.mu.Lock()
	defer singleton.mu.Unlock()
	return singleton.client != nil && singleton.client.Store.ID != nil
}

// IsConnected reports whether the client has an authenticated websocket.
func IsConnected() bool {
	singleton.mu.Lock()
	defer singleton.mu.Unlock()
	return singleton.client != nil && singleton.client.IsConnected() && singleton.client.IsLoggedIn()
}

// GetStatus returns a snapshot for the host UI.
func GetStatus() (string, error) {
	singleton.mu.Lock()
	defer singleton.mu.Unlock()
	status := Status{LastError: singleton.pairError}
	if singleton.client != nil && singleton.client.Store.ID != nil {
		status.Paired = true
		status.Number = singleton.client.Store.ID.String()
	}
	if singleton.client != nil {
		status.Connected = singleton.client.IsConnected() && singleton.client.IsLoggedIn()
	}
	return marshal(status)
}

// BeginPairing starts the QR flow and returns the first QR payload. Pairing
// continues asynchronously after this function returns; poll GetStatus or drain
// inbound events to learn the outcome.
func BeginPairing() (string, error) {
	return singleton.beginPairing()
}

// BeginCodePairing starts phone-number pairing and returns the code that must
// be entered in WhatsApp on the primary phone.
func BeginCodePairing(phone string) (string, error) {
	return singleton.beginCodePairing(phone)
}

// PairingError returns a sanitized terminal error from the asynchronous pairing
// flow, or an empty string while no error is known.
func PairingError() string {
	singleton.mu.Lock()
	defer singleton.mu.Unlock()
	return singleton.pairError
}

// CurrentQR returns the QR code currently valid for pairing, or "" when no QR
// pairing is active. The code rotates while pairing runs, so callers should
// re-read it instead of caching the first value.
func CurrentQR() string {
	singleton.mu.Lock()
	defer singleton.mu.Unlock()
	return singleton.currentQR
}

// CancelPairing stops an active QR or phone-code flow.
func CancelPairing() {
	singleton.mu.Lock()
	defer singleton.mu.Unlock()
	singleton.cancelPairingLocked()
}

// Connect connects an existing session and waits until it is authenticated.
// After BeginPairing it can also be used to wait for the authenticated
// post-pairing reconnection.
func Connect() error {
	return singleton.connect()
}

// Disconnect closes the network connection while retaining the local session.
func Disconnect() {
	singleton.disconnect()
}

// Close releases the connection and SQLite store. Initialize must be called
// again before any other operation.
func Close() error {
	return singleton.close()
}

// SendText sends a text message and returns the WhatsApp message ID.
func SendText(destination, text string) (string, error) {
	return singleton.sendText(destination, text, "")
}

// GenerateMessageID creates an ID that can be persisted before a scheduled
// send and reused by retries.
func GenerateMessageID() (string, error) {
	singleton.mu.Lock()
	defer singleton.mu.Unlock()
	if singleton.client == nil {
		return "", errors.New("core is not initialized")
	}
	return string(singleton.client.GenerateMessageID()), nil
}

// SendTextWithID sends text using a caller-persisted message ID. Reusing the
// same ID on an ambiguous retry lets the server deduplicate when possible.
func SendTextWithID(destination, text, messageID string) (string, error) {
	if strings.TrimSpace(messageID) == "" {
		return "", errors.New("message ID is required")
	}
	return singleton.sendText(destination, text, messageID)
}

// DrainInbound returns all buffered inbound events as a JSON array and clears
// the queue. It is the gomobile-compatible way to receive events.
func DrainInbound() (string, error) {
	singleton.mu.Lock()
	defer singleton.mu.Unlock()
	events := singleton.inbound
	singleton.inbound = nil
	if events == nil {
		events = []InboundEvent{}
	}
	return marshal(events)
}

// Ping is intentionally dependency-free at the API boundary and is used to
// prove the gomobile/AAR bridge.
func Ping() string {
	return "wacore:ok"
}

// RejectCall rejects an incoming WhatsApp call by its ID and caller.
func RejectCall(callFrom, callID string) error {
	singleton.mu.Lock()
	defer singleton.mu.Unlock()
	if singleton.client == nil {
		return errors.New("core is not initialized")
	}
	from, err := types.ParseJID(callFrom)
	if err != nil {
		return fmt.Errorf("invalid caller JID: %w", err)
	}
	return singleton.client.RejectCall(context.Background(), from, callID)
}

func (c *core) initialize(dataDir string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	dataDir = strings.TrimSpace(dataDir)
	if dataDir == "" {
		return errors.New("data directory is required")
	}
	if err := c.closeLocked(); err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return fmt.Errorf("create store directory: %w", err)
	}
	mediaDir := filepath.Join(dataDir, mediaDirName)
	if err := os.MkdirAll(mediaDir, 0o700); err != nil {
		return fmt.Errorf("create media directory: %w", err)
	}
	c.mediaDir = mediaDir
	if c.downloads == nil {
		c.downloads = make(chan struct{}, maxConcurrentDownloads)
	}
	configureDeviceIdentity()

	dbPath := filepath.Join(dataDir, "session.db")
	dsn := "file:" + filepath.ToSlash(dbPath) +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("open session store: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return fmt.Errorf("ping session store: %w", err)
	}

	container := sqlstore.NewWithDB(db, "sqlite3", nil)
	if err := container.Upgrade(context.Background()); err != nil {
		_ = db.Close()
		return fmt.Errorf("upgrade session store: %w", err)
	}
	device, err := container.GetFirstDevice(context.Background())
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("load device: %w", err)
	}

	// Log do protocolo em INFO: sem ele uma falha de pareamento (versão
	// recusada, 405, etc.) não deixa rastro nenhum.
	client := whatsmeow.NewClient(device, waLog.Stdout("whatsmeow", "INFO", true))
	client.EnableAutoReconnect = true
	client.AutoTrustIdentity = true
	client.AddEventHandler(c.handleEvent)

	c.container = container
	c.client = client
	return nil
}

// configureDeviceIdentity controls how newly linked sessions are presented in
// WhatsApp's device list. Existing sessions retain their original identity.
func configureDeviceIdentity() {
	store.SetOSInfo(linkedDeviceName, [3]uint32{1, 0, 0})
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_CHROME.Enum()
}

// resolveSender devolve o JID de número (PN) do remetente e, quando ele veio
// como LID, o LID original. Sem mapeamento conhecido, devolve o LID mesmo.
func (c *core) resolveSender(sender, alt types.JID) (types.JID, string) {
	if sender.Server != types.HiddenUserServer {
		return sender, ""
	}
	lid := sender.String()
	if alt.Server == types.DefaultUserServer && alt.User != "" {
		return alt.ToNonAD(), lid
	}
	if c.client != nil && c.client.Store != nil && c.client.Store.LIDs != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if pn, err := c.client.Store.LIDs.GetPNForLID(ctx, sender); err == nil && pn.User != "" {
			return pn.ToNonAD(), lid
		}
	}
	return sender, lid
}

func (c *core) handleEvent(evt any) {
	var event InboundEvent
	switch v := evt.(type) {
	case *events.Message:
		// IsFromMe sozinho não é eco: é também o que o atendente digita no
		// aparelho pareado, metade de cada conversa. Eco é só o ID que nós
		// mesmos distribuímos ao enviar.
		if v.Info.IsFromMe && c.sent.isOurs(v.Info.ID, time.Now()) {
			return
		}
		// Texto simples chega como Conversation; com prévia de link, resposta
		// citada ou em vários clientes atuais, como ExtendedTextMessage. Mídia
		// traz a legenda no próprio proto.
		text := v.Message.GetConversation()
		if text == "" {
			text = v.Message.GetExtendedTextMessage().GetText()
		}
		media := extractMedia(v.Message)
		if media != nil && text == "" {
			text = media.caption
		}
		location := extractLocation(v.Message)
		if location != nil && text == "" {
			text = location.Name
		}
		if text == "" && media == nil && location == nil {
			log.Printf("wacore: mensagem sem texto nem mídia ignorada (id=%s de %s)", v.Info.ID, v.Info.Sender.String())
			return
		}
		// Remetente em LID: o Linktor identifica o contato pelo número, então
		// traduz para o PN quando o WhatsApp informa (SenderAlt) ou quando o
		// mapeamento já está na sessão. Sem tradução, segue o LID e a resposta
		// volta por ele.
		sender, senderLID := c.resolveSender(v.Info.Sender, v.Info.SenderAlt)
		chat := v.Info.Chat
		if chat.Server == types.HiddenUserServer && chat.User == v.Info.Sender.User {
			chat = sender
		}
		senderName := v.Info.PushName
		operatorName := ""
		if v.Info.IsFromMe {
			// Mensagem do próprio número: quem identifica a conversa é o
			// cliente, que está no chat; o nome vai para operator_name para não
			// renomear o contato com o nome do atendente.
			sender, senderLID = c.resolveSender(v.Info.Chat, v.Info.RecipientAlt)
			chat = sender
			operatorName, senderName = senderName, ""
		}
		log.Printf("wacore: mensagem recebida (id=%s de %s lid=%s from_me=%t)", v.Info.ID, sender.String(), senderLID, v.Info.IsFromMe)
		event = InboundEvent{
			Type:         "message",
			MessageID:    v.Info.ID,
			From:         sender.String(),
			SenderName:   senderName,
			Chat:         chat.String(),
			SenderLID:    senderLID,
			IsFromMe:     v.Info.IsFromMe,
			OperatorName: operatorName,
			Text:         text,
			Timestamp:    v.Info.Timestamp.Unix(),
			ContentType:  "text",
		}
		if location != nil {
			event.ContentType = "location"
			event.Location = location
		}
		if media != nil {
			event.ContentType = media.kind
			event.MsgType = media.msgType
			event.Attachments = []InboundAttachment{media.attachment(v.Info.ID)}
			// O download bloqueia e não pode segurar a goroutine de eventos do
			// whatsmeow; o evento entra na fila quando o arquivo estiver no disco.
			go c.enqueueAfterDownload(event, media)
			return
		}
	case *events.CallOffer:
		event = callEvent(CallOffer, v.From, v.CallID, v.Timestamp)
		event.Video = isVideoOffer(v.Data)
	case *events.CallOfferNotice:
		// Num aparelho pareado (companion) a chamada 1:1 chega como aviso de
		// oferta, não como a oferta completa: quem atende é o celular. Para o
		// Linktor é a mesma coisa: a chamada começou a tocar.
		event = callEvent(CallOffer, v.From, v.CallID, v.Timestamp)
		event.Video = v.Media == "video"
	case *events.CallRelayLatency:
		return
	case *events.UnknownCallEvent:
		if v.Node != nil {
			log.Printf("wacore: evento de chamada desconhecido <%s>", v.Node.Tag)
		}
		return
	case *events.CallAccept:
		event = callEvent(CallAccept, v.From, v.CallID, v.Timestamp)
	case *events.CallPreAccept:
		event = callEvent(CallPreAccept, v.From, v.CallID, v.Timestamp)
	case *events.CallTransport:
		event = callEvent(CallTransport, v.From, v.CallID, v.Timestamp)
	case *events.CallReject:
		event = callEvent(CallReject, v.From, v.CallID, v.Timestamp)
	case *events.CallTerminate:
		event = callEvent(CallTerminate, v.From, v.CallID, v.Timestamp)
	case *events.PairSuccess:
		log.Printf("wacore: pareado como %s (%s)", v.ID.String(), v.Platform)
		return
	case *events.PairError:
		log.Printf("wacore: pareamento falhou: %v", v.Error)
		c.mu.Lock()
		c.pairError = "pairing failed: " + v.Error.Error()
		c.mu.Unlock()
		return
	case *events.Receipt:
		// Só entregue e lido interessam ao Linktor; sender/retry/played-self são
		// manutenção do protocolo. O Linktor casa os IDs com o que enviou e
		// ignora o resto.
		status := receiptStatus(v.Type)
		log.Printf("wacore: recibo %q (%s) de %s para %d id(s): %v", v.Type, status, v.Chat.String(), len(v.MessageIDs), v.MessageIDs)
		if status == "" || len(v.MessageIDs) == 0 {
			return
		}
		from, _ := c.resolveSender(v.Chat, v.RecipientAlt)
		event = InboundEvent{
			Type:       "receipt",
			From:       from.String(),
			Chat:       from.String(),
			Status:     status,
			MessageIDs: v.MessageIDs,
			Timestamp:  v.Timestamp.Unix(),
		}
	case *events.LoggedOut:
		event = InboundEvent{Type: "logout", Summary: "session revoked"}
	case *events.Connected:
		event = InboundEvent{Type: "connection", Summary: "connected"}
	case *events.Disconnected:
		event = InboundEvent{Type: "connection", Summary: "disconnected"}
	default:
		return
	}

	c.enqueue(event)
}

// enqueue guarda o evento para DrainInbound, descartando os mais velhos se o
// host não drena.
func (c *core) enqueue(event InboundEvent) {
	c.mu.Lock()
	if len(c.inbound) >= 256 {
		c.inbound = c.inbound[len(c.inbound)-128:]
	}
	c.inbound = append(c.inbound, event)
	c.mu.Unlock()
}

// enqueueAfterDownload baixa a mídia (com limite de concorrência) e só então
// enfileira o evento, com Path ou Error no anexo.
func (c *core) enqueueAfterDownload(event InboundEvent, media *mediaPart) {
	if c.downloads != nil {
		c.downloads <- struct{}{}
		defer func() { <-c.downloads }()
	}
	c.fetchMedia(event.MessageID, media, &event.Attachments[0])
	if event.Attachments[0].Error != "" {
		log.Printf("wacore: mídia de %s sem download: %s", event.MessageID, event.Attachments[0].Error)
	}
	c.enqueue(event)
}

// receiptStatus traduz o tipo de recibo do WhatsApp para o status do Linktor;
// vazio para os tipos que não mudam status.
func receiptStatus(t types.ReceiptType) string {
	switch t {
	case types.ReceiptTypeDelivered:
		return "delivered"
	case types.ReceiptTypeRead, types.ReceiptTypePlayed:
		return "read"
	default:
		return ""
	}
}

// isVideoOffer lê no nó da oferta se há faixa de vídeo; chamada de voz só
// traz <audio>.
func isVideoOffer(offer *waBinary.Node) bool {
	return offer != nil && len(offer.GetChildrenByTag("video")) > 0
}

func callEvent(kind CallEventKind, from types.JID, callID string, ts time.Time) InboundEvent {
	log.Printf("wacore: chamada %s (id=%s de %s)", kind, callID, from.String())
	return InboundEvent{
		Type:      "call",
		MessageID: callID,
		From:      from.String(),
		Timestamp: ts.Unix(),
		Summary:   string(kind),
	}
}

func (c *core) beginPairing() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	qrChannel, err := c.startPairingLocked()
	if err != nil {
		return "", err
	}

	for item := range qrChannel {
		switch item.Event {
		case whatsmeow.QRChannelEventCode:
			c.currentQR = item.Code
			go c.monitorPairing(qrChannel)
			return item.Code, nil
		case whatsmeow.QRChannelEventError:
			c.cancelPairingLocked()
			return "", fmt.Errorf("pairing failed: %w", item.Error)
		case whatsmeow.QRChannelTimeout.Event:
			c.cancelPairingLocked()
			return "", errors.New("pairing QR expired")
		}
	}
	c.cancelPairingLocked()
	return "", errors.New("pairing ended before a QR was available")
}

func (c *core) beginCodePairing(phone string) (string, error) {
	phone, err := normalizePhone(phone)
	if err != nil {
		return "", err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	qrChannel, err := c.startPairingLocked()
	if err != nil {
		return "", err
	}

	for item := range qrChannel {
		switch item.Event {
		case whatsmeow.QRChannelEventCode:
			code, pairErr := c.client.PairPhone(
				c.pairCtx,
				phone,
				true,
				whatsmeow.PairClientChrome,
				"Linktor (Android)",
			)
			if pairErr != nil {
				c.cancelPairingLocked()
				return "", fmt.Errorf("generate pairing code: %w", pairErr)
			}
			go c.monitorPairing(qrChannel)
			return code, nil
		case whatsmeow.QRChannelEventError:
			c.cancelPairingLocked()
			return "", fmt.Errorf("pairing failed: %w", item.Error)
		case whatsmeow.QRChannelTimeout.Event:
			c.cancelPairingLocked()
			return "", errors.New("pairing connection expired")
		}
	}
	c.cancelPairingLocked()
	return "", errors.New("pairing ended before the connection was ready")
}

func (c *core) startPairingLocked() (<-chan whatsmeow.QRChannelItem, error) {
	if c.client == nil {
		return nil, errors.New("core is not initialized")
	}
	if c.client.Store.ID != nil {
		return nil, errors.New("device is already paired")
	}
	if c.client.IsConnected() {
		return nil, errors.New("pairing is already active")
	}

	c.cancelPairingLocked()
	c.pairError = ""
	c.pairCtx, c.pairStop = context.WithCancel(context.Background())
	qrChannel, err := c.client.GetQRChannel(c.pairCtx)
	if err != nil {
		c.cancelPairingLocked()
		return nil, fmt.Errorf("start pairing: %w", err)
	}
	if err := c.client.Connect(); err != nil {
		c.cancelPairingLocked()
		return nil, fmt.Errorf("connect for pairing: %w", err)
	}
	return qrChannel, nil
}

func (c *core) monitorPairing(qrChannel <-chan whatsmeow.QRChannelItem) {
	defer func() {
		c.mu.Lock()
		c.currentQR = ""
		c.mu.Unlock()
	}()
	for item := range qrChannel {
		if item.Event == whatsmeow.QRChannelSuccess.Event {
			return
		}
		var message string
		switch item.Event {
		case whatsmeow.QRChannelEventCode:
			// Código novo: o anterior já não vale no WhatsApp.
			c.mu.Lock()
			c.currentQR = item.Code
			c.mu.Unlock()
			continue
		case whatsmeow.QRChannelEventError:
			message = "pairing failed"
			if item.Error != nil {
				message += ": " + item.Error.Error()
			}
		case whatsmeow.QRChannelTimeout.Event:
			message = "pairing expired"
		default:
			if strings.HasPrefix(item.Event, "err-") {
				message = "pairing failed: " + item.Event
			}
		}
		if message != "" {
			c.mu.Lock()
			c.pairError = message
			c.mu.Unlock()
			return
		}
	}
}

func (c *core) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client == nil {
		return errors.New("core is not initialized")
	}
	if c.client.Store.ID == nil {
		return errors.New("device is not paired")
	}
	if !c.client.IsConnected() {
		if err := c.client.Connect(); err != nil && !c.client.IsConnected() {
			return fmt.Errorf("connect: %w", err)
		}
	}
	if !c.client.WaitForConnection(readyTimeout) {
		return errors.New("connection did not become authenticated and ready")
	}
	c.cancelPairingLocked()
	return nil
}

func (c *core) disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelPairingLocked()
	if c.client != nil {
		c.client.Disconnect()
	}
}

func (c *core) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeLocked()
}

func (c *core) closeLocked() error {
	c.cancelPairingLocked()
	if c.client != nil {
		c.client.RemoveEventHandlers()
		c.client.Disconnect()
		c.client = nil
	}
	if c.container != nil {
		err := c.container.Close()
		c.container = nil
		if err != nil {
			return fmt.Errorf("close session store: %w", err)
		}
	}
	return nil
}

func (c *core) cancelPairingLocked() {
	c.currentQR = ""
	if c.pairStop != nil {
		c.pairStop()
		c.pairStop = nil
		c.pairCtx = nil
	}
}

func (c *core) sendText(destination, text, messageID string) (string, error) {
	recipient, err := normalizeDestination(destination)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(text) == "" {
		return "", errors.New("message text is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ensureReadyLocked(); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), readyTimeout)
	defer cancel()
	return c.sendMessageLocked(ctx, recipient, &waProto.Message{Conversation: proto.String(text)}, messageID)
}

// ensureReadyLocked confere que há sessão conectada e autenticada para enviar.
func (c *core) ensureReadyLocked() error {
	if c.client == nil {
		return errors.New("core is not initialized")
	}
	if !c.client.IsConnected() {
		return errors.New("client is not connected")
	}
	if !c.client.WaitForConnection(readyTimeout) {
		return errors.New("client is not connected and authenticated")
	}
	return nil
}

// sendMessageLocked envia qualquer proto de mensagem; messageID vazio deixa o
// whatsmeow gerar, ou reaproveita o ID dado para retry idempotente.
func (c *core) sendMessageLocked(ctx context.Context, recipient types.JID, message *waProto.Message, messageID string) (string, error) {
	var (
		response whatsmeow.SendResponse
		err      error
	)
	if messageID == "" {
		response, err = c.client.SendMessage(ctx, recipient, message)
	} else {
		response, err = c.client.SendMessage(ctx, recipient, message, whatsmeow.SendRequestExtra{ID: types.MessageID(messageID)})
	}
	if err != nil {
		return "", fmt.Errorf("send message: %w", err)
	}
	// Lembrado antes de devolver: o eco pode chegar enquanto o chamador ainda
	// trata o resultado, e eco não lembrado vira mensagem duplicada.
	if c.sent == nil {
		c.sent = newSentRegistry()
	}
	c.sent.remember(response.ID, time.Now())
	return response.ID, nil
}

func normalizeDestination(raw string) (types.JID, error) {
	value := strings.TrimSpace(raw)
	if strings.Contains(value, "@") {
		jid, err := types.ParseJID(value)
		if err != nil || jid.User == "" || (jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer) {
			return types.EmptyJID, errors.New("invalid destination JID; use a user JID such as 5511999999999@s.whatsapp.net or a @lid address")
		}
		return jid, nil
	}

	var digits strings.Builder
	for _, r := range value {
		switch {
		case unicode.IsDigit(r):
			digits.WriteRune(r)
		case unicode.IsSpace(r), strings.ContainsRune("+-().", r):
		default:
			return types.EmptyJID, errors.New("invalid destination; use an international number with country code")
		}
	}
	number := digits.String()
	if len(number) < 8 || len(number) > 15 {
		return types.EmptyJID, errors.New("invalid destination; expected 8 to 15 digits including country code")
	}
	return types.NewJID(number, types.DefaultUserServer), nil
}

func normalizePhone(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	var digits strings.Builder
	for _, r := range value {
		switch {
		case unicode.IsDigit(r):
			digits.WriteRune(r)
		case unicode.IsSpace(r), strings.ContainsRune("+-().", r):
		default:
			return "", errors.New("invalid phone; use an international number with country code")
		}
	}
	phone := digits.String()
	if len(phone) < 8 || len(phone) > 15 || strings.HasPrefix(phone, "0") {
		return "", errors.New("invalid phone; expected 8 to 15 digits including country code")
	}
	return phone, nil
}

func marshal(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	return string(data), nil
}
