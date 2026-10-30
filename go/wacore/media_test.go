package wacore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestExtractMedia(t *testing.T) {
	cases := []struct {
		name          string
		msg           *waProto.Message
		kind, msgType string
		caption       string
	}{
		{"imagem", &waProto.Message{ImageMessage: &waProto.ImageMessage{Mimetype: proto.String("image/jpeg"), Caption: proto.String("olha")}}, "image", "", "olha"},
		{"vídeo", &waProto.Message{VideoMessage: &waProto.VideoMessage{Mimetype: proto.String("video/mp4")}}, "video", "", ""},
		{"voz", &waProto.Message{AudioMessage: &waProto.AudioMessage{Mimetype: proto.String("audio/ogg; codecs=opus"), PTT: proto.Bool(true)}}, "audio", "ptt", ""},
		{"áudio", &waProto.Message{AudioMessage: &waProto.AudioMessage{Mimetype: proto.String("audio/mpeg")}}, "audio", "", ""},
		{"documento", &waProto.Message{DocumentMessage: &waProto.DocumentMessage{Mimetype: proto.String("application/pdf"), FileName: proto.String("nota.pdf")}}, "document", "", ""},
		{"figurinha", &waProto.Message{StickerMessage: &waProto.StickerMessage{Mimetype: proto.String("image/webp")}}, "image", "sticker", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := extractMedia(tc.msg)
			if p == nil {
				t.Fatal("extractMedia() = nil")
			}
			if p.kind != tc.kind || p.msgType != tc.msgType || p.caption != tc.caption {
				t.Fatalf("got kind=%s msgType=%s caption=%q", p.kind, p.msgType, p.caption)
			}
		})
	}
	if extractMedia(&waProto.Message{Conversation: proto.String("texto")}) != nil {
		t.Fatal("texto não é mídia")
	}
	if att := (&mediaPart{kind: "document", mimeType: "application/pdf", filename: "nota.pdf"}).attachment("ID1"); att.Filename != "nota.pdf" {
		t.Fatalf("documento deve manter o nome: %+v", att)
	}
	if att := (&mediaPart{kind: "image", mimeType: "image/jpeg"}).attachment("ID1"); att.Filename != "ID1.jpg" {
		t.Fatalf("imagem sem nome ganha id+extensão: %+v", att)
	}
}

// Uma imagem entra na fila só depois do download, com o arquivo em mediaDir;
// a legenda vira o texto. Download falho ou grande demais mantém o registro
// com o motivo.
func TestMediaEventQueuedAfterDownload(t *testing.T) {
	dir := t.TempDir()
	c := &core{mediaDir: dir, downloads: make(chan struct{}, 2)}
	c.download = func(_ context.Context, src whatsmeow.DownloadableMessage) ([]byte, error) {
		if img, ok := src.(*waProto.ImageMessage); ok && img.GetCaption() == "falha" {
			return nil, errors.New("rede")
		}
		return []byte("jpegbytes"), nil
	}

	send := func(id, caption string, size uint64) {
		c.handleEvent(&events.Message{
			Message: &waProto.Message{ImageMessage: &waProto.ImageMessage{
				Mimetype: proto.String("image/jpeg"), Caption: proto.String(caption), FileLength: proto.Uint64(size),
			}},
			Info: types.MessageInfo{
				ID: id, Timestamp: time.Unix(1, 0),
				MessageSource: types.MessageSource{Sender: types.NewJID("5511999999999", types.DefaultUserServer), Chat: types.NewJID("5511999999999", types.DefaultUserServer)},
			},
		})
	}
	send("OK1", "olha", 9)
	send("ERR1", "falha", 9)
	send("BIG1", "grande", maxInboundMediaBytes+1)

	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		n := len(c.inbound)
		c.mu.Unlock()
		if n == 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.mu.Lock()
	byID := map[string]InboundEvent{}
	for _, e := range c.inbound {
		byID[e.MessageID] = e
	}
	c.mu.Unlock()
	if len(byID) != 3 {
		t.Fatalf("fila com %d eventos, esperava 3", len(byID))
	}

	ok := byID["OK1"]
	if ok.ContentType != "image" || ok.Text != "olha" || len(ok.Attachments) != 1 {
		t.Fatalf("evento ok: %+v", ok)
	}
	if ok.Attachments[0].Path == "" || !strings.HasPrefix(ok.Attachments[0].Path, dir) || ok.Attachments[0].SizeBytes != 9 {
		t.Fatalf("anexo ok: %+v", ok.Attachments[0])
	}
	if data, _ := os.ReadFile(ok.Attachments[0].Path); string(data) != "jpegbytes" {
		t.Fatalf("arquivo = %q", data)
	}
	if filepath.Ext(ok.Attachments[0].Path) != ".jpg" {
		t.Fatalf("extensão = %q", ok.Attachments[0].Path)
	}

	if att := byID["ERR1"].Attachments[0]; att.Path != "" || !strings.Contains(att.Error, "download failed") {
		t.Fatalf("anexo com falha: %+v", att)
	}
	if att := byID["BIG1"].Attachments[0]; att.Path != "" || !strings.Contains(att.Error, "exceeds") {
		t.Fatalf("anexo grande: %+v", att)
	}
}

func TestSendMediaWithoutSession(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.jpg")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := &core{}
	if _, err := c.sendMedia("5511999999999", "image", path, "image/jpeg", "a.jpg", "", false, ""); err == nil {
		t.Fatal("sem sessão deve falhar")
	}
	if _, err := c.sendMedia("5511999999999", "image", filepath.Join(dir, "nao-existe"), "", "", "", false, ""); err == nil {
		t.Fatal("arquivo inexistente deve falhar")
	}
}

// Mensagem digitada no aparelho pareado segue com is_from_me, arquivada no
// cliente (chat) e sem renomear o contato; eco do que o bridge enviou cai fora.
func TestFromMeForwardedEchoDropped(t *testing.T) {
	c := &core{sent: newSentRegistry()}
	customer := types.NewJID("5511999999999", types.DefaultUserServer)
	me := types.NewJID("554497239232", types.DefaultUserServer)
	typed := func(id string) *events.Message {
		return &events.Message{
			Message: &waProto.Message{Conversation: proto.String("respondi pelo app")},
			Info: types.MessageInfo{
				ID: id, Timestamp: time.Unix(1, 0), PushName: "Loja",
				MessageSource: types.MessageSource{Sender: me, Chat: customer, IsFromMe: true},
			},
		}
	}
	c.sent.remember("ECHO1", time.Now())
	c.handleEvent(typed("ECHO1"))
	c.handleEvent(typed("TYPED1"))

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.inbound) != 1 {
		t.Fatalf("fila com %d eventos, esperava só a digitada", len(c.inbound))
	}
	e := c.inbound[0]
	if e.MessageID != "TYPED1" || !e.IsFromMe || e.From != customer.String() || e.Chat != customer.String() {
		t.Fatalf("evento: %+v", e)
	}
	if e.SenderName != "" || e.OperatorName != "Loja" {
		t.Fatalf("nome do atendente não pode virar nome do contato: %+v", e)
	}
}

// Localização entra como content_type location, com o nome no texto e as
// coordenadas no evento; ao vivo usa a legenda como nome.
func TestLocationForwarded(t *testing.T) {
	c := &core{}
	from := types.NewJID("5511999999999", types.DefaultUserServer)
	c.handleEvent(&events.Message{
		Message: &waProto.Message{LocationMessage: &waProto.LocationMessage{
			DegreesLatitude: proto.Float64(-23.55), DegreesLongitude: proto.Float64(-46.63), Name: proto.String("Loja"), Address: proto.String("Av. Paulista, 1"),
		}},
		Info: types.MessageInfo{ID: "LOC1", Timestamp: time.Unix(1, 0), MessageSource: types.MessageSource{Sender: from, Chat: from}},
	})
	c.handleEvent(&events.Message{
		Message: &waProto.Message{LiveLocationMessage: &waProto.LiveLocationMessage{
			DegreesLatitude: proto.Float64(-1), DegreesLongitude: proto.Float64(-2), Caption: proto.String("a caminho"),
		}},
		Info: types.MessageInfo{ID: "LOC2", Timestamp: time.Unix(1, 0), MessageSource: types.MessageSource{Sender: from, Chat: from}},
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.inbound) != 2 {
		t.Fatalf("fila com %d eventos, esperava 2", len(c.inbound))
	}
	fixed, live := c.inbound[0], c.inbound[1]
	if fixed.ContentType != "location" || fixed.Text != "Loja" || fixed.Location == nil || fixed.Location.Latitude != -23.55 || fixed.Location.Address != "Av. Paulista, 1" {
		t.Fatalf("fixa: %+v %+v", fixed, fixed.Location)
	}
	if live.Text != "a caminho" || live.Location == nil || live.Location.Longitude != -2 {
		t.Fatalf("ao vivo: %+v %+v", live, live.Location)
	}
}

// Recibo de entrega/leitura vira evento receipt com os IDs; os demais tipos
// (sender, retry, read-self) não geram nada.
func TestReceiptForwarded(t *testing.T) {
	c := &core{}
	customer := types.NewJID("5511999999999", types.DefaultUserServer)
	for _, rt := range []types.ReceiptType{types.ReceiptTypeDelivered, types.ReceiptTypeRead, types.ReceiptTypeSender, types.ReceiptTypeReadSelf} {
		c.handleEvent(&events.Receipt{
			MessageSource: types.MessageSource{Chat: customer, Sender: customer},
			MessageIDs:    []types.MessageID{"A1", "A2"},
			Timestamp:     time.Unix(1, 0),
			Type:          rt,
		})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.inbound) != 2 {
		t.Fatalf("fila com %d eventos, esperava delivered e read", len(c.inbound))
	}
	if e := c.inbound[0]; e.Type != "receipt" || e.Status != "delivered" || len(e.MessageIDs) != 2 || e.From != customer.String() {
		t.Fatalf("delivered: %+v", e)
	}
	if e := c.inbound[1]; e.Status != "read" {
		t.Fatalf("read: %+v", e)
	}
}

// Sinalização de chamada vira evento call com o call_id, quem ligou e o tipo
// de sinal; a oferta diz se é videochamada pela faixa de vídeo do nó.
func TestCallOfferForwarded(t *testing.T) {
	c := &core{}
	from := types.NewJID("5511999999999", types.DefaultUserServer)
	c.handleEvent(&events.CallOffer{
		BasicCallMeta: types.BasicCallMeta{From: from, CallID: "CALL1", Timestamp: time.Unix(1, 0)},
		Data:          &waBinary.Node{Tag: "offer", Content: []waBinary.Node{{Tag: "audio"}, {Tag: "video"}}},
	})
	c.handleEvent(&events.CallOffer{
		BasicCallMeta: types.BasicCallMeta{From: from, CallID: "CALL2", Timestamp: time.Unix(2, 0)},
		Data:          &waBinary.Node{Tag: "offer", Content: []waBinary.Node{{Tag: "audio"}}},
	})
	c.handleEvent(&events.CallTerminate{BasicCallMeta: types.BasicCallMeta{From: from, CallID: "CALL2", Timestamp: time.Unix(3, 0)}})
	// aparelho pareado recebe a chamada 1:1 como aviso de oferta
	c.handleEvent(&events.CallOfferNotice{BasicCallMeta: types.BasicCallMeta{From: from, CallID: "CALL3", Timestamp: time.Unix(4, 0)}, Media: "video"})
	c.handleEvent(&events.CallRelayLatency{BasicCallMeta: types.BasicCallMeta{From: from, CallID: "CALL3"}})
	c.handleEvent(&events.UnknownCallEvent{Node: &waBinary.Node{Tag: "mute"}})
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.inbound) != 4 {
		t.Fatalf("fila com %d eventos, esperava 4", len(c.inbound))
	}
	video, voice, end := c.inbound[0], c.inbound[1], c.inbound[2]
	if video.Type != "call" || video.MessageID != "CALL1" || video.Summary != "offer" || !video.Video || video.From != from.String() {
		t.Fatalf("oferta de vídeo: %+v", video)
	}
	if voice.Video || voice.Summary != "offer" {
		t.Fatalf("oferta de voz: %+v", voice)
	}
	if end.Summary != "terminate" || end.MessageID != "CALL2" || end.Timestamp != 3 {
		t.Fatalf("término: %+v", end)
	}
	if notice := c.inbound[3]; notice.Summary != "offer" || notice.MessageID != "CALL3" || !notice.Video {
		t.Fatalf("aviso de oferta: %+v", notice)
	}
}

// Chamada sinalizada por LID segue com o número em From (mesmo contato das
// mensagens) e o LID em SenderLID, que a recusa usa como endereço.
func TestCallFromLIDResolvedToNumber(t *testing.T) {
	c := &core{}
	lid := types.NewJID("252415295144120", types.HiddenUserServer)
	pn := types.NewJID("554499806761", types.DefaultUserServer)
	c.handleEvent(&events.CallOfferNotice{BasicCallMeta: types.BasicCallMeta{From: lid, CallCreator: lid, CallCreatorAlt: pn, CallID: "L1", Timestamp: time.Unix(5, 0)}, Media: "audio"})
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.inbound) != 1 {
		t.Fatalf("fila com %d eventos", len(c.inbound))
	}
	if e := c.inbound[0]; e.From != pn.String() || e.SenderLID != lid.String() {
		t.Fatalf("identidade: from=%s lid=%s", e.From, e.SenderLID)
	}
}
