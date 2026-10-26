package wacore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// Mídia atravessa a fronteira do wacore como arquivo em <dataDir>/media, nunca
// como bytes em JSON: a API é restrita pelo gomobile (strings e []byte) e a
// fila de eventos é carregada inteira na memória a cada ciclo.

const (
	mediaDirName = "media"
	// maxInboundMediaBytes é o maior anexo baixado do WhatsApp. Fotos têm no
	// máximo 16 MB; vídeos e documentos maiores seguem só com o registro.
	maxInboundMediaBytes = 32 << 20
	// maxConcurrentDownloads evita travar a máquina quando chegam dezenas de
	// fotos de uma vez.
	maxConcurrentDownloads = 4
	downloadTimeout        = 2 * time.Minute
	// mediaRetention é por quanto tempo um arquivo esquecido (bridge caiu antes
	// de enviar) fica no disco antes de PurgeMedia apagá-lo.
	mediaRetention = 24 * time.Hour
)

// InboundAttachment é um anexo já baixado e decifrado, pronto para o host
// mandar ao Linktor. Path aponta para o arquivo; Error explica quando o
// download não aconteceu (grande demais, falha de rede), e então só nome, tipo
// e tamanho seguem.
type InboundAttachment struct {
	Type      string `json:"type"` // image | video | audio | document
	MimeType  string `json:"mime_type,omitempty"`
	Filename  string `json:"filename,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	Path      string `json:"path,omitempty"`
	Error     string `json:"error,omitempty"`
}

// mediaPart é o que uma mensagem de mídia do WhatsApp tem antes do download.
type mediaPart struct {
	kind     string // image | video | audio | document
	msgType  string // ptt | sticker | "" (variante do WhatsApp)
	mimeType string
	filename string
	caption  string
	size     uint64
	source   whatsmeow.DownloadableMessage
}

// extractMedia lê a mídia de uma mensagem, ou nil se ela não tem mídia.
// Figurinha vira imagem marcada; áudio de voz vira áudio marcado como ptt.
func extractMedia(m *waProto.Message) *mediaPart {
	switch {
	case m.GetImageMessage() != nil:
		img := m.GetImageMessage()
		return &mediaPart{kind: "image", mimeType: img.GetMimetype(), caption: img.GetCaption(), size: img.GetFileLength(), source: img}
	case m.GetVideoMessage() != nil:
		vid := m.GetVideoMessage()
		return &mediaPart{kind: "video", mimeType: vid.GetMimetype(), caption: vid.GetCaption(), size: vid.GetFileLength(), source: vid}
	case m.GetAudioMessage() != nil:
		aud := m.GetAudioMessage()
		p := &mediaPart{kind: "audio", mimeType: aud.GetMimetype(), size: aud.GetFileLength(), source: aud}
		if aud.GetPTT() {
			p.msgType = "ptt"
		}
		return p
	case m.GetDocumentMessage() != nil:
		doc := m.GetDocumentMessage()
		return &mediaPart{kind: "document", mimeType: doc.GetMimetype(), filename: doc.GetFileName(), caption: doc.GetCaption(), size: doc.GetFileLength(), source: doc}
	case m.GetStickerMessage() != nil:
		st := m.GetStickerMessage()
		return &mediaPart{kind: "image", msgType: "sticker", mimeType: st.GetMimetype(), size: st.GetFileLength(), source: st}
	}
	return nil
}

// extractLocation lê uma localização fixa ou ao vivo, ou nil. O nome vira o
// texto da mensagem e o endereço vai à parte, como no adaptador embutido.
func extractLocation(m *waProto.Message) *Location {
	if loc := m.GetLocationMessage(); loc != nil {
		return &Location{Latitude: loc.GetDegreesLatitude(), Longitude: loc.GetDegreesLongitude(), Name: loc.GetName(), Address: loc.GetAddress()}
	}
	if live := m.GetLiveLocationMessage(); live != nil {
		return &Location{Latitude: live.GetDegreesLatitude(), Longitude: live.GetDegreesLongitude(), Name: live.GetCaption()}
	}
	return nil
}

// attachment monta o registro do anexo antes do download.
func (p *mediaPart) attachment(messageID string) InboundAttachment {
	filename := p.filename
	if filename == "" {
		filename = messageID + extensionFor(p.mimeType)
	}
	return InboundAttachment{
		Type:      p.kind,
		MimeType:  p.mimeType,
		Filename:  filename,
		SizeBytes: int64(p.size),
	}
}

// downloader baixa e decifra uma mídia. É um campo para os testes trocarem
// pelo que quiserem sem uma sessão real.
type downloader func(ctx context.Context, source whatsmeow.DownloadableMessage) ([]byte, error)

func (c *core) clientDownloader() downloader {
	return func(ctx context.Context, source whatsmeow.DownloadableMessage) ([]byte, error) {
		c.mu.Lock()
		client := c.client
		c.mu.Unlock()
		if client == nil {
			return nil, errors.New("core is not initialized")
		}
		return client.Download(ctx, source)
	}
}

// fetchMedia baixa a mídia para <dataDir>/media/<id><ext> e preenche Path, ou
// Error quando não dá. Bloqueia, então é chamada fora do handler de eventos.
func (c *core) fetchMedia(messageID string, part *mediaPart, att *InboundAttachment) {
	if part.size > maxInboundMediaBytes {
		att.Error = fmt.Sprintf("attachment exceeds %d MB", maxInboundMediaBytes>>20)
		return
	}
	download := c.download
	if download == nil {
		download = c.clientDownloader()
	}
	ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancel()
	data, err := download(ctx, part.source)
	if err != nil {
		att.Error = "download failed: " + err.Error()
		return
	}
	if len(data) > maxInboundMediaBytes {
		att.Error = fmt.Sprintf("attachment exceeds %d MB", maxInboundMediaBytes>>20)
		return
	}
	path := filepath.Join(c.mediaDir, sanitizeID(messageID)+extensionFor(part.mimeType))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		att.Error = "store failed: " + err.Error()
		return
	}
	att.Path = path
	att.SizeBytes = int64(len(data))
}

// MediaDir é a pasta onde as mídias recebidas ficam até o host enviá-las e
// onde o host pode guardar as de saída. Vazia antes de Initialize.
func MediaDir() string {
	singleton.mu.Lock()
	defer singleton.mu.Unlock()
	return singleton.mediaDir
}

// SendMedia envia um arquivo como imagem, vídeo, áudio ou documento. caption
// vira a legenda (áudio não tem legenda no WhatsApp). ptt marca o áudio como
// mensagem de voz, o que só soa certo com OGG/Opus. messageID segue a mesma
// regra de SendTextWithID.
func SendMedia(destination, kind, path, mimeType, filename, caption string, ptt bool, messageID string) (string, error) {
	return singleton.sendMedia(destination, kind, path, mimeType, filename, caption, ptt, messageID)
}

func (c *core) sendMedia(destination, kind, path, mimeType, filename, caption string, ptt bool, messageID string) (string, error) {
	recipient, err := normalizeDestination(destination)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read media: %w", err)
	}
	if len(data) == 0 {
		return "", errors.New("media file is empty")
	}
	if mimeType == "" {
		mimeType = mime.TypeByExtension(filepath.Ext(path))
	}
	if filename == "" {
		filename = filepath.Base(path)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ensureReadyLocked(); err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancel()
	message, err := c.buildMediaMessageLocked(ctx, kind, data, mimeType, filename, caption, ptt)
	if err != nil {
		return "", err
	}
	return c.sendMessageLocked(ctx, recipient, message, messageID)
}

// buildMediaMessageLocked sobe os bytes para o WhatsApp e monta o proto do
// tipo pedido com as chaves que o Upload devolve.
func (c *core) buildMediaMessageLocked(ctx context.Context, kind string, data []byte, mimeType, filename, caption string, ptt bool) (*waProto.Message, error) {
	var mediaType whatsmeow.MediaType
	switch kind {
	case "image":
		mediaType = whatsmeow.MediaImage
	case "video":
		mediaType = whatsmeow.MediaVideo
	case "audio":
		mediaType = whatsmeow.MediaAudio
	case "document":
		mediaType = whatsmeow.MediaDocument
	default:
		return nil, fmt.Errorf("unsupported media kind %q", kind)
	}
	up, err := c.client.Upload(ctx, data, mediaType)
	if err != nil {
		return nil, fmt.Errorf("upload media: %w", err)
	}
	length := uint64(len(data))
	switch kind {
	case "image":
		return &waProto.Message{ImageMessage: &waProto.ImageMessage{
			Caption: optional(caption), Mimetype: proto.String(mimeType),
			URL: proto.String(up.URL), DirectPath: proto.String(up.DirectPath), MediaKey: up.MediaKey,
			FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(length),
		}}, nil
	case "video":
		return &waProto.Message{VideoMessage: &waProto.VideoMessage{
			Caption: optional(caption), Mimetype: proto.String(mimeType),
			URL: proto.String(up.URL), DirectPath: proto.String(up.DirectPath), MediaKey: up.MediaKey,
			FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(length),
		}}, nil
	case "audio":
		if ptt {
			mimeType = "audio/ogg; codecs=opus"
		}
		return &waProto.Message{AudioMessage: &waProto.AudioMessage{
			Mimetype: proto.String(mimeType), PTT: proto.Bool(ptt),
			URL: proto.String(up.URL), DirectPath: proto.String(up.DirectPath), MediaKey: up.MediaKey,
			FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(length),
		}}, nil
	default:
		return &waProto.Message{DocumentMessage: &waProto.DocumentMessage{
			Caption: optional(caption), Mimetype: proto.String(mimeType), FileName: proto.String(filename), Title: proto.String(filename),
			URL: proto.String(up.URL), DirectPath: proto.String(up.DirectPath), MediaKey: up.MediaKey,
			FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(length),
		}}, nil
	}
}

// PurgeMedia apaga arquivos de mídia esquecidos (mais velhos que a retenção),
// o que acontece quando o bridge cai entre baixar e enviar.
func PurgeMedia() {
	singleton.mu.Lock()
	dir := singleton.mediaDir
	singleton.mu.Unlock()
	if dir == "" {
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-mediaRetention)
	for _, e := range entries {
		info, err := e.Info()
		if err != nil || e.IsDir() || info.ModTime().After(cutoff) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err == nil {
			log.Printf("wacore: mídia esquecida apagada: %s", e.Name())
		}
	}
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return proto.String(s)
}

// extensionFor devolve a extensão para um MIME, com os tipos do WhatsApp que
// o registro do sistema costuma não ter.
func extensionFor(mimeType string) string {
	base := strings.ToLower(strings.TrimSpace(strings.Split(mimeType, ";")[0]))
	switch base {
	case "image/jpeg":
		return ".jpg"
	case "image/webp":
		return ".webp"
	case "audio/ogg":
		return ".ogg"
	case "audio/mp4", "audio/x-m4a":
		return ".m4a"
	case "video/mp4":
		return ".mp4"
	case "application/pdf":
		return ".pdf"
	}
	if exts, _ := mime.ExtensionsByType(base); len(exts) > 0 {
		return exts[0]
	}
	return ".bin"
}

// sanitizeID deixa só o que é seguro num nome de arquivo.
func sanitizeID(id string) string {
	var b strings.Builder
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	if b.Len() == 0 {
		return fmt.Sprintf("m%d", time.Now().UnixNano())
	}
	return b.String()
}
