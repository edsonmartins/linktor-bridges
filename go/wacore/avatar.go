package wacore

import (
	"context"
	"errors"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// avatarTimeout limita a consulta da foto: é uma IQ só, e o Linktor desiste da
// resposta em 15s.
const avatarTimeout = 10 * time.Second

// Motivos de uma consulta de foto que não trouxe imagem. NotSet e Hidden vêm
// separados porque quem guarda a foto precisa saber se o titular escolheu
// esconder; NotConnected diz que a sessão do aparelho não está de pé.
const (
	AvatarReasonNotSet       = "not_set"
	AvatarReasonHidden       = "hidden"
	AvatarReasonNotConnected = "not_connected"
)

// Avatar é a resposta de GetAvatar. Com Reason vazio, há foto: Unchanged diz
// que o existingID pedido continua valendo (sem URL); senão URL aponta para a
// imagem no CDN do WhatsApp, que quem pediu baixa.
type Avatar struct {
	PictureID string `json:"picture_id,omitempty"`
	URL       string `json:"url,omitempty"`
	Unchanged bool   `json:"unchanged,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// GetAvatar consulta a foto de perfil de um contato (telefone com DDI, JID de
// usuário ou @lid), passando existingID para o WhatsApp responder "não mudou".
// Devolve o Avatar em JSON (API compatível com gomobile). "Sem foto", "oculta"
// e "sem sessão" vêm em Reason, não como erro; erro é falha de verdade.
func GetAvatar(target, existingID string) (string, error) {
	avatar, err := singleton.getAvatar(target, existingID)
	if err != nil {
		return "", err
	}
	return marshal(avatar)
}

// LookupAvatar é a mesma consulta de GetAvatar, devolvendo a struct para hosts
// em Go (o desktop).
func LookupAvatar(target, existingID string) (Avatar, error) {
	return singleton.getAvatar(target, existingID)
}

func (c *core) getAvatar(target, existingID string) (Avatar, error) {
	jid, err := normalizeDestination(target)
	if err != nil {
		return Avatar{}, err
	}

	// Sem esperar pela conexão (ao contrário do envio): quem pergunta tem
	// prazo curto, e sessão caída é resposta, não motivo para segurar o lock.
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil || !client.IsConnected() || !client.IsLoggedIn() {
		return Avatar{Reason: AvatarReasonNotConnected}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), avatarTimeout)
	defer cancel()
	info, err := client.GetProfilePictureInfo(ctx, jid.ToNonAD(), &whatsmeow.GetProfilePictureParams{
		ExistingID: existingID,
		Preview:    false,
	})
	return avatarFrom(info, existingID, err)
}

// avatarFrom traduz a resposta do whatsmeow. Separado para o teste cobrir os
// dois erros sem sessão de verdade.
func avatarFrom(info *types.ProfilePictureInfo, existingID string, err error) (Avatar, error) {
	switch {
	case errors.Is(err, whatsmeow.ErrProfilePictureUnauthorized):
		return Avatar{Reason: AvatarReasonHidden}, nil
	case errors.Is(err, whatsmeow.ErrProfilePictureNotSet):
		return Avatar{Reason: AvatarReasonNotSet}, nil
	case err != nil:
		return Avatar{}, err
	}
	if info == nil {
		// Com existingID, nil é o "304" do WhatsApp: a foto não mudou.
		if existingID != "" {
			return Avatar{PictureID: existingID, Unchanged: true}, nil
		}
		return Avatar{Reason: AvatarReasonNotSet}, nil
	}
	if info.URL == "" {
		return Avatar{Reason: AvatarReasonNotSet}, nil
	}
	return Avatar{PictureID: info.ID, URL: info.URL}, nil
}
