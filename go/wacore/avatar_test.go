package wacore

import (
	"errors"
	"fmt"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

func TestAvatarFrom(t *testing.T) {
	hidden := fmt.Errorf("%w: %w", whatsmeow.ErrProfilePictureUnauthorized, whatsmeow.ErrIQNotAuthorized)
	notSet := fmt.Errorf("%w: %w", whatsmeow.ErrProfilePictureNotSet, whatsmeow.ErrIQNotFound)
	boom := errors.New("iq timed out")

	cases := []struct {
		name       string
		info       *types.ProfilePictureInfo
		existingID string
		err        error
		want       Avatar
		wantErr    error
	}{
		{name: "oculta", err: hidden, want: Avatar{Reason: AvatarReasonHidden}},
		{name: "sem foto", err: notSet, want: Avatar{Reason: AvatarReasonNotSet}},
		{name: "sem foto (204)", err: whatsmeow.ErrProfilePictureNotSet, want: Avatar{Reason: AvatarReasonNotSet}},
		{name: "falha", err: boom, wantErr: boom},
		{name: "não mudou", existingID: "p1", want: Avatar{PictureID: "p1", Unchanged: true}},
		{name: "nil sem existingID", want: Avatar{Reason: AvatarReasonNotSet}},
		{name: "sem URL", info: &types.ProfilePictureInfo{ID: "p2"}, want: Avatar{Reason: AvatarReasonNotSet}},
		{name: "foto", info: &types.ProfilePictureInfo{ID: "p2", URL: "https://pps.whatsapp.net/x"}, existingID: "p1",
			want: Avatar{PictureID: "p2", URL: "https://pps.whatsapp.net/x"}},
	}
	for _, tc := range cases {
		got, err := avatarFrom(tc.info, tc.existingID, tc.err)
		if tc.wantErr != nil {
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: err = %v, want %v", tc.name, err, tc.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected err %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: got %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

func TestGetAvatarSemSessao(t *testing.T) {
	c := &core{}
	got, err := c.getAvatar("5544999990000", "")
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if got.Reason != AvatarReasonNotConnected {
		t.Fatalf("got %+v, want not_connected", got)
	}
	if _, err := c.getAvatar("120363000@g.us", ""); err == nil {
		t.Fatal("grupo deveria ser recusado")
	}
}
