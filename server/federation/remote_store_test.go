package federation

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

func TestRemoteSessionStore_Get_NotFound(t *testing.T) {
	store := NewRemoteSessionStore(slog.Default())
	sn := state.NewIdentScreenName("cooluser@chivanet")

	sess := store.Get(sn)
	assert.Nil(t, sess)
}

func TestRemoteSessionStore_PresenceArrived_NewUser(t *testing.T) {
	store := NewRemoteSessionStore(slog.Default())
	sn := state.NewIdentScreenName("cooluser@chivanet")

	tlvBlock := wire.TLVRestBlock{}
	tlvBlock.Append(wire.NewTLVBE(wire.OServiceUserInfoUserFlags, uint16(wire.OServiceUserFlagOSCARFree)))
	tlvBlock.Append(wire.NewTLVBE(wire.OServiceUserInfoStatus, uint32(wire.OServiceUserStatusAvailable)))

	sess := store.PresenceArrived(sn, tlvBlock)

	assert.NotNil(t, sess)
	assert.Equal(t, sn, sess.IdentScreenName())
	assert.Equal(t, state.DisplayScreenName("cooluser@chivanet"), sess.DisplayScreenName())
	assert.True(t, sess.HasLiveInstances())

	// Verify retrievable via Get
	got := store.Get(sn)
	assert.Equal(t, sess, got)
}

func TestRemoteSessionStore_PresenceArrived_UpdateExisting(t *testing.T) {
	store := NewRemoteSessionStore(slog.Default())
	sn := state.NewIdentScreenName("cooluser@chivanet")

	tlvBlock1 := wire.TLVRestBlock{}
	tlvBlock1.Append(wire.NewTLVBE(wire.OServiceUserInfoUserFlags, uint16(wire.OServiceUserFlagOSCARFree)))

	sess1 := store.PresenceArrived(sn, tlvBlock1)

	// Second arrival should return the same session object, not create a new one
	tlvBlock2 := wire.TLVRestBlock{}
	tlvBlock2.Append(wire.NewTLVBE(wire.OServiceUserInfoUserFlags, uint16(wire.OServiceUserFlagOSCARFree|wire.OServiceUserFlagUnavailable)))

	sess2 := store.PresenceArrived(sn, tlvBlock2)

	assert.Equal(t, sess1, sess2, "should return the same session object on update")
}

func TestRemoteSessionStore_PresenceDeparted(t *testing.T) {
	store := NewRemoteSessionStore(slog.Default())
	sn := state.NewIdentScreenName("cooluser@chivanet")

	tlvBlock := wire.TLVRestBlock{}
	store.PresenceArrived(sn, tlvBlock)

	removed := store.PresenceDeparted(sn)
	assert.True(t, removed)

	sess := store.Get(sn)
	assert.Nil(t, sess)
}

func TestRemoteSessionStore_PresenceDeparted_NotFound(t *testing.T) {
	store := NewRemoteSessionStore(slog.Default())
	sn := state.NewIdentScreenName("nobody@chivanet")

	removed := store.PresenceDeparted(sn)
	assert.False(t, removed)
}

func TestRemoteSessionStore_PresenceArrived_WithCapabilities(t *testing.T) {
	store := NewRemoteSessionStore(slog.Default())
	sn := state.NewIdentScreenName("cooluser@chivanet")

	cap1 := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	cap2 := [16]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20}
	capBytes := append(cap1[:], cap2[:]...)

	tlvBlock := wire.TLVRestBlock{}
	tlvBlock.Append(wire.NewTLVBE(wire.OServiceUserInfoOscarCaps, capBytes))

	sess := store.PresenceArrived(sn, tlvBlock)

	assert.True(t, sess.HasCap(cap1))
	assert.True(t, sess.HasCap(cap2))
	assert.False(t, sess.HasCap([16]byte{0xFF}))
}

func TestRemoteSessionStore_TLVUserInfo(t *testing.T) {
	store := NewRemoteSessionStore(slog.Default())
	sn := state.NewIdentScreenName("cooluser@chivanet")

	tlvBlock := wire.TLVRestBlock{}
	tlvBlock.Append(wire.NewTLVBE(wire.OServiceUserInfoUserFlags, uint16(wire.OServiceUserFlagOSCARFree)))

	sess := store.PresenceArrived(sn, tlvBlock)

	userInfo := sess.TLVUserInfo()
	assert.Equal(t, "cooluser@chivanet", userInfo.ScreenName)
	assert.Equal(t, uint16(0), userInfo.WarningLevel)
}

func TestRemoteSessionStore_Subscribers(t *testing.T) {
	store := NewRemoteSessionStore(slog.Default())

	sn1 := state.NewIdentScreenName("user1@chivanet")
	sn2 := state.NewIdentScreenName("user2@retra")

	store.PresenceArrived(sn1, wire.TLVRestBlock{})
	store.PresenceArrived(sn2, wire.TLVRestBlock{})

	subs := store.Subscribers()
	assert.Len(t, subs, 2)
	assert.Contains(t, subs, sn1)
	assert.Contains(t, subs, sn2)
}
