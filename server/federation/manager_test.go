package federation

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mk6i/open-oscar-server/config"
	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

// newTestManager creates a Manager with a single connected peer for testing.
func newTestManager(t *testing.T, peerNetwork string) (*Manager, *PeerConnection) {
	t.Helper()
	peerCfg := config.FederationPeerConfig{
		NetworkName: peerNetwork,
		Address:     "127.0.0.1:5195",
		Secret:      "testsecret",
	}
	mgr := NewManager(
		"localnet",
		[]config.FederationPeerConfig{peerCfg},
		&mockMessageRelayer{},
		&mockSessionRetriever{sessions: map[state.IdentScreenName]*state.Session{}},
		&mockAllSessionRetriever{},
		&mockRelationshipFetcher{},
		&mockFeedbagManager{},
		&mockProfileManager{},
		NewRemoteSessionStore(slog.Default()),
		slog.Default(),
	)
	// Mark the peer as connected so sends don't fail.
	pc := mgr.peers[peerNetwork]
	pc.mu.Lock()
	pc.connected = true
	pc.mu.Unlock()
	return mgr, pc
}

func TestRouteToRemote_ICBMChannelMsgToClient(t *testing.T) {
	mgr, pc := newTestManager(t, "chivanet")

	recipient := state.NewIdentScreenName("bob@chivanet")
	msg := wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.ICBM,
			SubGroup:  wire.ICBMChannelMsgToClient,
		},
		Body: wire.SNAC_0x04_0x07_ICBMChannelMsgToClient{
			Cookie:    12345,
			ChannelID: wire.ICBMChannelIM,
			TLVUserInfo: wire.TLVUserInfo{
				ScreenName: "Alice",
			},
			TLVRestBlock: wire.TLVRestBlock{
				TLVList: wire.TLVList{
					wire.NewTLVBE(wire.ICBMTLVAOLIMData, []byte("hello")),
					wire.NewTLVBE(wire.ICBMTLVRequestHostAck, []byte{}), // should be stripped
				},
			},
		},
	}

	err := mgr.RouteToRemote(context.Background(), recipient, msg)
	require.NoError(t, err)

	// Read the message from the peer's send channel.
	fedMsg := <-pc.sendCh
	assert.Equal(t, wire.Federation, fedMsg.Frame.FoodGroup)
	assert.Equal(t, wire.FedMessage, fedMsg.Frame.SubGroup)

	body, ok := fedMsg.Body.(wire.SNAC_0x0100_0x0004_FedMessage)
	require.True(t, ok)
	assert.Equal(t, uint64(12345), body.Cookie)
	assert.Equal(t, "Alice", body.FromUser)
	assert.Equal(t, "bob", body.ToUser)
	assert.Equal(t, wire.ICBMChannelIM, body.ChannelID)

	// ICBMTLVRequestHostAck should have been stripped.
	_, hasAckTLV := body.TLVRestBlock.Bytes(wire.ICBMTLVRequestHostAck)
	assert.False(t, hasAckTLV, "ICBMTLVRequestHostAck should be stripped")

	// The actual message TLV should be present.
	_, hasData := body.TLVRestBlock.Bytes(wire.ICBMTLVAOLIMData)
	assert.True(t, hasData, "message data TLV should be forwarded")
}

func TestRouteToRemote_ICBMClientEvent(t *testing.T) {
	mgr, pc := newTestManager(t, "chivanet")

	recipient := state.NewIdentScreenName("bob@chivanet")
	msg := wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.ICBM,
			SubGroup:  wire.ICBMClientEvent,
		},
		Body: wire.SNAC_0x04_0x14_ICBMClientEvent{
			Cookie:     99,
			ChannelID:  wire.ICBMChannelIM,
			ScreenName: "Alice",
			Event:      0x0002, // typing
		},
	}

	err := mgr.RouteToRemote(context.Background(), recipient, msg)
	require.NoError(t, err)

	fedMsg := <-pc.sendCh
	assert.Equal(t, wire.Federation, fedMsg.Frame.FoodGroup)
	assert.Equal(t, wire.FedTypingEvent, fedMsg.Frame.SubGroup)

	body, ok := fedMsg.Body.(wire.SNAC_0x0100_0x000A_FedTypingEvent)
	require.True(t, ok)
	assert.Equal(t, uint64(99), body.Cookie)
	assert.Equal(t, "Alice", body.FromUser)
	assert.Equal(t, "bob", body.ToUser)
	assert.Equal(t, uint16(0x0002), body.Event)
}

func TestRouteToRemote_BuddyArrived(t *testing.T) {
	mgr, pc := newTestManager(t, "chivanet")

	recipient := state.NewIdentScreenName("bob@chivanet")
	msg := wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Buddy,
			SubGroup:  wire.BuddyArrived,
		},
	}

	err := mgr.RouteToRemote(context.Background(), recipient, msg)
	require.NoError(t, err)

	fedMsg := <-pc.sendCh
	assert.Equal(t, wire.Federation, fedMsg.Frame.FoodGroup)
	assert.Equal(t, wire.FedPresenceNotify, fedMsg.Frame.SubGroup)

	body, ok := fedMsg.Body.(wire.SNAC_0x0100_0x0009_FedPresenceNotify)
	require.True(t, ok)
	assert.Equal(t, uint8(1), body.Online)
	assert.Equal(t, "bob", body.ScreenName)
}

func TestRouteToRemote_UnknownNetwork(t *testing.T) {
	mgr, _ := newTestManager(t, "chivanet")

	recipient := state.NewIdentScreenName("bob@unknown")
	msg := wire.SNACMessage{
		Frame: wire.SNACFrame{FoodGroup: wire.ICBM, SubGroup: wire.ICBMChannelMsgToClient},
		Body:  wire.SNAC_0x04_0x07_ICBMChannelMsgToClient{},
	}

	err := mgr.RouteToRemote(context.Background(), recipient, msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown")
}

func TestRouteToRemote_UnhandledSNAC(t *testing.T) {
	mgr, _ := newTestManager(t, "chivanet")

	recipient := state.NewIdentScreenName("bob@chivanet")
	msg := wire.SNACMessage{
		Frame: wire.SNACFrame{FoodGroup: wire.Locate, SubGroup: wire.LocateUserInfoQuery},
	}

	// Should not error — just logs a warning and drops.
	err := mgr.RouteToRemote(context.Background(), recipient, msg)
	assert.NoError(t, err)
}

// Additional mocks needed by Manager constructor.

type mockAllSessionRetriever struct{}

func (m *mockAllSessionRetriever) AllSessions() []*state.Session { return nil }

type mockRelationshipFetcher struct{}

func (m *mockRelationshipFetcher) Relationship(_ context.Context, _, _ state.IdentScreenName) (state.Relationship, error) {
	return state.Relationship{}, nil
}

func (m *mockRelationshipFetcher) AllRelationships(_ context.Context, _ state.IdentScreenName, _ []state.IdentScreenName) ([]state.Relationship, error) {
	return nil, nil
}

type mockProfileManager struct{}

func (m *mockProfileManager) FindByAIMEmail(_ context.Context, _ string) (state.User, error) {
	return state.User{}, nil
}
func (m *mockProfileManager) FindByAIMKeyword(_ context.Context, _ string) ([]state.User, error) {
	return nil, nil
}
func (m *mockProfileManager) FindByAIMNameAndAddr(_ context.Context, _ state.AIMNameAndAddr) ([]state.User, error) {
	return nil, nil
}
func (m *mockProfileManager) InterestList(_ context.Context) ([]wire.ODirKeywordListItem, error) {
	return nil, nil
}
func (m *mockProfileManager) Profile(_ context.Context, _ state.IdentScreenName) (state.UserProfile, error) {
	return state.UserProfile{}, nil
}
func (m *mockProfileManager) SetDirectoryInfo(_ context.Context, _ state.IdentScreenName, _ state.AIMNameAndAddr) error {
	return nil
}
func (m *mockProfileManager) SetKeywords(_ context.Context, _ state.IdentScreenName, _ [5]string) error {
	return nil
}
func (m *mockProfileManager) SetProfile(_ context.Context, _ state.IdentScreenName, _ state.UserProfile) error {
	return nil
}
func (m *mockProfileManager) User(_ context.Context, _ state.IdentScreenName) (*state.User, error) {
	return nil, nil
}
