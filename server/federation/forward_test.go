package federation

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mk6i/open-oscar-server/config"
	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

func TestSerializeDeserializeInnerSNAC(t *testing.T) {
	original := wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedMessage,
		},
		Body: wire.SNAC_0x0100_0x0004_FedMessage{
			Cookie:    12345,
			FromUser:  "alice",
			ToUser:    "bob",
			ChannelID: wire.ICBMChannelIM,
		},
	}

	data, err := serializeInnerSNAC(original)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	frame, reader, err := deserializeInnerSNAC(data)
	require.NoError(t, err)
	assert.Equal(t, wire.Federation, frame.FoodGroup)
	assert.Equal(t, wire.FedMessage, frame.SubGroup)

	var body wire.SNAC_0x0100_0x0004_FedMessage
	err = wire.UnmarshalBE(&body, reader)
	require.NoError(t, err)
	assert.Equal(t, uint64(12345), body.Cookie)
	assert.Equal(t, "alice", body.FromUser)
	assert.Equal(t, "bob", body.ToUser)
}

func TestFedForwardRoundTrip(t *testing.T) {
	// Test that FedForward can be marshaled and unmarshaled.
	fwd := wire.SNAC_0x0100_0x0014_FedForward{
		OriginNetwork: "server-a",
		TargetNetwork: "server-c",
		TTL:           8,
		InnerSNAC:     []byte{0x01, 0x02, 0x03, 0x04},
	}

	buf := &bytes.Buffer{}
	err := wire.MarshalBE(fwd, buf)
	require.NoError(t, err)

	var decoded wire.SNAC_0x0100_0x0014_FedForward
	err = wire.UnmarshalBE(&decoded, buf)
	require.NoError(t, err)

	assert.Equal(t, "server-a", decoded.OriginNetwork)
	assert.Equal(t, "server-c", decoded.TargetNetwork)
	assert.Equal(t, uint8(8), decoded.TTL)
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, decoded.InnerSNAC)
}

func TestGossipDigestRoundTrip(t *testing.T) {
	digest := wire.SNAC_0x0100_0x0011_FedGossipDigest{
		Generation: 42,
		Digests: []wire.FedNetworkDigest{
			{NetworkName: "server-a", MaxVersion: 5, IsAlive: 1},
			{NetworkName: "server-b", MaxVersion: 3, IsAlive: 0},
		},
	}

	buf := &bytes.Buffer{}
	err := wire.MarshalBE(digest, buf)
	require.NoError(t, err)

	var decoded wire.SNAC_0x0100_0x0011_FedGossipDigest
	err = wire.UnmarshalBE(&decoded, buf)
	require.NoError(t, err)

	assert.Equal(t, uint64(42), decoded.Generation)
	require.Len(t, decoded.Digests, 2)
	assert.Equal(t, "server-a", decoded.Digests[0].NetworkName)
	assert.Equal(t, uint64(5), decoded.Digests[0].MaxVersion)
	assert.Equal(t, uint8(1), decoded.Digests[0].IsAlive)
	assert.Equal(t, "server-b", decoded.Digests[1].NetworkName)
}

func TestGossipDigestAckRoundTrip(t *testing.T) {
	ack := wire.SNAC_0x0100_0x0012_FedGossipDigestAck{
		Updates: []wire.FedNetworkState{
			{
				NetworkName: "server-a",
				Version:     10,
				IsAlive:     1,
				PeerList: []wire.FedNetworkPeer{
					{NetworkName: "server-b"},
					{NetworkName: "server-c"},
				},
				UserCount: 100,
			},
		},
		NeedFrom: []wire.FedNetworkDigest{
			{NetworkName: "server-d", MaxVersion: 7, IsAlive: 1},
		},
	}

	buf := &bytes.Buffer{}
	err := wire.MarshalBE(ack, buf)
	require.NoError(t, err)

	var decoded wire.SNAC_0x0100_0x0012_FedGossipDigestAck
	err = wire.UnmarshalBE(&decoded, buf)
	require.NoError(t, err)

	require.Len(t, decoded.Updates, 1)
	assert.Equal(t, "server-a", decoded.Updates[0].NetworkName)
	assert.Equal(t, uint64(10), decoded.Updates[0].Version)
	require.Len(t, decoded.Updates[0].PeerList, 2)
	assert.Equal(t, "server-b", decoded.Updates[0].PeerList[0].NetworkName)
	assert.Equal(t, uint32(100), decoded.Updates[0].UserCount)

	require.Len(t, decoded.NeedFrom, 1)
	assert.Equal(t, "server-d", decoded.NeedFrom[0].NetworkName)
}

// newTestManagerMultiHop creates a Manager with two peers (B and C) where
// B is connected and C is not, simulating a topology where C is reachable
// through B.
func newTestManagerMultiHop(t *testing.T) (*Manager, *PeerConnection) {
	t.Helper()

	peerB := config.FederationPeerConfig{
		NetworkName: "server-b",
		Address:     "127.0.0.1:5195",
		Secret:      "secret-b",
	}

	mgr := NewManager(
		"server-a",
		[]config.FederationPeerConfig{peerB},
		&mockMessageRelayer{},
		&mockSessionRetriever{sessions: map[state.IdentScreenName]*state.Session{}},
		&mockAllSessionRetriever{},
		&mockRelationshipFetcher{},
		&mockFeedbagManager{},
		&mockProfileManager{},
		NewRemoteSessionStore(),
		slog.Default(),
		0, 0,
	)

	// Mark server-b as connected.
	pcB := mgr.peers["server-b"]
	pcB.mu.Lock()
	pcB.connected = true
	pcB.mu.Unlock()

	// Set up gossip so server-a knows about server-c reachable through server-b.
	mgr.gossip.SetDirectPeers([]string{"server-b"})
	mgr.gossip.MergeUpdates([]wire.FedNetworkState{
		{
			NetworkName: "server-b",
			Version:     1,
			IsAlive:     1,
			PeerList: []wire.FedNetworkPeer{
				{NetworkName: "server-a"},
				{NetworkName: "server-c"},
			},
		},
		{
			NetworkName: "server-c",
			Version:     1,
			IsAlive:     1,
			PeerList: []wire.FedNetworkPeer{
				{NetworkName: "server-b"},
			},
		},
	})

	return mgr, pcB
}

func TestRouteToRemote_MultiHopForwarding(t *testing.T) {
	mgr, pcB := newTestManagerMultiHop(t)

	// Send a message to a user on server-c (not directly connected).
	recipient := state.NewIdentScreenName("charlie@server-c")
	msg := wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.ICBM,
			SubGroup:  wire.ICBMChannelMsgToClient,
		},
		Body: wire.SNAC_0x04_0x07_ICBMChannelMsgToClient{
			Cookie:    55555,
			ChannelID: wire.ICBMChannelIM,
			TLVUserInfo: wire.TLVUserInfo{
				ScreenName: "Alice",
			},
			TLVRestBlock: wire.TLVRestBlock{
				TLVList: wire.TLVList{
					wire.NewTLVBE(wire.ICBMTLVAOLIMData, []byte("hello charlie")),
				},
			},
		},
	}

	err := mgr.RouteToRemote(context.Background(), recipient, msg)
	require.NoError(t, err)

	// The message should be sent to server-b as a FedForward.
	fedMsg := <-pcB.sendCh
	assert.Equal(t, wire.Federation, fedMsg.Frame.FoodGroup)
	assert.Equal(t, wire.FedForward, fedMsg.Frame.SubGroup)

	fwd, ok := fedMsg.Body.(wire.SNAC_0x0100_0x0014_FedForward)
	require.True(t, ok)
	assert.Equal(t, "server-a", fwd.OriginNetwork)
	assert.Equal(t, "server-c", fwd.TargetNetwork)
	assert.Equal(t, mgr.maxTTL, fwd.TTL)
	assert.NotEmpty(t, fwd.InnerSNAC)

	// Verify the inner SNAC is a FedMessage.
	frame, reader, err := deserializeInnerSNAC(fwd.InnerSNAC)
	require.NoError(t, err)
	assert.Equal(t, wire.Federation, frame.FoodGroup)
	assert.Equal(t, wire.FedMessage, frame.SubGroup)

	var innerBody wire.SNAC_0x0100_0x0004_FedMessage
	err = wire.UnmarshalBE(&innerBody, reader)
	require.NoError(t, err)
	assert.Equal(t, uint64(55555), innerBody.Cookie)
	assert.Equal(t, "Alice", innerBody.FromUser)
	assert.Equal(t, "charlie", innerBody.ToUser)
}

func TestConnectedPeerOrRoute_DirectPreferred(t *testing.T) {
	mgr, _ := newTestManager(t, "chivanet")

	// Direct peer should be preferred.
	pc, forwarded, err := mgr.connectedPeerOrRoute("chivanet")
	require.NoError(t, err)
	assert.False(t, forwarded)
	assert.Equal(t, "chivanet", pc.config.NetworkName)
}

func TestConnectedPeerOrRoute_FallbackToGossipRoute(t *testing.T) {
	mgr, _ := newTestManagerMultiHop(t)

	// server-c has no direct peer but is reachable through server-b.
	pc, forwarded, err := mgr.connectedPeerOrRoute("server-c")
	require.NoError(t, err)
	assert.True(t, forwarded)
	assert.Equal(t, "server-b", pc.config.NetworkName)
}

func TestConnectedPeerOrRoute_NoRoute(t *testing.T) {
	mgr, _ := newTestManager(t, "chivanet")

	_, _, err := mgr.connectedPeerOrRoute("unreachable")
	assert.Error(t, err)
}
