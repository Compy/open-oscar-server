package federation

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mk6i/open-oscar-server/wire"
)

func TestGossipState_NewGossipState(t *testing.T) {
	gs := NewGossipState("server-a", slog.Default())

	assert.Equal(t, "server-a", gs.localNet)
	assert.Equal(t, uint64(1), gs.version)

	info, ok := gs.networks["server-a"]
	require.True(t, ok)
	assert.True(t, info.IsAlive)
	assert.Equal(t, "server-a", info.NetworkName)
}

func TestGossipState_SetDirectPeers(t *testing.T) {
	gs := NewGossipState("server-a", slog.Default())
	gs.SetDirectPeers([]string{"server-b", "server-c"})

	info := gs.networks["server-a"]
	assert.Contains(t, info.DirectPeers, "server-b")
	assert.Contains(t, info.DirectPeers, "server-c")
	assert.Greater(t, info.Version, uint64(1))
}

func TestGossipState_BuildDigest(t *testing.T) {
	gs := NewGossipState("server-a", slog.Default())
	gs.SetDirectPeers([]string{"server-b"})

	digest := gs.BuildDigest()
	assert.Equal(t, gs.version, digest.Generation)
	require.Len(t, digest.Digests, 1)
	assert.Equal(t, "server-a", digest.Digests[0].NetworkName)
	assert.Equal(t, uint8(1), digest.Digests[0].IsAlive)
}

func TestGossipState_HandleDigest_SenderBehind(t *testing.T) {
	// Local server knows about server-a at version 5.
	gs := NewGossipState("server-a", slog.Default())
	gs.mu.Lock()
	gs.networks["server-a"].Version = 5
	gs.mu.Unlock()

	// Sender thinks server-a is at version 2.
	digest := wire.SNAC_0x0100_0x0011_FedGossipDigest{
		Generation: 1,
		Digests: []wire.FedNetworkDigest{
			{NetworkName: "server-a", MaxVersion: 2, IsAlive: 1},
		},
	}

	updates, needFrom := gs.HandleDigest(digest)

	// We should send our newer state.
	require.Len(t, updates, 1)
	assert.Equal(t, "server-a", updates[0].NetworkName)
	assert.Equal(t, uint64(5), updates[0].Version)

	// We don't need anything from sender.
	assert.Empty(t, needFrom)
}

func TestGossipState_HandleDigest_LocalBehind(t *testing.T) {
	gs := NewGossipState("server-a", slog.Default())

	// Sender knows about server-b which we don't know about.
	digest := wire.SNAC_0x0100_0x0011_FedGossipDigest{
		Generation: 1,
		Digests: []wire.FedNetworkDigest{
			{NetworkName: "server-a", MaxVersion: 1, IsAlive: 1},
			{NetworkName: "server-b", MaxVersion: 3, IsAlive: 1},
		},
	}

	updates, needFrom := gs.HandleDigest(digest)

	// We should request server-b's state.
	require.Len(t, needFrom, 1)
	assert.Equal(t, "server-b", needFrom[0].NetworkName)

	// No updates to send (versions equal for server-a).
	assert.Empty(t, updates)
}

func TestGossipState_MergeUpdates(t *testing.T) {
	gs := NewGossipState("server-a", slog.Default())
	gs.SetDirectPeers([]string{"server-b"})

	// Merge in state for server-b and server-c.
	changed := gs.MergeUpdates([]wire.FedNetworkState{
		{
			NetworkName: "server-b",
			Version:     5,
			IsAlive:     1,
			PeerList: []wire.FedNetworkPeer{
				{NetworkName: "server-a"},
				{NetworkName: "server-c"},
			},
			UserCount: 42,
		},
		{
			NetworkName: "server-c",
			Version:     3,
			IsAlive:     1,
			PeerList: []wire.FedNetworkPeer{
				{NetworkName: "server-b"},
			},
			UserCount: 10,
		},
	})

	assert.True(t, changed)

	// Verify server-b and server-c are now known.
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	bInfo, ok := gs.networks["server-b"]
	require.True(t, ok)
	assert.Equal(t, uint64(5), bInfo.Version)
	assert.True(t, bInfo.IsAlive)
	assert.Equal(t, uint32(42), bInfo.UserCount)

	cInfo, ok := gs.networks["server-c"]
	require.True(t, ok)
	assert.Equal(t, uint64(3), cInfo.Version)
	assert.True(t, cInfo.IsAlive)
}

func TestGossipState_MergeUpdates_OlderVersionIgnored(t *testing.T) {
	gs := NewGossipState("server-a", slog.Default())

	// First merge: version 5.
	gs.MergeUpdates([]wire.FedNetworkState{
		{NetworkName: "server-b", Version: 5, IsAlive: 1},
	})

	// Second merge: version 3 (older, should be ignored).
	changed := gs.MergeUpdates([]wire.FedNetworkState{
		{NetworkName: "server-b", Version: 3, IsAlive: 1},
	})

	assert.False(t, changed)

	gs.mu.RLock()
	assert.Equal(t, uint64(5), gs.networks["server-b"].Version)
	gs.mu.RUnlock()
}

func TestGossipState_Routing_LinearChain(t *testing.T) {
	// Topology: A -- B -- C -- D
	gs := NewGossipState("server-a", slog.Default())
	gs.SetDirectPeers([]string{"server-b"})

	gs.MergeUpdates([]wire.FedNetworkState{
		{
			NetworkName: "server-b",
			Version:     1,
			IsAlive:     1,
			PeerList:    []wire.FedNetworkPeer{{NetworkName: "server-a"}, {NetworkName: "server-c"}},
		},
		{
			NetworkName: "server-c",
			Version:     1,
			IsAlive:     1,
			PeerList:    []wire.FedNetworkPeer{{NetworkName: "server-b"}, {NetworkName: "server-d"}},
		},
		{
			NetworkName: "server-d",
			Version:     1,
			IsAlive:     1,
			PeerList:    []wire.FedNetworkPeer{{NetworkName: "server-c"}},
		},
	})

	// Route to B: direct hop.
	assert.Equal(t, "server-b", gs.NextHop("server-b"))

	// Route to C: through B.
	assert.Equal(t, "server-b", gs.NextHop("server-c"))

	// Route to D: also through B (shortest path: A->B->C->D).
	assert.Equal(t, "server-b", gs.NextHop("server-d"))

	// No route to unknown.
	assert.Equal(t, "", gs.NextHop("server-z"))
}

func TestGossipState_Routing_BranchedTopology(t *testing.T) {
	// Topology:
	//     B
	//    / \
	//   A   D
	//    \ /
	//     C

	gs := NewGossipState("server-a", slog.Default())
	gs.SetDirectPeers([]string{"server-b", "server-c"})

	gs.MergeUpdates([]wire.FedNetworkState{
		{
			NetworkName: "server-b",
			Version:     1,
			IsAlive:     1,
			PeerList:    []wire.FedNetworkPeer{{NetworkName: "server-a"}, {NetworkName: "server-d"}},
		},
		{
			NetworkName: "server-c",
			Version:     1,
			IsAlive:     1,
			PeerList:    []wire.FedNetworkPeer{{NetworkName: "server-a"}, {NetworkName: "server-d"}},
		},
		{
			NetworkName: "server-d",
			Version:     1,
			IsAlive:     1,
			PeerList:    []wire.FedNetworkPeer{{NetworkName: "server-b"}, {NetworkName: "server-c"}},
		},
	})

	// Route to D should go through either B or C (both are 2 hops).
	hop := gs.NextHop("server-d")
	assert.True(t, hop == "server-b" || hop == "server-c",
		"expected hop through server-b or server-c, got %s", hop)
}

func TestGossipState_Routing_DeadNodeRemoved(t *testing.T) {
	// Topology: A -- B -- C
	gs := NewGossipState("server-a", slog.Default())
	gs.SetDirectPeers([]string{"server-b"})

	gs.MergeUpdates([]wire.FedNetworkState{
		{
			NetworkName: "server-b",
			Version:     1,
			IsAlive:     1,
			PeerList:    []wire.FedNetworkPeer{{NetworkName: "server-a"}, {NetworkName: "server-c"}},
		},
		{
			NetworkName: "server-c",
			Version:     1,
			IsAlive:     1,
			PeerList:    []wire.FedNetworkPeer{{NetworkName: "server-b"}},
		},
	})

	assert.Equal(t, "server-b", gs.NextHop("server-c"))

	// Mark server-b as dead.
	gs.MergeUpdates([]wire.FedNetworkState{
		{
			NetworkName: "server-b",
			Version:     2,
			IsAlive:     0,
		},
	})

	// No route to server-c anymore (B is dead).
	assert.Equal(t, "", gs.NextHop("server-c"))
}

func TestGossipState_SweepDead(t *testing.T) {
	gs := NewGossipState("server-a", slog.Default())

	gs.MergeUpdates([]wire.FedNetworkState{
		{
			NetworkName: "server-b",
			Version:     1,
			IsAlive:     1,
		},
	})

	// Artificially age the entry.
	gs.mu.Lock()
	gs.networks["server-b"].LastUpdated = gs.networks["server-b"].LastUpdated.Add(-suspectTimeout - 1)
	gs.mu.Unlock()

	// Sweep with no direct connection — should mark as dead.
	changed := gs.SweepDead(func(network string) bool { return false })
	assert.True(t, changed)

	gs.mu.RLock()
	assert.False(t, gs.networks["server-b"].IsAlive)
	gs.mu.RUnlock()
}

func TestGossipState_SweepDead_DirectPeerNotSuspected(t *testing.T) {
	gs := NewGossipState("server-a", slog.Default())

	gs.MergeUpdates([]wire.FedNetworkState{
		{
			NetworkName: "server-b",
			Version:     1,
			IsAlive:     1,
		},
	})

	// Artificially age the entry.
	gs.mu.Lock()
	gs.networks["server-b"].LastUpdated = gs.networks["server-b"].LastUpdated.Add(-suspectTimeout - 1)
	gs.mu.Unlock()

	// Sweep with direct connection active — should NOT mark as dead.
	changed := gs.SweepDead(func(network string) bool { return network == "server-b" })
	assert.False(t, changed)

	gs.mu.RLock()
	assert.True(t, gs.networks["server-b"].IsAlive)
	gs.mu.RUnlock()
}

func TestGossipState_KnownNetworks(t *testing.T) {
	gs := NewGossipState("server-a", slog.Default())

	gs.MergeUpdates([]wire.FedNetworkState{
		{NetworkName: "server-b", Version: 1, IsAlive: 1},
		{NetworkName: "server-c", Version: 1, IsAlive: 0}, // dead
	})

	known := gs.KnownNetworks()
	assert.Contains(t, known, "server-a")
	assert.Contains(t, known, "server-b")
	assert.NotContains(t, known, "server-c")
}

func TestGossipState_GetStatesForDigests(t *testing.T) {
	gs := NewGossipState("server-a", slog.Default())
	gs.SetDirectPeers([]string{"server-b"})

	digests := []wire.FedNetworkDigest{
		{NetworkName: "server-a", MaxVersion: 0},
		{NetworkName: "server-z", MaxVersion: 0}, // unknown
	}

	states := gs.GetStatesForDigests(digests)
	require.Len(t, states, 1)
	assert.Equal(t, "server-a", states[0].NetworkName)
}

func TestStringSlicesEqual(t *testing.T) {
	assert.True(t, stringSlicesEqual(nil, nil))
	assert.True(t, stringSlicesEqual([]string{}, []string{}))
	assert.True(t, stringSlicesEqual([]string{"a", "b"}, []string{"b", "a"}))
	assert.False(t, stringSlicesEqual([]string{"a"}, []string{"b"}))
	assert.False(t, stringSlicesEqual([]string{"a"}, []string{"a", "b"}))
}
