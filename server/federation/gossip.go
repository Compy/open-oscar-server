package federation

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/mk6i/open-oscar-server/wire"
)

const (
	defaultGossipInterval = 2 * time.Second
	defaultMaxTTL         = 10

	// suspectTimeout is how long after last update before a network is
	// marked as suspected dead (if the direct connection is also down).
	suspectTimeout = 10 * time.Second

	// deadTimeout is how long after last update before a network is
	// removed from the routing table entirely.
	deadTimeout = 30 * time.Second
)

// NetworkInfo represents a known server in the federation network.
type NetworkInfo struct {
	NetworkName string
	Version     uint64
	IsAlive     bool
	DirectPeers []string // which networks this server reports peering with
	UserCount   uint32
	LastUpdated time.Time // local wall clock when last updated
}

// GossipState manages the network-wide topology view and routing table.
// It is safe for concurrent access.
type GossipState struct {
	mu         sync.RWMutex
	networks   map[string]*NetworkInfo // all known networks
	routeTable map[string]string       // destination network -> next-hop peer network
	localNet   string
	version    uint64 // our own version counter (incremented on local state changes)
	logger     *slog.Logger

	// directPeers tracks which networks we have direct connections to.
	// This is used to build our own adjacency entry in the gossip state.
	directPeers map[string]bool
}

// NewGossipState creates a new GossipState for the given local network name.
func NewGossipState(localNet string, logger *slog.Logger) *GossipState {
	gs := &GossipState{
		networks:    make(map[string]*NetworkInfo),
		routeTable:  make(map[string]string),
		localNet:    localNet,
		version:     1,
		logger:      logger,
		directPeers: make(map[string]bool),
	}
	// Add ourselves as a known network.
	gs.networks[localNet] = &NetworkInfo{
		NetworkName: localNet,
		Version:     1,
		IsAlive:     true,
		LastUpdated: time.Now(),
	}
	return gs
}

// SetDirectPeers updates the set of networks we have direct connections to.
// This triggers a version bump and route recomputation.
func (gs *GossipState) SetDirectPeers(peers []string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	gs.directPeers = make(map[string]bool, len(peers))
	for _, p := range peers {
		gs.directPeers[p] = true
	}

	// Update our own network info with our direct peers.
	peerList := make([]string, 0, len(gs.directPeers))
	for p := range gs.directPeers {
		peerList = append(peerList, p)
	}

	gs.version++
	if info, ok := gs.networks[gs.localNet]; ok {
		info.DirectPeers = peerList
		info.Version = gs.version
		info.LastUpdated = time.Now()
	}

	gs.recomputeRoutesLocked()
}

// SetLocalUserCount updates the approximate online user count for this server.
func (gs *GossipState) SetLocalUserCount(count uint32) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if info, ok := gs.networks[gs.localNet]; ok {
		info.UserCount = count
	}
	gs.version++
	gs.networks[gs.localNet].Version = gs.version
}

// NextHop returns the next-hop network name for reaching the given destination,
// or empty string if no route exists.
func (gs *GossipState) NextHop(dest string) string {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.routeTable[dest]
}

// KnownNetworks returns a list of all known alive networks.
func (gs *GossipState) KnownNetworks() []string {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	names := make([]string, 0, len(gs.networks))
	for name, info := range gs.networks {
		if info.IsAlive {
			names = append(names, name)
		}
	}
	return names
}

// BuildDigest creates a gossip digest containing version vectors for all
// known networks.
func (gs *GossipState) BuildDigest() wire.SNAC_0x0100_0x0011_FedGossipDigest {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	digest := wire.SNAC_0x0100_0x0011_FedGossipDigest{
		Generation: gs.version,
	}

	for _, info := range gs.networks {
		alive := uint8(0)
		if info.IsAlive {
			alive = 1
		}
		digest.Digests = append(digest.Digests, wire.FedNetworkDigest{
			NetworkName: info.NetworkName,
			MaxVersion:  info.Version,
			IsAlive:     alive,
		})
	}

	return digest
}

// HandleDigest processes an incoming gossip digest and returns:
// - updates: full state for networks where the sender is behind us
// - needFrom: digests for networks where we are behind the sender
func (gs *GossipState) HandleDigest(digest wire.SNAC_0x0100_0x0011_FedGossipDigest) (
	updates []wire.FedNetworkState,
	needFrom []wire.FedNetworkDigest,
) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	for _, d := range digest.Digests {
		localInfo, known := gs.networks[d.NetworkName]

		if !known {
			// We don't know about this network — request it.
			needFrom = append(needFrom, d)
			continue
		}

		if d.MaxVersion > localInfo.Version {
			// Sender has newer version — request it.
			needFrom = append(needFrom, d)
		} else if d.MaxVersion < localInfo.Version {
			// We have newer version — send it.
			updates = append(updates, gs.networkInfoToState(localInfo))
		}
		// Equal versions — nothing to exchange.
	}

	// Also send state for networks we know about that the sender doesn't.
	senderNetworks := make(map[string]bool, len(digest.Digests))
	for _, d := range digest.Digests {
		senderNetworks[d.NetworkName] = true
	}
	for name, info := range gs.networks {
		if !senderNetworks[name] && info.IsAlive {
			updates = append(updates, gs.networkInfoToState(info))
		}
	}

	return updates, needFrom
}

// MergeUpdates merges a set of network state updates into local state.
// Returns true if the topology graph changed (requiring route recomputation).
func (gs *GossipState) MergeUpdates(states []wire.FedNetworkState) bool {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	changed := false
	for _, s := range states {
		changed = gs.mergeStateLocked(s) || changed
	}

	if changed {
		gs.recomputeRoutesLocked()
	}
	return changed
}

// GetStatesForDigests returns full state for the requested network digests.
func (gs *GossipState) GetStatesForDigests(digests []wire.FedNetworkDigest) []wire.FedNetworkState {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	var states []wire.FedNetworkState
	for _, d := range digests {
		if info, ok := gs.networks[d.NetworkName]; ok {
			states = append(states, gs.networkInfoToState(info))
		}
	}
	return states
}

// SweepDead checks for networks that have not been updated recently and marks
// them as dead if appropriate. Returns true if any networks were removed.
func (gs *GossipState) SweepDead(isDirectPeerConnected func(network string) bool) bool {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	now := time.Now()
	changed := false

	for name, info := range gs.networks {
		if name == gs.localNet {
			continue // never mark ourselves dead
		}
		if !info.IsAlive {
			// Already dead — check if we should remove entirely.
			if now.Sub(info.LastUpdated) > deadTimeout {
				delete(gs.networks, name)
				changed = true
			}
			continue
		}

		age := now.Sub(info.LastUpdated)
		if age > suspectTimeout {
			// Only suspect dead if we don't have a direct connection to them.
			if !isDirectPeerConnected(name) {
				info.IsAlive = false
				changed = true
				gs.logger.Info("marking network as suspected dead",
					"network", name,
					"last_updated", info.LastUpdated,
				)
			}
		}
	}

	if changed {
		gs.recomputeRoutesLocked()
	}
	return changed
}

// mergeStateLocked merges a single network state update. Must be called with
// gs.mu held for writing. Returns true if the topology changed.
func (gs *GossipState) mergeStateLocked(s wire.FedNetworkState) bool {
	existing, known := gs.networks[s.NetworkName]

	if known && existing.Version >= s.Version {
		return false // we already have equal or newer data
	}

	peers := make([]string, len(s.PeerList))
	for i, p := range s.PeerList {
		peers[i] = p.NetworkName
	}

	alive := s.IsAlive == 1

	if !known {
		gs.networks[s.NetworkName] = &NetworkInfo{
			NetworkName: s.NetworkName,
			Version:     s.Version,
			IsAlive:     alive,
			DirectPeers: peers,
			UserCount:   s.UserCount,
			LastUpdated: time.Now(),
		}
		return true
	}

	// Check if topology actually changed.
	topologyChanged := !stringSlicesEqual(existing.DirectPeers, peers) ||
		existing.IsAlive != alive

	existing.Version = s.Version
	existing.IsAlive = alive
	existing.DirectPeers = peers
	existing.UserCount = s.UserCount
	existing.LastUpdated = time.Now()

	return topologyChanged
}

func (gs *GossipState) networkInfoToState(info *NetworkInfo) wire.FedNetworkState {
	alive := uint8(0)
	if info.IsAlive {
		alive = 1
	}
	peerList := make([]wire.FedNetworkPeer, len(info.DirectPeers))
	for i, p := range info.DirectPeers {
		peerList[i] = wire.FedNetworkPeer{NetworkName: p}
	}
	return wire.FedNetworkState{
		NetworkName: info.NetworkName,
		Version:     info.Version,
		IsAlive:     alive,
		PeerList:    peerList,
		UserCount:   info.UserCount,
	}
}

// recomputeRoutesLocked builds a shortest-path routing table using BFS
// on the topology graph. Must be called with gs.mu held for writing.
func (gs *GossipState) recomputeRoutesLocked() {
	gs.routeTable = make(map[string]string)

	// Build adjacency list from all alive networks.
	adj := make(map[string][]string)
	for _, info := range gs.networks {
		if !info.IsAlive {
			continue
		}
		for _, peer := range info.DirectPeers {
			if peerInfo, ok := gs.networks[peer]; ok && peerInfo.IsAlive {
				adj[info.NetworkName] = append(adj[info.NetworkName], peer)
			}
		}
	}

	// BFS from localNet.
	visited := map[string]bool{gs.localNet: true}
	// firstHop tracks the first hop on the path from localNet to each network.
	firstHop := make(map[string]string)

	type bfsEntry struct {
		network string
		hop     string // first hop from localNet
	}

	queue := make([]bfsEntry, 0)

	// Seed with our direct neighbors.
	for _, neighbor := range adj[gs.localNet] {
		if !visited[neighbor] {
			visited[neighbor] = true
			firstHop[neighbor] = neighbor
			queue = append(queue, bfsEntry{network: neighbor, hop: neighbor})
		}
	}

	// Process BFS.
	for len(queue) > 0 {
		entry := queue[0]
		queue = queue[1:]

		gs.routeTable[entry.network] = entry.hop

		for _, next := range adj[entry.network] {
			if !visited[next] {
				visited[next] = true
				firstHop[next] = entry.hop
				queue = append(queue, bfsEntry{network: next, hop: entry.hop})
			}
		}
	}
}

// GossipLoop runs the periodic gossip protocol. It sends digests to random
// peers and handles failure detection. It blocks until the context is canceled.
func (gs *GossipState) GossipLoop(ctx context.Context, interval time.Duration, sendDigest func(peerNetwork string, digest wire.SNAC_0x0100_0x0011_FedGossipDigest), connectedPeers func() []string, isDirectPeerConnected func(string) bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sweepTicker := time.NewTicker(suspectTimeout)
	defer sweepTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peers := connectedPeers()
			if len(peers) == 0 {
				continue
			}

			digest := gs.BuildDigest()

			// Fan out to 1-3 random peers (scales with log(N)).
			fanout := 1
			if len(peers) > 3 {
				fanout = 2
			}
			if len(peers) > 10 {
				fanout = 3
			}

			// Shuffle and pick up to fanout peers.
			rand.Shuffle(len(peers), func(i, j int) {
				peers[i], peers[j] = peers[j], peers[i]
			})
			if fanout > len(peers) {
				fanout = len(peers)
			}

			for _, peer := range peers[:fanout] {
				sendDigest(peer, digest)
			}

		case <-sweepTicker.C:
			gs.SweepDead(isDirectPeerConnected)
		}
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int, len(a))
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		m[s]--
		if m[s] < 0 {
			return false
		}
	}
	return true
}
