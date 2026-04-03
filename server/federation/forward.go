package federation

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/mk6i/open-oscar-server/wire"
)

// serializeInnerSNAC marshals a SNACFrame + body into bytes suitable for
// embedding in a FedForward envelope.
func serializeInnerSNAC(msg wire.SNACMessage) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := wire.MarshalBE(msg.Frame, buf); err != nil {
		return nil, fmt.Errorf("marshal inner frame: %w", err)
	}
	if msg.Body != nil {
		if err := wire.MarshalBE(msg.Body, buf); err != nil {
			return nil, fmt.Errorf("marshal inner body: %w", err)
		}
	}
	return buf.Bytes(), nil
}

// deserializeInnerSNAC unmarshals a SNACFrame from raw bytes and returns the
// frame and a reader positioned at the body. The caller is responsible for
// decoding the body based on the frame's FoodGroup/SubGroup.
func deserializeInnerSNAC(data []byte) (wire.SNACFrame, io.Reader, error) {
	buf := bytes.NewBuffer(data)
	var frame wire.SNACFrame
	if err := wire.UnmarshalBE(&frame, buf); err != nil {
		return frame, nil, fmt.Errorf("unmarshal inner frame: %w", err)
	}
	return frame, buf, nil
}

// sendForwarded wraps a federation SNAC message in a FedForward envelope and
// sends it to the given peer connection for multi-hop transit.
func (m *Manager) sendForwarded(pc *PeerConnection, origin, target string, msg wire.SNACMessage, maxTTL uint8) error {
	innerBytes, err := serializeInnerSNAC(msg)
	if err != nil {
		return fmt.Errorf("serialize inner SNAC for forwarding: %w", err)
	}

	return m.trySend(pc, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedForward,
		},
		Body: wire.SNAC_0x0100_0x0014_FedForward{
			OriginNetwork: origin,
			TargetNetwork: target,
			TTL:           maxTTL,
			InnerSNAC:     innerBytes,
		},
	})
}

// handleFedForward processes an inbound FedForward message. If this server is
// the target, the inner SNAC is unwrapped and dispatched locally. Otherwise,
// the message is forwarded to the next hop toward the target.
func (m *Manager) handleFedForward(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var fwd wire.SNAC_0x0100_0x0014_FedForward
	if err := wire.UnmarshalBE(&fwd, r); err != nil {
		m.logger.Error("unmarshal FedForward", "err", err)
		return
	}

	if fwd.TTL == 0 {
		m.logger.Warn("dropping forwarded message: TTL expired",
			"origin", fwd.OriginNetwork,
			"target", fwd.TargetNetwork,
		)
		return
	}

	// Prevent forwarding back to origin.
	if fwd.OriginNetwork == m.localNetwork {
		m.logger.Warn("dropping forwarded message: looped back to origin",
			"target", fwd.TargetNetwork,
		)
		return
	}

	if fwd.TargetNetwork == m.localNetwork {
		// We are the destination — unwrap and dispatch the inner SNAC.
		m.handleInboundSNAC(ctx, pc, fwd.InnerSNAC)
		return
	}

	// Forward to next hop.
	fwd.TTL--

	nextPC, err := m.routeToNextHop(fwd.TargetNetwork)
	if err != nil {
		m.logger.Warn("cannot forward message: no route",
			"origin", fwd.OriginNetwork,
			"target", fwd.TargetNetwork,
			"err", err,
		)
		return
	}

	if err := m.trySend(nextPC, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedForward,
		},
		Body: fwd,
	}); err != nil {
		m.logger.Error("failed to forward message",
			"origin", fwd.OriginNetwork,
			"target", fwd.TargetNetwork,
			"next_hop", nextPC.config.NetworkName,
			"err", err,
		)
	}
}

// routeToNextHop looks up the next-hop peer for a destination network using
// the gossip routing table, then returns the PeerConnection for that hop.
func (m *Manager) routeToNextHop(targetNetwork string) (*PeerConnection, error) {
	if m.gossip == nil {
		return nil, fmt.Errorf("gossip not enabled")
	}

	nextHop := m.gossip.NextHop(targetNetwork)
	if nextHop == "" {
		return nil, fmt.Errorf("no route to network: %s", targetNetwork)
	}

	m.mu.RLock()
	pc, ok := m.peers[nextHop]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("next hop peer not found: %s", nextHop)
	}

	pc.mu.Lock()
	connected := pc.connected
	pc.mu.Unlock()
	if !connected {
		return nil, fmt.Errorf("next hop peer not connected: %s", nextHop)
	}

	return pc, nil
}

// connectedPeerOrRoute tries to find a direct peer for the given network.
// If no direct peer is available, it falls back to the gossip routing table.
// Returns the PeerConnection and a boolean indicating if forwarding is needed.
func (m *Manager) connectedPeerOrRoute(network string) (*PeerConnection, bool, error) {
	// 1. Try direct peer first (existing behavior).
	m.mu.RLock()
	pc, ok := m.peers[network]
	m.mu.RUnlock()

	if ok {
		pc.mu.Lock()
		connected := pc.connected
		pc.mu.Unlock()
		if connected {
			return pc, false, nil
		}
	}

	// 2. Consult gossip routing table for next hop.
	nextPC, err := m.routeToNextHop(network)
	if err != nil {
		return nil, false, err
	}
	return nextPC, true, nil
}
