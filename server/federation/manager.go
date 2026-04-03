package federation

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mk6i/open-oscar-server/config"
	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

const (
	federationProtocolVersion = 1
	keepAliveInterval         = 30 * time.Second
	sendChanSize              = 256
	maxReconnectBackoff       = 60 * time.Second
	initialReconnectBackoff   = 1 * time.Second
	userInfoQueryTimeout      = 10 * time.Second
)

type userInfoPendingRequest struct {
	replyCh chan wire.SNAC_0x0100_0x000E_FedUserInfoReply
}

type evilPendingRequest struct {
	replyCh chan wire.SNAC_0x0100_0x0010_FedEvilReply
}

// PeerConnection manages a single federation peer link.
type PeerConnection struct {
	config    config.FederationPeerConfig
	conn      net.Conn
	flapc     *wire.FlapClient
	sendCh    chan wire.SNACMessage
	connected bool
	inbound   bool
	mu        sync.Mutex
	cancel    context.CancelFunc
}

// Manager coordinates federation with peer servers. It implements the
// Transport interface used by the federation decorator layer.
type Manager struct {
	localNetwork string
	peers        map[string]*PeerConnection
	peerConfigs  []config.FederationPeerConfig
	logger       *slog.Logger
	mu           sync.RWMutex

	// Local dependencies for handling inbound federation messages.
	localRelayer      MessageRelayer
	localRetriever    SessionRetriever
	localRelFetcher   RelationshipFetcher
	localFeedbag      FeedbagManager
	localProfile      ProfileManager
	remoteStore       *RemoteSessionStore
	allSessionRetriever interface {
		AllSessions() []*state.Session
	}

	// Presence subscriptions: localUser -> set of remote subscribers
	presenceSubs   map[state.IdentScreenName]map[state.IdentScreenName]bool
	presenceSubsMu sync.RWMutex

	pendingUserInfo   map[uint64]*userInfoPendingRequest
	pendingUserInfoMu sync.Mutex
	pendingEvil       map[uint64]*evilPendingRequest
	pendingEvilMu     sync.Mutex

	cookieCounter atomic.Uint64
}

// NewManager creates a new federation Manager.
func NewManager(
	localNetwork string,
	peerConfigs []config.FederationPeerConfig,
	localRelayer MessageRelayer,
	localRetriever SessionRetriever,
	allSessionRetriever interface{ AllSessions() []*state.Session },
	localRelFetcher RelationshipFetcher,
	localFeedbag FeedbagManager,
	localProfile ProfileManager,
	remoteStore *RemoteSessionStore,
	logger *slog.Logger,
) *Manager {
	peers := make(map[string]*PeerConnection)
	for _, cfg := range peerConfigs {
		peers[cfg.NetworkName] = &PeerConnection{
			config: cfg,
			sendCh: make(chan wire.SNACMessage, sendChanSize),
		}
	}
	return &Manager{
		localNetwork:        localNetwork,
		peers:               peers,
		peerConfigs:         peerConfigs,
		localRelayer:        localRelayer,
		localRetriever:      localRetriever,
		allSessionRetriever: allSessionRetriever,
		localRelFetcher:     localRelFetcher,
		localFeedbag:        localFeedbag,
		localProfile:        localProfile,
		remoteStore:         remoteStore,
		logger:              logger,
		presenceSubs:        make(map[state.IdentScreenName]map[state.IdentScreenName]bool),
		pendingUserInfo:     make(map[uint64]*userInfoPendingRequest),
		pendingEvil:         make(map[uint64]*evilPendingRequest),
	}
}

// LocalNetwork returns the local server's network name.
func (m *Manager) LocalNetwork() string {
	return m.localNetwork
}

//
// Transport interface implementation
//

// RouteToRemote inspects the SNAC message type and routes it to the
// appropriate federation peer.
func (m *Manager) RouteToRemote(ctx context.Context, recipient state.IdentScreenName, msg wire.SNACMessage) error {
	network := recipient.Network()
	if network == "" {
		return fmt.Errorf("not a federated user: %s", recipient)
	}

	pc, err := m.connectedPeer(network)
	if err != nil {
		return err
	}

	switch {
	case msg.Frame.FoodGroup == wire.Buddy && msg.Frame.SubGroup == wire.BuddyArrived:
		return m.sendPresenceNotify(pc, recipient, true)
	case msg.Frame.FoodGroup == wire.Buddy && msg.Frame.SubGroup == wire.BuddyDeparted:
		return m.sendPresenceNotify(pc, recipient, false)
	default:
		// Forward the raw SNAC to the peer — this handles ICBM messages,
		// typing events, and any other SNAC types relayed through the
		// message relayer.
		return m.trySend(pc, msg)
	}
}

func (m *Manager) sendPresenceNotify(pc *PeerConnection, recipient state.IdentScreenName, online bool) error {
	onlineFlag := uint8(0)
	if online {
		onlineFlag = 1
	}
	return m.trySend(pc, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedPresenceNotify,
		},
		Body: wire.SNAC_0x0100_0x0009_FedPresenceNotify{
			ScreenName: recipient.LocalPart().String(),
			Online:     onlineFlag,
		},
	})
}

// SubscribePresence requests presence notifications for a remote user.
func (m *Manager) SubscribePresence(_ context.Context, localUser, remoteUser state.IdentScreenName) error {
	network := remoteUser.Network()
	if network == "" {
		return nil
	}

	pc, err := m.connectedPeer(network)
	if err != nil {
		return nil // silently skip if peer not connected; will resubscribe on reconnect
	}

	return m.trySend(pc, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedPresenceSubscribe,
		},
		Body: wire.SNAC_0x0100_0x0007_FedPresenceSubscribe{
			FromUser: localUser.String(),
			ToUser:   remoteUser.LocalPart().String(),
		},
	})
}

// UnsubscribePresence cancels presence notifications for a remote user.
func (m *Manager) UnsubscribePresence(_ context.Context, localUser, remoteUser state.IdentScreenName) error {
	network := remoteUser.Network()
	if network == "" {
		return nil
	}

	pc, err := m.connectedPeer(network)
	if err != nil {
		return nil
	}

	return m.trySend(pc, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedPresenceUnsubscribe,
		},
		Body: wire.SNAC_0x0100_0x0008_FedPresenceUnsubscribe{
			FromUser: localUser.String(),
			ToUser:   remoteUser.LocalPart().String(),
		},
	})
}

// NotifyPresenceToSubscribers notifies all remote peers that have subscribed
// to this local user's presence.
func (m *Manager) NotifyPresenceToSubscribers(_ context.Context, localUser state.IdentScreenName, online bool, _ wire.TLVUserInfo) error {
	m.presenceSubsMu.RLock()
	subs, ok := m.presenceSubs[localUser]
	m.presenceSubsMu.RUnlock()
	if !ok || len(subs) == 0 {
		return nil
	}

	// Group subscribers by network
	networkPeers := make(map[string]bool)
	for sub := range subs {
		if net := sub.Network(); net != "" {
			networkPeers[net] = true
		}
	}

	onlineFlag := uint8(0)
	if online {
		onlineFlag = 1
	}

	for network := range networkPeers {
		pc, err := m.connectedPeer(network)
		if err != nil {
			continue
		}
		m.trySend(pc, wire.SNACMessage{
			Frame: wire.SNACFrame{
				FoodGroup: wire.Federation,
				SubGroup:  wire.FedPresenceNotify,
			},
			Body: wire.SNAC_0x0100_0x0009_FedPresenceNotify{
				ScreenName: localUser.String(),
				Online:     onlineFlag,
			},
		})
	}
	return nil
}

// QueryUserInfo queries a remote server for a user's profile/away message.
func (m *Manager) QueryUserInfo(ctx context.Context, remoteUser state.IdentScreenName, requestType uint32) (*wire.SNAC_0x0100_0x000E_FedUserInfoReply, error) {
	network := remoteUser.Network()
	pc, err := m.connectedPeer(network)
	if err != nil {
		return nil, err
	}

	cookie := m.cookieCounter.Add(1)

	pending := &userInfoPendingRequest{
		replyCh: make(chan wire.SNAC_0x0100_0x000E_FedUserInfoReply, 1),
	}
	m.pendingUserInfoMu.Lock()
	m.pendingUserInfo[cookie] = pending
	m.pendingUserInfoMu.Unlock()
	defer func() {
		m.pendingUserInfoMu.Lock()
		delete(m.pendingUserInfo, cookie)
		m.pendingUserInfoMu.Unlock()
	}()

	if err := m.trySend(pc, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedUserInfoQuery,
		},
		Body: wire.SNAC_0x0100_0x000D_FedUserInfoQuery{
			Cookie: cookie,
			ToUser: remoteUser.LocalPart().String(),
			Type:   uint16(requestType),
		},
	}); err != nil {
		return nil, err
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, userInfoQueryTimeout)
	defer cancel()
	select {
	case reply := <-pending.replyCh:
		return &reply, nil
	case <-timeoutCtx.Done():
		return nil, fmt.Errorf("user info query timed out for %s", remoteUser)
	}
}

//
// Peer connection management
//

// ConnectToPeers starts outbound connections to all configured peers.
// Blocks until ctx is cancelled.
func (m *Manager) ConnectToPeers(ctx context.Context) {
	var wg sync.WaitGroup
	for _, pc := range m.peers {
		wg.Add(1)
		go func(pc *PeerConnection) {
			defer wg.Done()
			m.maintainConnection(ctx, pc)
		}(pc)
	}
	wg.Wait()
}

func (m *Manager) maintainConnection(ctx context.Context, pc *PeerConnection) {
	backoff := initialReconnectBackoff
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pc.mu.Lock()
		alreadyConnected := pc.connected && pc.inbound
		pc.mu.Unlock()
		if alreadyConnected {
			select {
			case <-ctx.Done():
				return
			case <-time.After(keepAliveInterval):
				continue
			}
		}

		m.logger.Info("connecting to federation peer",
			"peer", pc.config.NetworkName, "address", pc.config.Address)

		err := m.connectAndRun(ctx, pc)
		if err != nil && ctx.Err() == nil {
			m.logger.Error("federation peer connection failed",
				"peer", pc.config.NetworkName, "err", err)
		}

		pc.mu.Lock()
		if !pc.inbound {
			pc.connected = false
			pc.conn = nil
			pc.flapc = nil
		}
		pc.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff = min(backoff*2, maxReconnectBackoff)
		}
	}
}

func (m *Manager) connectAndRun(ctx context.Context, pc *PeerConnection) error {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", pc.config.Address)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	flapc := wire.NewFlapClient(100, conn, conn)
	if err := m.performOutboundAuth(flapc, pc.config); err != nil {
		return fmt.Errorf("auth: %w", err)
	}

	peerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	pc.mu.Lock()
	pc.conn = conn
	pc.flapc = flapc
	pc.connected = true
	pc.inbound = false
	pc.cancel = cancel
	pc.mu.Unlock()

	m.logger.Info("federation peer connected",
		"peer", pc.config.NetworkName, "direction", "outbound")

	m.resubscribePresence(peerCtx, pc)
	m.runPeerLoops(peerCtx, pc, flapc)
	return nil
}

// RegisterInboundPeer registers an authenticated inbound peer connection.
func (m *Manager) RegisterInboundPeer(ctx context.Context, networkName string, conn net.Conn, flapc *wire.FlapClient) error {
	m.mu.Lock()
	pc, ok := m.peers[networkName]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown peer network: %s", networkName)
	}

	pc.mu.Lock()
	if pc.connected {
		if m.localNetwork < networkName {
			pc.mu.Unlock()
			return fmt.Errorf("duplicate connection: keeping outbound")
		}
		if pc.cancel != nil {
			pc.cancel()
		}
	}
	pc.conn = conn
	pc.flapc = flapc
	pc.connected = true
	pc.inbound = true
	peerCtx, cancel := context.WithCancel(ctx)
	pc.cancel = cancel
	pc.mu.Unlock()

	m.logger.Info("federation peer connected",
		"peer", networkName, "direction", "inbound")

	m.resubscribePresence(peerCtx, pc)
	m.runPeerLoops(peerCtx, pc, flapc)

	pc.mu.Lock()
	pc.connected = false
	pc.inbound = false
	pc.conn = nil
	pc.flapc = nil
	pc.mu.Unlock()

	return nil
}

// PeerConfigByName returns the peer config for the given network name.
func (m *Manager) PeerConfigByName(networkName string) (config.FederationPeerConfig, bool) {
	for _, cfg := range m.peerConfigs {
		if cfg.NetworkName == networkName {
			return cfg, true
		}
	}
	return config.FederationPeerConfig{}, false
}

//
// Wire protocol: read/write loops
//

func (m *Manager) runPeerLoops(ctx context.Context, pc *PeerConnection, flapc *wire.FlapClient) {
	var wg sync.WaitGroup
	peerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(2)
	go func() { defer wg.Done(); defer cancel(); m.writeLoop(peerCtx, pc, flapc) }()
	go func() { defer wg.Done(); defer cancel(); m.readLoop(peerCtx, pc, flapc) }()
	wg.Wait()
}

func (m *Manager) writeLoop(ctx context.Context, pc *PeerConnection, flapc *wire.FlapClient) {
	ticker := time.NewTicker(keepAliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-pc.sendCh:
			if err := flapc.SendSNAC(msg.Frame, msg.Body); err != nil {
				m.logger.Error("failed to send federation message",
					"peer", pc.config.NetworkName, "err", err)
				return
			}
		case <-ticker.C:
			if err := flapc.SendKeepAliveFrame(); err != nil {
				return
			}
		}
	}
}

func (m *Manager) readLoop(ctx context.Context, pc *PeerConnection, flapc *wire.FlapClient) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		flap, err := flapc.ReceiveFLAP()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.logger.Error("failed to receive federation frame",
				"peer", pc.config.NetworkName, "err", err)
			return
		}
		switch flap.FrameType {
		case wire.FLAPFrameKeepAlive:
		case wire.FLAPFrameData:
			m.handleInboundSNAC(ctx, pc, flap.Payload)
		case wire.FLAPFrameSignoff:
			m.logger.Info("federation peer disconnected", "peer", pc.config.NetworkName)
			return
		}
	}
}

//
// Inbound SNAC dispatch
//

func (m *Manager) handleInboundSNAC(ctx context.Context, pc *PeerConnection, payload []byte) {
	buf := bytes.NewBuffer(payload)
	var frame wire.SNACFrame
	if err := wire.UnmarshalBE(&frame, buf); err != nil {
		m.logger.Error("unmarshal federation frame", "err", err)
		return
	}

	switch frame.SubGroup {
	case wire.FedMessage:
		m.handleFedMessage(ctx, pc, buf)
	case wire.FedMessageAck:
		// fire-and-forget
	case wire.FedMessageErr:
		var errMsg wire.SNAC_0x0100_0x0006_FedMessageErr
		if err := wire.UnmarshalBE(&errMsg, buf); err == nil {
			m.logger.Warn("federation message delivery failed",
				"peer", pc.config.NetworkName, "cookie", errMsg.Cookie, "code", errMsg.Code)
		}
	case wire.FedPresenceSubscribe:
		m.handleFedPresenceSubscribe(ctx, pc, buf)
	case wire.FedPresenceUnsubscribe:
		m.handleFedPresenceUnsubscribe(pc, buf)
	case wire.FedPresenceNotify:
		m.handleFedPresenceNotify(ctx, pc, buf)
	case wire.FedPresenceSubscribeAck:
		m.handleFedPresenceSubscribeAck(ctx, pc, buf)
	case wire.FedTypingEvent:
		m.handleFedTypingEvent(ctx, pc, buf)
	case wire.FedUserInfoQuery:
		m.handleFedUserInfoQuery(ctx, pc, buf)
	case wire.FedUserInfoReply:
		m.handleFedUserInfoReply(buf)
	case wire.FedEvilRequest:
		m.handleFedEvilRequest(ctx, pc, buf)
	case wire.FedEvilReply:
		m.handleFedEvilReply(buf)
	case wire.FedKeepAlive:
	}
}

func (m *Manager) handleFedMessage(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x0004_FedMessage
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		m.logger.Error("unmarshal FedMessage", "err", err)
		return
	}

	federatedSender := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.FromUser), pc.config.NetworkName)
	localRecipient := state.NewIdentScreenName(msg.ToUser)

	recipSess := m.localRetriever.RetrieveSession(localRecipient)
	if recipSess == nil {
		m.sendMessageErr(pc, msg.Cookie, wire.ErrorCodeNotLoggedOn)
		return
	}

	rel, err := m.localRelFetcher.Relationship(ctx, localRecipient, federatedSender)
	if err != nil {
		m.sendMessageErr(pc, msg.Cookie, wire.ErrorCodeGeneralFailure)
		return
	}
	if rel.YouBlock {
		m.sendMessageErr(pc, msg.Cookie, wire.ErrorCodeInLocalPermitDeny)
		return
	}

	m.localRelayer.RelayToScreenName(ctx, localRecipient, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.ICBM,
			SubGroup:  wire.ICBMChannelMsgToClient,
			RequestID: wire.ReqIDFromServer,
		},
		Body: wire.SNAC_0x04_0x07_ICBMChannelMsgToClient{
			Cookie:       msg.Cookie,
			ChannelID:    msg.ChannelID,
			TLVUserInfo:  wire.TLVUserInfo{ScreenName: federatedSender.String()},
			TLVRestBlock: msg.TLVRestBlock,
		},
	})
	m.sendMessageAck(pc, msg.Cookie)
}

func (m *Manager) handleFedTypingEvent(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x000A_FedTypingEvent
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		return
	}
	federatedSender := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.FromUser), pc.config.NetworkName)
	localRecipient := state.NewIdentScreenName(msg.ToUser)

	m.localRelayer.RelayToScreenName(ctx, localRecipient, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.ICBM,
			SubGroup:  wire.ICBMClientEvent,
			RequestID: wire.ReqIDFromServer,
		},
		Body: wire.SNAC_0x04_0x14_ICBMClientEvent{
			Cookie:     msg.Cookie,
			ChannelID:  msg.ChannelID,
			ScreenName: federatedSender.String(),
			Event:      msg.Event,
		},
	})
}

func (m *Manager) handleFedPresenceSubscribe(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x0007_FedPresenceSubscribe
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		return
	}
	localUser := state.NewIdentScreenName(msg.ToUser)
	remoteSubscriber := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.FromUser), pc.config.NetworkName)

	m.presenceSubsMu.Lock()
	if m.presenceSubs[localUser] == nil {
		m.presenceSubs[localUser] = make(map[state.IdentScreenName]bool)
	}
	m.presenceSubs[localUser][remoteSubscriber] = true
	m.presenceSubsMu.Unlock()

	sess := m.localRetriever.RetrieveSession(localUser)
	online := uint8(0)
	if sess != nil {
		online = 1
	}

	m.trySend(pc, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedPresenceSubscribeAck,
		},
		Body: wire.SNAC_0x0100_0x000C_FedPresenceSubscribeAck{
			ScreenName: localUser.String(),
			Online:     online,
		},
	})
}

func (m *Manager) handleFedPresenceUnsubscribe(pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x0008_FedPresenceUnsubscribe
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		return
	}
	localUser := state.NewIdentScreenName(msg.ToUser)
	remoteSubscriber := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.FromUser), pc.config.NetworkName)

	m.presenceSubsMu.Lock()
	if subs, ok := m.presenceSubs[localUser]; ok {
		delete(subs, remoteSubscriber)
		if len(subs) == 0 {
			delete(m.presenceSubs, localUser)
		}
	}
	m.presenceSubsMu.Unlock()
}

func (m *Manager) handleFedPresenceNotify(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x0009_FedPresenceNotify
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		return
	}

	remoteUser := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.ScreenName), pc.config.NetworkName)

	if msg.Online == 1 {
		m.remoteStore.PresenceArrived(remoteUser, msg.TLVRestBlock)
	} else {
		m.remoteStore.PresenceDeparted(remoteUser)
	}

	m.deliverPresenceToLocalSubscribers(ctx, remoteUser, msg.Online == 1)
}

func (m *Manager) handleFedPresenceSubscribeAck(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x000C_FedPresenceSubscribeAck
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		return
	}

	remoteUser := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.ScreenName), pc.config.NetworkName)

	if msg.Online == 1 {
		m.remoteStore.PresenceArrived(remoteUser, msg.TLVRestBlock)
	} else {
		m.remoteStore.PresenceDeparted(remoteUser)
	}

	m.deliverPresenceToLocalSubscribers(ctx, remoteUser, msg.Online == 1)
}

func (m *Manager) deliverPresenceToLocalSubscribers(ctx context.Context, remoteUser state.IdentScreenName, online bool) {
	sessions := m.allSessionRetriever.AllSessions()
	for _, sess := range sessions {
		localUser := sess.IdentScreenName()
		feedbag, err := m.localFeedbag.Feedbag(ctx, localUser)
		if err != nil {
			continue
		}
		hasBuddy := false
		for _, item := range feedbag {
			if item.ClassID == wire.FeedbagClassIdBuddy {
				if state.NewIdentScreenName(item.Name) == remoteUser {
					hasBuddy = true
					break
				}
			}
		}
		if !hasBuddy {
			continue
		}

		if online {
			m.localRelayer.RelayToScreenName(ctx, localUser, wire.SNACMessage{
				Frame: wire.SNACFrame{
					FoodGroup: wire.Buddy,
					SubGroup:  wire.BuddyArrived,
					RequestID: wire.ReqIDFromServer,
				},
				Body: wire.SNAC_0x03_0x0B_BuddyArrived{
					TLVUserInfo: wire.TLVUserInfo{
						ScreenName: remoteUser.String(),
						TLVBlock: wire.TLVBlock{
							TLVList: wire.TLVList{
								wire.NewTLVBE(wire.OServiceUserInfoUserFlags, wire.OServiceUserFlagOSCARFree),
								wire.NewTLVBE(wire.OServiceUserInfoSignonTOD, uint32(time.Now().Unix())),
							},
						},
					},
				},
			})
		} else {
			m.localRelayer.RelayToScreenName(ctx, localUser, wire.SNACMessage{
				Frame: wire.SNACFrame{
					FoodGroup: wire.Buddy,
					SubGroup:  wire.BuddyDeparted,
					RequestID: wire.ReqIDFromServer,
				},
				Body: wire.SNAC_0x03_0x0C_BuddyDeparted{
					TLVUserInfo: wire.TLVUserInfo{
						ScreenName: remoteUser.String(),
						TLVBlock: wire.TLVBlock{
							TLVList: wire.TLVList{
								wire.NewTLVBE(wire.OServiceUserInfoUserFlags, uint16(0)),
							},
						},
					},
				},
			})
		}
	}
}

func (m *Manager) handleFedUserInfoQuery(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var query wire.SNAC_0x0100_0x000D_FedUserInfoQuery
	if err := wire.UnmarshalBE(&query, r); err != nil {
		return
	}

	localUser := state.NewIdentScreenName(query.ToUser)
	reply := wire.SNAC_0x0100_0x000E_FedUserInfoReply{
		Cookie:     query.Cookie,
		ScreenName: localUser.String(),
	}

	sess := m.localRetriever.RetrieveSession(localUser)

	if query.Type&uint16(wire.LocateTypeSig) != 0 {
		var prof state.UserProfile
		if sess != nil {
			// Check session-level profile first
			instances := sess.Instances()
			if len(instances) > 0 {
				prof = instances[0].Profile()
			}
		}
		if prof.ProfileText == "" {
			if serverProf, err := m.localProfile.Profile(ctx, localUser); err == nil {
				prof = serverProf
			}
		}
		reply.Append(wire.NewTLVBE(wire.LocateTLVTagsInfoSigMime, prof.MIMEType))
		reply.Append(wire.NewTLVBE(wire.LocateTLVTagsInfoSigData, prof.ProfileText))
	}

	if query.Type&uint16(wire.LocateTypeUnavailable) != 0 && sess != nil && sess.Away() {
		reply.Append(wire.NewTLVBE(wire.LocateTLVTagsInfoUnavailableMime, `text/aolrtf; charset="us-ascii"`))
		reply.Append(wire.NewTLVBE(wire.LocateTLVTagsInfoUnavailableData, sess.AwayMessage()))
	}

	if sess != nil {
		for _, tlv := range sess.TLVUserInfo().TLVList {
			if tlv.Tag == wire.OServiceUserInfoUserFlags && len(tlv.Value) >= 2 {
				reply.Flags = uint16(tlv.Value[0])<<8 | uint16(tlv.Value[1])
				break
			}
		}
	}

	m.trySend(pc, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedUserInfoReply,
		},
		Body: reply,
	})
}

func (m *Manager) handleFedUserInfoReply(r io.Reader) {
	var reply wire.SNAC_0x0100_0x000E_FedUserInfoReply
	if err := wire.UnmarshalBE(&reply, r); err != nil {
		return
	}
	m.pendingUserInfoMu.Lock()
	pending, ok := m.pendingUserInfo[reply.Cookie]
	m.pendingUserInfoMu.Unlock()
	if ok {
		select {
		case pending.replyCh <- reply:
		default:
		}
	}
}

func (m *Manager) handleFedEvilRequest(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x000F_FedEvilRequest
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		return
	}

	federatedSender := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.FromUser), pc.config.NetworkName)
	localRecipient := state.NewIdentScreenName(msg.ToUser)

	recipSess := m.localRetriever.RetrieveSession(localRecipient)
	if recipSess == nil {
		m.sendEvilReply(pc, msg.Cookie, 0, 0, wire.ErrorCodeNotLoggedOn)
		return
	}

	rel, err := m.localRelFetcher.Relationship(ctx, localRecipient, federatedSender)
	if err != nil {
		m.sendEvilReply(pc, msg.Cookie, 0, 0, wire.ErrorCodeGeneralFailure)
		return
	}
	if rel.YouBlock {
		m.sendEvilReply(pc, msg.Cookie, 0, 0, wire.ErrorCodeInLocalPermitDeny)
		return
	}

	increase := int16(100)
	if msg.SendAs == 1 {
		increase = 30
	}
	newWarning := int32(recipSess.Warning()) + int32(increase)
	if newWarning > 1000 {
		newWarning = 1000
	}
	recipSess.SetWarning(uint16(newWarning))

	notif := wire.SNAC_0x01_0x10_OServiceEvilNotification{
		NewEvil: uint16(newWarning),
	}
	if msg.SendAs == 0 {
		notif.Snitcher = &struct{ wire.TLVUserInfo }{
			TLVUserInfo: wire.TLVUserInfo{ScreenName: federatedSender.String()},
		}
	}
	m.localRelayer.RelayToScreenName(ctx, localRecipient, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.OService,
			SubGroup:  wire.OServiceEvilNotification,
		},
		Body: notif,
	})

	m.sendEvilReply(pc, msg.Cookie, uint16(increase), uint16(newWarning), 0)
}

func (m *Manager) handleFedEvilReply(r io.Reader) {
	var reply wire.SNAC_0x0100_0x0010_FedEvilReply
	if err := wire.UnmarshalBE(&reply, r); err != nil {
		return
	}
	m.pendingEvilMu.Lock()
	pending, ok := m.pendingEvil[reply.Cookie]
	m.pendingEvilMu.Unlock()
	if ok {
		select {
		case pending.replyCh <- reply:
		default:
		}
	}
}

//
// Auth handshake
//

func (m *Manager) performOutboundAuth(flapc *wire.FlapClient, cfg config.FederationPeerConfig) error {
	var challenge [32]byte
	if _, err := rand.Read(challenge[:]); err != nil {
		return fmt.Errorf("generating challenge: %w", err)
	}

	if err := flapc.SendSNAC(wire.SNACFrame{
		FoodGroup: wire.Federation, SubGroup: wire.FedAuthRequest,
	}, wire.SNAC_0x0100_0x0001_FedAuthRequest{
		NetworkName: m.localNetwork, Challenge: challenge, Version: federationProtocolVersion,
	}); err != nil {
		return fmt.Errorf("send auth request: %w", err)
	}

	flap, err := flapc.ReceiveFLAP()
	if err != nil {
		return fmt.Errorf("recv peer auth request: %w", err)
	}
	var peerFrame wire.SNACFrame
	var peerAuthReq wire.SNAC_0x0100_0x0001_FedAuthRequest
	buf := bytes.NewBuffer(flap.Payload)
	if err := wire.UnmarshalBE(&peerFrame, buf); err != nil {
		return err
	}
	if err := wire.UnmarshalBE(&peerAuthReq, buf); err != nil {
		return err
	}
	if peerAuthReq.NetworkName != cfg.NetworkName {
		return fmt.Errorf("peer network name mismatch: expected %q, got %q", cfg.NetworkName, peerAuthReq.NetworkName)
	}

	digest := computeHMAC(peerAuthReq.Challenge[:], m.localNetwork, cfg.Secret)
	if err := flapc.SendSNAC(wire.SNACFrame{
		FoodGroup: wire.Federation, SubGroup: wire.FedAuthResponse,
	}, wire.SNAC_0x0100_0x0002_FedAuthResponse{
		NetworkName: m.localNetwork, Digest: digest,
	}); err != nil {
		return fmt.Errorf("send auth response: %w", err)
	}

	flap, err = flapc.ReceiveFLAP()
	if err != nil {
		return fmt.Errorf("recv auth response: %w", err)
	}
	var respFrame wire.SNACFrame
	var peerAuthResp wire.SNAC_0x0100_0x0002_FedAuthResponse
	buf = bytes.NewBuffer(flap.Payload)
	if err := wire.UnmarshalBE(&respFrame, buf); err != nil {
		return err
	}
	if err := wire.UnmarshalBE(&peerAuthResp, buf); err != nil {
		return err
	}

	expectedDigest := computeHMAC(challenge[:], cfg.NetworkName, cfg.Secret)
	if peerAuthResp.Digest != expectedDigest {
		flapc.SendSNAC(wire.SNACFrame{
			FoodGroup: wire.Federation, SubGroup: wire.FedAuthResult,
		}, wire.SNAC_0x0100_0x0003_FedAuthResult{Code: wire.FedAuthResultFailed})
		return fmt.Errorf("auth failed: invalid digest from peer %q", cfg.NetworkName)
	}

	if err := flapc.SendSNAC(wire.SNACFrame{
		FoodGroup: wire.Federation, SubGroup: wire.FedAuthResult,
	}, wire.SNAC_0x0100_0x0003_FedAuthResult{Code: wire.FedAuthResultSuccess}); err != nil {
		return err
	}

	flap, err = flapc.ReceiveFLAP()
	if err != nil {
		return fmt.Errorf("recv auth result: %w", err)
	}
	var resultFrame wire.SNACFrame
	var authResult wire.SNAC_0x0100_0x0003_FedAuthResult
	buf = bytes.NewBuffer(flap.Payload)
	if err := wire.UnmarshalBE(&resultFrame, buf); err != nil {
		return err
	}
	if err := wire.UnmarshalBE(&authResult, buf); err != nil {
		return err
	}
	if authResult.Code != wire.FedAuthResultSuccess {
		return fmt.Errorf("auth rejected by peer: code %d", authResult.Code)
	}
	return nil
}

//
// Helpers
//

func (m *Manager) resubscribePresence(ctx context.Context, pc *PeerConnection) {
	sessions := m.allSessionRetriever.AllSessions()
	for _, sess := range sessions {
		localUser := sess.IdentScreenName()
		feedbag, err := m.localFeedbag.Feedbag(ctx, localUser)
		if err != nil {
			continue
		}
		for _, item := range feedbag {
			if item.ClassID == wire.FeedbagClassIdBuddy {
				buddyName := state.NewIdentScreenName(item.Name)
				if buddyName.Network() == pc.config.NetworkName {
					m.trySend(pc, wire.SNACMessage{
						Frame: wire.SNACFrame{
							FoodGroup: wire.Federation,
							SubGroup:  wire.FedPresenceSubscribe,
						},
						Body: wire.SNAC_0x0100_0x0007_FedPresenceSubscribe{
							FromUser: localUser.String(),
							ToUser:   buddyName.LocalPart().String(),
						},
					})
				}
			}
		}
	}
}

func (m *Manager) connectedPeer(network string) (*PeerConnection, error) {
	m.mu.RLock()
	pc, ok := m.peers[network]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown federation network: %s", network)
	}
	pc.mu.Lock()
	connected := pc.connected
	pc.mu.Unlock()
	if !connected {
		return nil, fmt.Errorf("federation peer not connected: %s", network)
	}
	return pc, nil
}

func (m *Manager) trySend(pc *PeerConnection, msg wire.SNACMessage) error {
	select {
	case pc.sendCh <- msg:
		return nil
	default:
		return fmt.Errorf("send channel full for peer: %s", pc.config.NetworkName)
	}
}

func (m *Manager) sendMessageAck(pc *PeerConnection, cookie uint64) {
	m.trySend(pc, wire.SNACMessage{
		Frame: wire.SNACFrame{FoodGroup: wire.Federation, SubGroup: wire.FedMessageAck},
		Body:  wire.SNAC_0x0100_0x0005_FedMessageAck{Cookie: cookie},
	})
}

func (m *Manager) sendMessageErr(pc *PeerConnection, cookie uint64, code uint16) {
	m.trySend(pc, wire.SNACMessage{
		Frame: wire.SNACFrame{FoodGroup: wire.Federation, SubGroup: wire.FedMessageErr},
		Body:  wire.SNAC_0x0100_0x0006_FedMessageErr{Cookie: cookie, Code: code},
	})
}

func (m *Manager) sendEvilReply(pc *PeerConnection, cookie uint64, delta, updated, errCode uint16) {
	m.trySend(pc, wire.SNACMessage{
		Frame: wire.SNACFrame{FoodGroup: wire.Federation, SubGroup: wire.FedEvilReply},
		Body: wire.SNAC_0x0100_0x0010_FedEvilReply{
			Cookie: cookie, EvilDeltaApplied: delta, UpdatedEvilValue: updated, ErrorCode: errCode,
		},
	})
}

// computeHMAC computes HMAC-SHA256(challenge + networkName, secret).
func computeHMAC(challenge []byte, networkName string, secret string) [32]byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(challenge)
	mac.Write([]byte(networkName))
	var digest [32]byte
	copy(digest[:], mac.Sum(nil))
	return digest
}
