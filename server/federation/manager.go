// Package federation implements inter-server federation for the OSCAR protocol.
// It allows multiple Open Oscar Server instances to exchange messages and
// presence notifications, enabling users on different servers to communicate.
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
	keepAliveTimeout          = 90 * time.Second
	sendChanSize              = 256
	maxReconnectBackoff       = 60 * time.Second
	initialReconnectBackoff   = 1 * time.Second
	userInfoQueryTimeout      = 10 * time.Second
)

// userInfoPendingRequest holds the channel used to deliver a FedUserInfoReply
// back to the goroutine that initiated the query.
type userInfoPendingRequest struct {
	replyCh chan wire.SNAC_0x0100_0x000E_FedUserInfoReply
}

// evilPendingRequest holds the channel used to deliver a FedEvilReply
// back to the goroutine that initiated the evil request.
type evilPendingRequest struct {
	replyCh chan wire.SNAC_0x0100_0x0010_FedEvilReply
}

// MessageRelayer defines methods for delivering SNAC messages to local users.
type MessageRelayer interface {
	RelayToScreenName(ctx context.Context, screenName state.IdentScreenName, msg wire.SNACMessage)
}

// SessionRetriever retrieves an active session by screen name.
type SessionRetriever interface {
	RetrieveSession(screenName state.IdentScreenName) *state.Session
}

// RelationshipFetcher checks the relationship between two users.
type RelationshipFetcher interface {
	Relationship(ctx context.Context, me state.IdentScreenName, them state.IdentScreenName) (state.Relationship, error)
}

// FeedbagRetriever retrieves buddy list entries for a user.
type FeedbagRetriever interface {
	Feedbag(ctx context.Context, screenName state.IdentScreenName) ([]wire.FeedbagItem, error)
}

// ProfileRetriever retrieves a user's server-side profile.
type ProfileRetriever interface {
	Profile(ctx context.Context, screenName state.IdentScreenName) (state.UserProfile, error)
}

// AllSessionsRetriever retrieves all online sessions.
type AllSessionsRetriever interface {
	AllSessions() []*state.Session
}

// PeerConnection manages a single federation peer link.
type PeerConnection struct {
	config    config.FederationPeerConfig
	conn      net.Conn
	flapc     *wire.FlapClient
	sendCh    chan wire.SNACMessage
	connected bool
	inbound   bool // true if the active connection was established by the peer
	mu        sync.Mutex
	cancel    context.CancelFunc
}

// Manager coordinates federation with peer servers.
type Manager struct {
	localNetwork        string
	peers               map[string]*PeerConnection // keyed by network name
	peerConfigs         []config.FederationPeerConfig
	messageRelayer      MessageRelayer
	sessionRetriever    SessionRetriever
	allSessionRetriever AllSessionsRetriever
	relationshipFetcher RelationshipFetcher
	feedbagRetriever    FeedbagRetriever
	profileRetriever    ProfileRetriever
	logger              *slog.Logger
	mu                  sync.RWMutex

	// Presence subscriptions: remoteUser@network -> set of local subscribers
	presenceSubs   map[state.IdentScreenName]map[state.IdentScreenName]bool
	presenceSubsMu sync.RWMutex

	// Pending user info queries awaiting replies from remote peers
	pendingUserInfo   map[uint64]*userInfoPendingRequest
	pendingUserInfoMu sync.Mutex

	// Pending evil requests awaiting replies from remote peers
	pendingEvil   map[uint64]*evilPendingRequest
	pendingEvilMu sync.Mutex

	cookieCounter atomic.Uint64
}

// NewManager creates a new federation Manager.
func NewManager(
	localNetwork string,
	peerConfigs []config.FederationPeerConfig,
	messageRelayer MessageRelayer,
	sessionRetriever SessionRetriever,
	allSessionRetriever AllSessionsRetriever,
	relationshipFetcher RelationshipFetcher,
	feedbagRetriever FeedbagRetriever,
	profileRetriever ProfileRetriever,
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
		messageRelayer:      messageRelayer,
		sessionRetriever:    sessionRetriever,
		allSessionRetriever: allSessionRetriever,
		relationshipFetcher: relationshipFetcher,
		feedbagRetriever:    feedbagRetriever,
		profileRetriever:    profileRetriever,
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

// ConnectToPeers starts outbound connections to all configured peers.
// It blocks until the context is cancelled. Call SetContext before calling this.
func (m *Manager) ConnectToPeers(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, pc := range m.peers {
		wg.Add(1)
		go func(pc *PeerConnection) {
			defer wg.Done()
			m.maintainConnection(ctx, pc)
		}(pc)
	}
	wg.Wait()
	return nil
}

// ConnectToPeersFunc returns a function suitable for errgroup.Go that starts
// outbound connections using the provided context.
func (m *Manager) ConnectToPeersFunc(ctx context.Context) func() error {
	return func() error {
		return m.ConnectToPeers(ctx)
	}
}

func (m *Manager) maintainConnection(ctx context.Context, pc *PeerConnection) {
	backoff := initialReconnectBackoff

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// If this peer already has an active inbound connection (registered
		// by RegisterInboundPeer), don't try to dial outbound. Just wait
		// and check again later.
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
			"peer", pc.config.NetworkName,
			"address", pc.config.Address,
		)

		err := m.connectAndRun(ctx, pc)
		if err != nil && ctx.Err() == nil {
			m.logger.Error("federation peer connection failed",
				"peer", pc.config.NetworkName,
				"err", err,
			)
		}

		// Only clear the connection state if we still own it (i.e., an
		// inbound connection hasn't taken over while we were running).
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

	// Perform auth handshake
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
		"peer", pc.config.NetworkName,
		"direction", "outbound",
	)

	// Re-subscribe to presence for all relevant buddies
	m.resubscribePresence(peerCtx, pc)

	// Run read and write loops
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer cancel()
		m.writeLoop(peerCtx, pc, flapc)
	}()

	go func() {
		defer wg.Done()
		defer cancel()
		m.readLoop(peerCtx, pc, flapc)
	}()

	wg.Wait()
	return nil
}

func (m *Manager) performOutboundAuth(flapc *wire.FlapClient, cfg config.FederationPeerConfig) error {
	// Generate challenge
	var challenge [32]byte
	if _, err := rand.Read(challenge[:]); err != nil {
		return fmt.Errorf("generating challenge: %w", err)
	}

	// Send auth request
	if err := flapc.SendSNAC(wire.SNACFrame{
		FoodGroup: wire.Federation,
		SubGroup:  wire.FedAuthRequest,
	}, wire.SNAC_0x0100_0x0001_FedAuthRequest{
		NetworkName: m.localNetwork,
		Challenge:   challenge,
		Version:     federationProtocolVersion,
	}); err != nil {
		return fmt.Errorf("sending auth request: %w", err)
	}

	// Receive peer's auth request
	flap, err := flapc.ReceiveFLAP()
	if err != nil {
		return fmt.Errorf("receiving peer auth request: %w", err)
	}

	var peerFrame wire.SNACFrame
	var peerAuthReq wire.SNAC_0x0100_0x0001_FedAuthRequest
	buf := bytes.NewBuffer(flap.Payload)
	if err := wire.UnmarshalBE(&peerFrame, buf); err != nil {
		return fmt.Errorf("unmarshalling peer frame: %w", err)
	}
	if err := wire.UnmarshalBE(&peerAuthReq, buf); err != nil {
		return fmt.Errorf("unmarshalling peer auth request: %w", err)
	}

	// Verify peer network name matches expected
	if peerAuthReq.NetworkName != cfg.NetworkName {
		return fmt.Errorf("peer network name mismatch: expected %q, got %q", cfg.NetworkName, peerAuthReq.NetworkName)
	}

	// Compute HMAC digest for their challenge
	digest := computeHMAC(peerAuthReq.Challenge[:], m.localNetwork, cfg.Secret)

	// Send auth response
	if err := flapc.SendSNAC(wire.SNACFrame{
		FoodGroup: wire.Federation,
		SubGroup:  wire.FedAuthResponse,
	}, wire.SNAC_0x0100_0x0002_FedAuthResponse{
		NetworkName: m.localNetwork,
		Digest:      digest,
	}); err != nil {
		return fmt.Errorf("sending auth response: %w", err)
	}

	// Receive peer's auth response
	flap, err = flapc.ReceiveFLAP()
	if err != nil {
		return fmt.Errorf("receiving peer auth response: %w", err)
	}

	var respFrame wire.SNACFrame
	var peerAuthResp wire.SNAC_0x0100_0x0002_FedAuthResponse
	buf = bytes.NewBuffer(flap.Payload)
	if err := wire.UnmarshalBE(&respFrame, buf); err != nil {
		return fmt.Errorf("unmarshalling response frame: %w", err)
	}
	if err := wire.UnmarshalBE(&peerAuthResp, buf); err != nil {
		return fmt.Errorf("unmarshalling auth response: %w", err)
	}

	// Verify their digest
	expectedDigest := computeHMAC(challenge[:], cfg.NetworkName, cfg.Secret)
	if peerAuthResp.Digest != expectedDigest {
		// Send auth failure
		flapc.SendSNAC(wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedAuthResult,
		}, wire.SNAC_0x0100_0x0003_FedAuthResult{
			Code: wire.FedAuthResultFailed,
		})
		return fmt.Errorf("auth failed: invalid digest from peer %q", cfg.NetworkName)
	}

	// Send auth success
	if err := flapc.SendSNAC(wire.SNACFrame{
		FoodGroup: wire.Federation,
		SubGroup:  wire.FedAuthResult,
	}, wire.SNAC_0x0100_0x0003_FedAuthResult{
		Code: wire.FedAuthResultSuccess,
	}); err != nil {
		return fmt.Errorf("sending auth result: %w", err)
	}

	// Receive peer's auth result
	flap, err = flapc.ReceiveFLAP()
	if err != nil {
		return fmt.Errorf("receiving auth result: %w", err)
	}

	var resultFrame wire.SNACFrame
	var authResult wire.SNAC_0x0100_0x0003_FedAuthResult
	buf = bytes.NewBuffer(flap.Payload)
	if err := wire.UnmarshalBE(&resultFrame, buf); err != nil {
		return fmt.Errorf("unmarshalling result frame: %w", err)
	}
	if err := wire.UnmarshalBE(&authResult, buf); err != nil {
		return fmt.Errorf("unmarshalling auth result: %w", err)
	}
	if authResult.Code != wire.FedAuthResultSuccess {
		return fmt.Errorf("auth rejected by peer: code %d", authResult.Code)
	}

	return nil
}

func (m *Manager) writeLoop(ctx context.Context, pc *PeerConnection, flapc *wire.FlapClient) {
	keepAliveTicker := time.NewTicker(keepAliveInterval)
	defer keepAliveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-pc.sendCh:
			if err := flapc.SendSNAC(msg.Frame, msg.Body); err != nil {
				m.logger.Error("failed to send federation message",
					"peer", pc.config.NetworkName,
					"err", err,
				)
				return
			}
		case <-keepAliveTicker.C:
			if err := flapc.SendKeepAliveFrame(); err != nil {
				m.logger.Error("failed to send keepalive",
					"peer", pc.config.NetworkName,
					"err", err,
				)
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
				"peer", pc.config.NetworkName,
				"err", err,
			)
			return
		}

		switch flap.FrameType {
		case wire.FLAPFrameKeepAlive:
			continue
		case wire.FLAPFrameData:
			m.handleInboundSNAC(ctx, pc, flap.Payload)
		case wire.FLAPFrameSignoff:
			m.logger.Info("federation peer disconnected",
				"peer", pc.config.NetworkName,
			)
			return
		}
	}
}

func (m *Manager) handleInboundSNAC(ctx context.Context, pc *PeerConnection, payload []byte) {
	buf := bytes.NewBuffer(payload)

	var frame wire.SNACFrame
	if err := wire.UnmarshalBE(&frame, buf); err != nil {
		m.logger.Error("failed to unmarshal federation SNAC frame",
			"peer", pc.config.NetworkName,
			"err", err,
		)
		return
	}

	switch frame.SubGroup {
	case wire.FedMessage:
		m.handleFedMessage(ctx, pc, buf)
	case wire.FedMessageAck:
		// Currently fire-and-forget; acks are not awaited.
	case wire.FedMessageErr:
		// Log errors from the remote side.
		var errMsg wire.SNAC_0x0100_0x0006_FedMessageErr
		if err := wire.UnmarshalBE(&errMsg, buf); err == nil {
			m.logger.Warn("federation message delivery failed",
				"peer", pc.config.NetworkName,
				"cookie", errMsg.Cookie,
				"code", errMsg.Code,
			)
		}
	case wire.FedPresenceSubscribe:
		m.handleFedPresenceSubscribe(ctx, pc, buf)
	case wire.FedPresenceUnsubscribe:
		m.handleFedPresenceUnsubscribe(ctx, pc, buf)
	case wire.FedPresenceNotify:
		m.handleFedPresenceNotify(ctx, pc, buf)
	case wire.FedPresenceSubscribeAck:
		m.handleFedPresenceSubscribeAck(ctx, pc, buf)
	case wire.FedTypingEvent:
		m.handleFedTypingEvent(ctx, pc, buf)
	case wire.FedUserInfoQuery:
		m.handleFedUserInfoQuery(ctx, pc, buf)
	case wire.FedUserInfoReply:
		m.handleFedUserInfoReply(pc, buf)
	case wire.FedEvilRequest:
		m.handleFedEvilRequest(ctx, pc, buf)
	case wire.FedEvilReply:
		m.handleFedEvilReply(pc, buf)
	case wire.FedKeepAlive:
		// no-op
	default:
		m.logger.Warn("unknown federation SNAC subgroup",
			"peer", pc.config.NetworkName,
			"subgroup", frame.SubGroup,
		)
	}
}

// handleFedMessage processes an inbound instant message from a federation peer.
func (m *Manager) handleFedMessage(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x0004_FedMessage
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		m.logger.Error("failed to unmarshal federation message", "err", err)
		return
	}

	// Construct the federated sender name (e.g., "compy@retra.im")
	federatedSender := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.FromUser),
		pc.config.NetworkName,
	)
	localRecipient := state.NewIdentScreenName(msg.ToUser)

	m.logger.Debug("recv FedMessage",
		"peer", pc.config.NetworkName,
		"from", federatedSender,
		"to", localRecipient,
		"channel", msg.ChannelID,
	)

	// Check if local recipient is online
	recipSess := m.sessionRetriever.RetrieveSession(localRecipient)
	if recipSess == nil {
		m.logger.Debug("federated message recipient offline",
			"to", localRecipient,
		)
		m.sendMessageErr(pc, msg.Cookie, wire.ErrorCodeNotLoggedOn)
		return
	}

	// Check if the local user has blocked the federated sender
	rel, err := m.relationshipFetcher.Relationship(ctx, localRecipient, federatedSender)
	if err != nil {
		m.logger.Error("failed to check relationship for federation message", "err", err)
		m.sendMessageErr(pc, msg.Cookie, wire.ErrorCodeGeneralFailure)
		return
	}
	if rel.YouBlock {
		m.logger.Debug("federated message blocked by recipient",
			"from", federatedSender,
			"to", localRecipient,
		)
		m.sendMessageErr(pc, msg.Cookie, wire.ErrorCodeInLocalPermitDeny)
		return
	}

	// Build the ICBM message to deliver to the local user
	clientIM := wire.SNAC_0x04_0x07_ICBMChannelMsgToClient{
		Cookie:    msg.Cookie,
		ChannelID: msg.ChannelID,
		TLVUserInfo: wire.TLVUserInfo{
			ScreenName: federatedSender.String(),
		},
		TLVRestBlock: msg.TLVRestBlock,
	}

	m.messageRelayer.RelayToScreenName(ctx, localRecipient, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.ICBM,
			SubGroup:  wire.ICBMChannelMsgToClient,
			RequestID: wire.ReqIDFromServer,
		},
		Body: clientIM,
	})

	m.logger.Debug("delivered federated message to local user",
		"from", federatedSender,
		"to", localRecipient,
	)

	// Send ack back to peer
	m.sendMessageAck(pc, msg.Cookie)
}

// handleFedTypingEvent processes an inbound typing notification from a federation peer.
func (m *Manager) handleFedTypingEvent(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x000A_FedTypingEvent
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		m.logger.Error("failed to unmarshal federation typing event", "err", err)
		return
	}

	federatedSender := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.FromUser),
		pc.config.NetworkName,
	)
	localRecipient := state.NewIdentScreenName(msg.ToUser)

	m.logger.Debug("received federated typing event",
		"peer", pc.config.NetworkName,
		"from", federatedSender,
		"to", localRecipient,
		"event", msg.Event,
	)

	m.messageRelayer.RelayToScreenName(ctx, localRecipient, wire.SNACMessage{
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

// handleFedPresenceSubscribe processes an inbound presence subscription request.
func (m *Manager) handleFedPresenceSubscribe(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x0007_FedPresenceSubscribe
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		m.logger.Error("failed to unmarshal presence subscribe", "err", err)
		return
	}

	localUser := state.NewIdentScreenName(msg.ToUser)
	remoteSubscriber := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.FromUser),
		pc.config.NetworkName,
	)

	m.logger.Debug("received presence subscribe",
		"peer", pc.config.NetworkName,
		"remote_subscriber", remoteSubscriber,
		"local_user", localUser,
	)

	// Track that this remote user wants to know about localUser's presence
	m.presenceSubsMu.Lock()
	if m.presenceSubs[localUser] == nil {
		m.presenceSubs[localUser] = make(map[state.IdentScreenName]bool)
	}
	m.presenceSubs[localUser][remoteSubscriber] = true
	m.presenceSubsMu.Unlock()

	// Send current status
	sess := m.sessionRetriever.RetrieveSession(localUser)
	online := uint8(0)
	var restBlock wire.TLVRestBlock
	if sess != nil {
		online = 1
		info := sess.TLVUserInfo()
		restBlock.Append(wire.NewTLVBE(0x0001, info.WarningLevel))
	}

	m.logger.Debug("sending presence subscribe ack",
		"peer", pc.config.NetworkName,
		"local_user", localUser,
		"online", online == 1,
	)

	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.connected && pc.flapc != nil {
		pc.flapc.SendSNAC(wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedPresenceSubscribeAck,
		}, wire.SNAC_0x0100_0x000C_FedPresenceSubscribeAck{
			ScreenName:   localUser.String(),
			Online:       online,
			TLVRestBlock: restBlock,
		})
	} else {
		m.logger.Warn("cannot send presence subscribe ack, peer not connected",
			"peer", pc.config.NetworkName,
		)
	}
}

// handleFedPresenceUnsubscribe processes an inbound presence unsubscription.
func (m *Manager) handleFedPresenceUnsubscribe(_ context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x0008_FedPresenceUnsubscribe
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		m.logger.Error("failed to unmarshal presence unsubscribe", "err", err)
		return
	}

	localUser := state.NewIdentScreenName(msg.ToUser)
	remoteSubscriber := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.FromUser),
		pc.config.NetworkName,
	)

	m.logger.Debug("received presence unsubscribe",
		"peer", pc.config.NetworkName,
		"remote_subscriber", remoteSubscriber,
		"local_user", localUser,
	)

	m.presenceSubsMu.Lock()
	if subs, ok := m.presenceSubs[localUser]; ok {
		delete(subs, remoteSubscriber)
		if len(subs) == 0 {
			delete(m.presenceSubs, localUser)
		}
	}
	m.presenceSubsMu.Unlock()
}

// handleFedPresenceNotify processes an inbound presence notification from a peer.
func (m *Manager) handleFedPresenceNotify(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x0009_FedPresenceNotify
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		m.logger.Error("failed to unmarshal presence notify", "err", err)
		return
	}

	// The remote user's full federated name
	remoteUser := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.ScreenName),
		pc.config.NetworkName,
	)

	m.logger.Debug("received presence notify",
		"peer", pc.config.NetworkName,
		"remote_user", remoteUser,
		"online", msg.Online == 1,
	)

	m.deliverPresenceNotification(ctx, remoteUser, msg.Online == 1)
}

// handleFedPresenceSubscribeAck processes the ack for a presence subscription
// which includes the current online status.
func (m *Manager) handleFedPresenceSubscribeAck(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x000C_FedPresenceSubscribeAck
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		m.logger.Error("failed to unmarshal presence subscribe ack", "err", err)
		return
	}

	remoteUser := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.ScreenName),
		pc.config.NetworkName,
	)

	m.logger.Debug("received presence subscribe ack",
		"peer", pc.config.NetworkName,
		"remote_user", remoteUser,
		"online", msg.Online == 1,
	)

	m.deliverPresenceNotification(ctx, remoteUser, msg.Online == 1)
}

// deliverPresenceNotification sends buddy arrived/departed to all local users
// who have the given remote user on their buddy list.
func (m *Manager) deliverPresenceNotification(ctx context.Context, remoteUser state.IdentScreenName, online bool) {
	sessions := m.allSessionRetriever.AllSessions()

	m.logger.Debug("delivering presence notification to local users",
		"remote_user", remoteUser,
		"online", online,
		"local_session_count", len(sessions),
	)

	for _, sess := range sessions {
		localUser := sess.IdentScreenName()
		feedbag, err := m.feedbagRetriever.Feedbag(ctx, localUser)
		if err != nil {
			m.logger.Debug("failed to fetch feedbag for presence delivery",
				"local_user", localUser,
				"err", err,
			)
			continue
		}
		hasBuddy := false
		for _, item := range feedbag {
			if item.ClassID == wire.FeedbagClassIdBuddy {
				buddyName := state.NewIdentScreenName(item.Name)
				if buddyName == remoteUser {
					hasBuddy = true
					break
				}
			}
		}
		if !hasBuddy {
			m.logger.Debug("local user does not have remote buddy in feedbag",
				"local_user", localUser,
				"remote_user", remoteUser,
			)
			continue
		}

		m.logger.Debug("sending buddy presence to local user",
			"local_user", localUser,
			"remote_user", remoteUser,
			"online", online,
		)

		if online {
			m.messageRelayer.RelayToScreenName(ctx, localUser, wire.SNACMessage{
				Frame: wire.SNACFrame{
					FoodGroup: wire.Buddy,
					SubGroup:  wire.BuddyArrived,
					RequestID: wire.ReqIDFromServer,
				},
				Body: wire.SNAC_0x03_0x0B_BuddyArrived{
					TLVUserInfo: wire.TLVUserInfo{
						ScreenName:   remoteUser.String(),
						WarningLevel: 0,
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
			m.messageRelayer.RelayToScreenName(ctx, localUser, wire.SNACMessage{
				Frame: wire.SNACFrame{
					FoodGroup: wire.Buddy,
					SubGroup:  wire.BuddyDeparted,
					RequestID: wire.ReqIDFromServer,
				},
				Body: wire.SNAC_0x03_0x0C_BuddyDeparted{
					TLVUserInfo: wire.TLVUserInfo{
						ScreenName:   remoteUser.String(),
						WarningLevel: 0,
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

// RouteMessage routes an instant message to a federated user on a remote server.
func (m *Manager) RouteMessage(ctx context.Context, instance *state.SessionInstance, inFrame wire.SNACFrame, inBody wire.SNAC_0x04_0x06_ICBMChannelMsgToHost) (*wire.SNACMessage, error) {
	recip := state.NewIdentScreenName(inBody.ScreenName)
	network := recip.Network()

	m.logger.Debug("send FedMessage",
		"from", instance.IdentScreenName(),
		"to", recip,
		"network", network,
	)

	m.mu.RLock()
	pc, ok := m.peers[network]
	m.mu.RUnlock()

	if !ok {
		m.logger.Debug("federation route failed: unknown network",
			"network", network,
		)
		return newICBMErr(inFrame.RequestID, wire.ErrorCodeNotLoggedOn), nil
	}

	pc.mu.Lock()
	connected := pc.connected
	pc.mu.Unlock()

	if !connected {
		m.logger.Debug("federation route failed: peer not connected",
			"peer", network,
		)
		return newICBMErr(inFrame.RequestID, wire.ErrorCodeNotLoggedOn), nil
	}

	// Build federation message
	fedMsg := wire.SNAC_0x0100_0x0004_FedMessage{
		Cookie:    inBody.Cookie,
		FromUser:  instance.IdentScreenName().String(),
		ToUser:    recip.LocalPart().String(),
		ChannelID: inBody.ChannelID,
	}
	for _, tlv := range inBody.TLVRestBlock.TLVList {
		if tlv.Tag == wire.ICBMTLVRequestHostAck {
			continue // don't forward ack requests
		}
		fedMsg.Append(tlv)
	}

	// Send via the peer's send channel
	select {
	case pc.sendCh <- wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedMessage,
		},
		Body: fedMsg,
	}:
	default:
		return newICBMErr(inFrame.RequestID, wire.ErrorCodeServiceUnavailable), nil
	}

	// Return ack to sender if requested
	if _, requestedAck := inBody.TLVRestBlock.Bytes(wire.ICBMTLVRequestHostAck); requestedAck {
		return &wire.SNACMessage{
			Frame: wire.SNACFrame{
				FoodGroup: wire.ICBM,
				SubGroup:  wire.ICBMHostAck,
				RequestID: inFrame.RequestID,
			},
			Body: wire.SNAC_0x04_0x0C_ICBMHostAck{
				Cookie:     inBody.Cookie,
				ChannelID:  inBody.ChannelID,
				ScreenName: inBody.ScreenName,
			},
		}, nil
	}

	return nil, nil
}

// RouteTypingEvent routes a typing notification to a federated user.
func (m *Manager) RouteTypingEvent(_ context.Context, instance *state.SessionInstance, inFrame wire.SNACFrame, inBody wire.SNAC_0x04_0x14_ICBMClientEvent) error {
	recip := state.NewIdentScreenName(inBody.ScreenName)
	network := recip.Network()

	m.logger.Debug("send FedTypingEvent",
		"from", instance.IdentScreenName(),
		"to", recip,
		"network", network,
	)

	m.mu.RLock()
	pc, ok := m.peers[network]
	m.mu.RUnlock()

	if !ok {
		m.logger.Debug("federation typing event dropped: unknown network", "network", network)
		return nil
	}

	pc.mu.Lock()
	connected := pc.connected
	pc.mu.Unlock()

	if !connected {
		m.logger.Debug("federation typing event dropped: peer not connected", "peer", network)
		return nil
	}

	select {
	case pc.sendCh <- wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedTypingEvent,
		},
		Body: wire.SNAC_0x0100_0x000A_FedTypingEvent{
			Cookie:    inBody.Cookie,
			FromUser:  instance.IdentScreenName().String(),
			ToUser:    recip.LocalPart().String(),
			ChannelID: inBody.ChannelID,
			Event:     inBody.Event,
		},
	}:
	default:
		// drop if queue is full
	}

	return nil
}

// QueryUserInfo sends a FedUserInfoQuery to the remote server hosting
// remoteUser and blocks until a FedUserInfoReply is received or the request
// times out.
func (m *Manager) QueryUserInfo(ctx context.Context, remoteUser state.IdentScreenName, requestType uint16) (*wire.SNAC_0x0100_0x000E_FedUserInfoReply, error) {
	network := remoteUser.Network()

	m.logger.Debug("send FedUserInfoQuery",
		"remote_user", remoteUser,
		"network", network,
		"type", requestType,
	)

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

	select {
	case pc.sendCh <- wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedUserInfoQuery,
		},
		Body: wire.SNAC_0x0100_0x000D_FedUserInfoQuery{
			Cookie: cookie,
			ToUser: remoteUser.LocalPart().String(),
			Type:   requestType,
		},
	}:
	default:
		return nil, fmt.Errorf("send channel full for peer: %s", network)
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

// handleFedUserInfoQuery processes an inbound user info query from a peer.
// It looks up the requested local user's profile and away message and sends
// a FedUserInfoReply back to the requesting peer.
func (m *Manager) handleFedUserInfoQuery(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var query wire.SNAC_0x0100_0x000D_FedUserInfoQuery
	if err := wire.UnmarshalBE(&query, r); err != nil {
		m.logger.Error("failed to unmarshal FedUserInfoQuery", "err", err)
		return
	}

	localUser := state.NewIdentScreenName(query.ToUser)

	m.logger.Debug("recv FedUserInfoQuery",
		"peer", pc.config.NetworkName,
		"lookup_user", localUser,
		"type", query.Type,
	)

	reply := wire.SNAC_0x0100_0x000E_FedUserInfoReply{
		Cookie:     query.Cookie,
		ScreenName: localUser.String(),
	}

	sess := m.sessionRetriever.RetrieveSession(localUser)

	requestProfile := query.Type&uint16(wire.LocateTypeSig) != 0
	requestAway := query.Type&uint16(wire.LocateTypeUnavailable) != 0

	if requestProfile {
		var prof state.UserProfile
		if sess != nil {
			prof = sess.Profile()
		}
		// Fall back to server-side profile if session profile is empty
		if prof.ProfileText == "" && m.profileRetriever != nil {
			if serverProf, err := m.profileRetriever.Profile(ctx, localUser); err == nil {
				prof = serverProf
			}
		}
		reply.Append(wire.NewTLVBE(wire.LocateTLVTagsInfoSigMime, prof.MIMEType))
		reply.Append(wire.NewTLVBE(wire.LocateTLVTagsInfoSigData, prof.ProfileText))
	}

	if requestAway && sess != nil && sess.Away() {
		reply.Append(wire.NewTLVBE(wire.LocateTLVTagsInfoUnavailableMime, `text/aolrtf; charset="us-ascii"`))
		reply.Append(wire.NewTLVBE(wire.LocateTLVTagsInfoUnavailableData, sess.AwayMessage()))
	}

	if sess != nil {
		// Extract user flags from session
		for _, tlv := range sess.TLVUserInfo().TLVList {
			if tlv.Tag == wire.OServiceUserInfoUserFlags && len(tlv.Value) >= 2 {
				reply.Flags = uint16(tlv.Value[0])<<8 | uint16(tlv.Value[1])
				break
			}
		}
	}

	select {
	case pc.sendCh <- wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedUserInfoReply,
		},
		Body: reply,
	}:
	default:
		m.logger.Warn("failed to send FedUserInfoReply: send channel full",
			"peer", pc.config.NetworkName,
		)
	}
}

// handleFedUserInfoReply processes an inbound user info reply from a peer,
// delivering it to the goroutine waiting on the matching cookie.
func (m *Manager) handleFedUserInfoReply(pc *PeerConnection, r io.Reader) {
	var reply wire.SNAC_0x0100_0x000E_FedUserInfoReply
	if err := wire.UnmarshalBE(&reply, r); err != nil {
		m.logger.Error("failed to unmarshal FedUserInfoReply", "err", err)
		return
	}

	m.logger.Debug("recv FedUserInfoReply",
		"peer", pc.config.NetworkName,
		"screen_name", reply.ScreenName,
		"cookie", reply.Cookie,
	)

	m.pendingUserInfoMu.Lock()
	pending, ok := m.pendingUserInfo[reply.Cookie]
	m.pendingUserInfoMu.Unlock()

	if !ok {
		m.logger.Warn("received FedUserInfoReply with unknown cookie",
			"peer", pc.config.NetworkName,
			"cookie", reply.Cookie,
		)
		return
	}

	select {
	case pending.replyCh <- reply:
	default:
	}
}

// SubscribePresence subscribes a local user to a remote user's presence.
func (m *Manager) SubscribePresence(ctx context.Context, localUser state.IdentScreenName, remoteUser state.IdentScreenName) error {
	network := remoteUser.Network()
	if network == "" {
		return nil // not a federated user
	}

	m.logger.Debug("send FedPresenceSubscribe",
		"local_user", localUser,
		"remote_user", remoteUser,
		"network", network,
	)

	m.mu.RLock()
	pc, ok := m.peers[network]
	m.mu.RUnlock()

	if !ok {
		m.logger.Debug("presence subscribe skipped: unknown network",
			"network", network,
		)
		return nil // unknown network
	}

	pc.mu.Lock()
	connected := pc.connected
	pc.mu.Unlock()

	if !connected {
		m.logger.Debug("presence subscribe deferred: peer not connected",
			"peer", network,
		)
		return nil // will re-subscribe on reconnect
	}

	select {
	case pc.sendCh <- wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedPresenceSubscribe,
		},
		Body: wire.SNAC_0x0100_0x0007_FedPresenceSubscribe{
			FromUser: localUser.String(),
			ToUser:   remoteUser.LocalPart().String(),
		},
	}:
	default:
	}

	return nil
}

// UnsubscribePresence unsubscribes a local user from a remote user's presence.
func (m *Manager) UnsubscribePresence(ctx context.Context, localUser state.IdentScreenName, remoteUser state.IdentScreenName) error {
	network := remoteUser.Network()
	if network == "" {
		return nil
	}

	m.logger.Debug("send FedPresenceUnsubscribe",
		"local_user", localUser,
		"remote_user", remoteUser,
		"network", network,
	)

	m.mu.RLock()
	pc, ok := m.peers[network]
	m.mu.RUnlock()

	if !ok {
		m.logger.Debug("presence unsubscribe skipped: unknown network", "network", network)
		return nil
	}

	pc.mu.Lock()
	connected := pc.connected
	pc.mu.Unlock()

	if !connected {
		m.logger.Debug("presence unsubscribe deferred: peer not connected", "peer", network)
		return nil
	}

	select {
	case pc.sendCh <- wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedPresenceUnsubscribe,
		},
		Body: wire.SNAC_0x0100_0x0008_FedPresenceUnsubscribe{
			FromUser: localUser.String(),
			ToUser:   remoteUser.LocalPart().String(),
		},
	}:
	default:
	}

	return nil
}

// NotifyPresence notifies subscribing federation peers that a local user has
// come online or gone offline.
func (m *Manager) NotifyPresence(ctx context.Context, screenName state.IdentScreenName, online bool) error {
	m.presenceSubsMu.RLock()
	subscribers, ok := m.presenceSubs[screenName]
	m.presenceSubsMu.RUnlock()

	if !ok || len(subscribers) == 0 {
		m.logger.Debug("send FedPresenceNotify: no remote subscribers",
			"local_user", screenName,
			"online", online,
		)
		return nil
	}

	m.logger.Debug("send FedPresenceNotify",
		"local_user", screenName,
		"online", online,
		"subscriber_count", len(subscribers),
	)

	// Group subscribers by network so we send one notification per peer
	networkPeers := make(map[string]bool)
	for sub := range subscribers {
		network := sub.Network()
		if network != "" {
			networkPeers[network] = true
		}
	}

	onlineFlag := uint8(0)
	if online {
		onlineFlag = 1
	}

	for network := range networkPeers {
		m.mu.RLock()
		pc, ok := m.peers[network]
		m.mu.RUnlock()
		if !ok {
			continue
		}

		pc.mu.Lock()
		connected := pc.connected
		pc.mu.Unlock()
		if !connected {
			continue
		}

		select {
		case pc.sendCh <- wire.SNACMessage{
			Frame: wire.SNACFrame{
				FoodGroup: wire.Federation,
				SubGroup:  wire.FedPresenceNotify,
			},
			Body: wire.SNAC_0x0100_0x0009_FedPresenceNotify{
				ScreenName: screenName.String(),
				Online:     onlineFlag,
			},
		}:
		default:
		}
	}

	return nil
}

// resubscribePresence re-sends presence subscriptions for all local users who
// have buddies on the given peer's network.
func (m *Manager) resubscribePresence(ctx context.Context, pc *PeerConnection) {
	sessions := m.allSessionRetriever.AllSessions()
	m.logger.Debug("resubscribing presence on peer reconnect",
		"peer", pc.config.NetworkName,
		"local_session_count", len(sessions),
	)
	subCount := 0
	for _, sess := range sessions {
		localUser := sess.IdentScreenName()
		feedbag, err := m.feedbagRetriever.Feedbag(ctx, localUser)
		if err != nil {
			continue
		}
		for _, item := range feedbag {
			if item.ClassID == wire.FeedbagClassIdBuddy {
				buddyName := state.NewIdentScreenName(item.Name)
				if buddyName.Network() == pc.config.NetworkName {
					m.logger.Debug("resubscribing presence",
						"local_user", localUser,
						"remote_buddy", buddyName,
					)
					subCount++
					select {
					case pc.sendCh <- wire.SNACMessage{
						Frame: wire.SNACFrame{
							FoodGroup: wire.Federation,
							SubGroup:  wire.FedPresenceSubscribe,
						},
						Body: wire.SNAC_0x0100_0x0007_FedPresenceSubscribe{
							FromUser: localUser.String(),
							ToUser:   buddyName.LocalPart().String(),
						},
					}:
					default:
					}
				}
			}
		}
	}
	m.logger.Debug("resubscribe complete",
		"peer", pc.config.NetworkName,
		"subscriptions_sent", subCount,
	)
}

// RegisterInboundPeer registers an authenticated inbound peer connection.
// This is called by the federation server when a peer connects to us.
func (m *Manager) RegisterInboundPeer(ctx context.Context, networkName string, conn net.Conn, flapc *wire.FlapClient) error {
	m.mu.Lock()
	pc, ok := m.peers[networkName]
	m.mu.Unlock()

	if !ok {
		return fmt.Errorf("unknown peer network: %s", networkName)
	}

	// If we already have an active connection, use duplicate resolution.
	// Convention: the lexicographically smaller network name keeps its
	// outbound connection; the larger name uses the inbound instead.
	pc.mu.Lock()
	if pc.connected {
		if m.localNetwork < networkName {
			pc.mu.Unlock()
			m.logger.Debug("rejecting duplicate inbound connection (keeping outbound)",
				"peer", networkName,
			)
			return fmt.Errorf("duplicate connection: keeping outbound")
		}
		// We are the lex-larger name. Close our outbound and use this inbound.
		m.logger.Info("replacing outbound with inbound connection",
			"peer", networkName,
		)
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
		"peer", networkName,
		"direction", "inbound",
	)

	// Re-subscribe to presence
	m.resubscribePresence(peerCtx, pc)

	// Run read and write loops
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer cancel()
		m.writeLoop(peerCtx, pc, flapc)
	}()

	go func() {
		defer wg.Done()
		defer cancel()
		m.readLoop(peerCtx, pc, flapc)
	}()

	wg.Wait()

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

func (m *Manager) sendMessageAck(pc *PeerConnection, cookie uint64) {
	select {
	case pc.sendCh <- wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedMessageAck,
		},
		Body: wire.SNAC_0x0100_0x0005_FedMessageAck{
			Cookie: cookie,
		},
	}:
	default:
	}
}

func (m *Manager) sendMessageErr(pc *PeerConnection, cookie uint64, code uint16) {
	select {
	case pc.sendCh <- wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedMessageErr,
		},
		Body: wire.SNAC_0x0100_0x0006_FedMessageErr{
			Cookie: cookie,
			Code:   code,
		},
	}:
	default:
	}
}

// RouteEvil routes a warning (evil) request to a federated user.
// RouteEvil routes a warning (evil) request to a federated user and blocks
// until the remote server replies with the result.
func (m *Manager) RouteEvil(ctx context.Context, instance *state.SessionInstance, inFrame wire.SNACFrame, inBody wire.SNAC_0x04_0x08_ICBMEvilRequest) (wire.SNACMessage, error) {
	recip := state.NewIdentScreenName(inBody.ScreenName)
	network := recip.Network()

	m.logger.Debug("send FedEvilRequest",
		"from", instance.IdentScreenName(),
		"to", recip,
		"network", network,
	)

	m.mu.RLock()
	pc, ok := m.peers[network]
	m.mu.RUnlock()

	if !ok {
		m.logger.Debug("federation route failed: unknown network",
			"network", network,
		)
		return *newICBMErr(inFrame.RequestID, wire.ErrorCodeNotLoggedOn), nil
	}

	pc.mu.Lock()
	connected := pc.connected
	pc.mu.Unlock()

	if !connected {
		m.logger.Debug("federation route failed: peer not connected",
			"peer", network,
		)
		return *newICBMErr(inFrame.RequestID, wire.ErrorCodeNotLoggedOn), nil
	}

	cookie := m.cookieCounter.Add(1)

	pending := &evilPendingRequest{
		replyCh: make(chan wire.SNAC_0x0100_0x0010_FedEvilReply, 1),
	}
	m.pendingEvilMu.Lock()
	m.pendingEvil[cookie] = pending
	m.pendingEvilMu.Unlock()

	defer func() {
		m.pendingEvilMu.Lock()
		delete(m.pendingEvil, cookie)
		m.pendingEvilMu.Unlock()
	}()

	select {
	case pc.sendCh <- wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedEvilRequest,
		},
		Body: wire.SNAC_0x0100_0x000F_FedEvilRequest{
			Cookie:   cookie,
			FromUser: instance.IdentScreenName().String(),
			ToUser:   recip.LocalPart().String(),
			SendAs:   inBody.SendAs,
		},
	}:
	default:
		return *newICBMErr(inFrame.RequestID, wire.ErrorCodeServiceUnavailable), nil
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, userInfoQueryTimeout)
	defer cancel()

	select {
	case reply := <-pending.replyCh:
		if reply.ErrorCode != 0 {
			return *newICBMErr(inFrame.RequestID, reply.ErrorCode), nil
		}
		return wire.SNACMessage{
			Frame: wire.SNACFrame{
				FoodGroup: wire.ICBM,
				SubGroup:  wire.ICBMEvilReply,
				RequestID: inFrame.RequestID,
			},
			Body: wire.SNAC_0x04_0x09_ICBMEvilReply{
				EvilDeltaApplied: reply.EvilDeltaApplied,
				UpdatedEvilValue: reply.UpdatedEvilValue,
			},
		}, nil
	case <-timeoutCtx.Done():
		m.logger.Debug("federation evil request timed out",
			"to", recip,
			"network", network,
		)
		return *newICBMErr(inFrame.RequestID, wire.ErrorCodeNotLoggedOn), nil
	}
}

// handleFedEvilRequest processes an inbound warning (evil) request from a federation peer.
func (m *Manager) handleFedEvilRequest(ctx context.Context, pc *PeerConnection, r io.Reader) {
	var msg wire.SNAC_0x0100_0x000F_FedEvilRequest
	if err := wire.UnmarshalBE(&msg, r); err != nil {
		m.logger.Error("failed to unmarshal federation evil request", "err", err)
		return
	}

	federatedSender := state.NewFederatedIdentScreenName(
		state.NewIdentScreenName(msg.FromUser),
		pc.config.NetworkName,
	)
	localRecipient := state.NewIdentScreenName(msg.ToUser)

	m.logger.Debug("recv FedEvilRequest",
		"peer", pc.config.NetworkName,
		"from", federatedSender,
		"to", localRecipient,
		"sendAs", msg.SendAs,
	)

	recipSess := m.sessionRetriever.RetrieveSession(localRecipient)
	if recipSess == nil {
		m.logger.Debug("federated evil request recipient offline", "to", localRecipient)
		m.sendEvilReply(pc, msg.Cookie, 0, 0, wire.ErrorCodeNotLoggedOn)
		return
	}

	// Check if the local user has blocked the federated sender
	rel, err := m.relationshipFetcher.Relationship(ctx, localRecipient, federatedSender)
	if err != nil {
		m.logger.Error("failed to check relationship for federation evil request", "err", err)
		m.sendEvilReply(pc, msg.Cookie, 0, 0, wire.ErrorCodeGeneralFailure)
		return
	}
	if rel.YouBlock {
		m.logger.Debug("federated evil request blocked by recipient",
			"from", federatedSender,
			"to", localRecipient,
		)
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
		notif.Snitcher = &struct {
			wire.TLVUserInfo
		}{
			TLVUserInfo: wire.TLVUserInfo{
				ScreenName: federatedSender.String(),
			},
		}
	}

	m.messageRelayer.RelayToScreenName(ctx, localRecipient, wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.OService,
			SubGroup:  wire.OServiceEvilNotification,
		},
		Body: notif,
	})

	m.sendEvilReply(pc, msg.Cookie, uint16(increase), uint16(newWarning), 0)

	m.logger.Debug("applied federated evil request to local user",
		"from", federatedSender,
		"to", localRecipient,
		"newWarning", newWarning,
	)
}

// handleFedEvilReply processes an inbound evil reply from a peer, delivering
// it to the goroutine waiting on the matching cookie.
func (m *Manager) handleFedEvilReply(pc *PeerConnection, r io.Reader) {
	var reply wire.SNAC_0x0100_0x0010_FedEvilReply
	if err := wire.UnmarshalBE(&reply, r); err != nil {
		m.logger.Error("failed to unmarshal FedEvilReply", "err", err)
		return
	}

	m.logger.Debug("recv FedEvilReply",
		"peer", pc.config.NetworkName,
		"cookie", reply.Cookie,
		"delta", reply.EvilDeltaApplied,
		"newLevel", reply.UpdatedEvilValue,
		"errorCode", reply.ErrorCode,
	)

	m.pendingEvilMu.Lock()
	pending, ok := m.pendingEvil[reply.Cookie]
	m.pendingEvilMu.Unlock()

	if !ok {
		m.logger.Warn("received FedEvilReply with unknown cookie",
			"peer", pc.config.NetworkName,
			"cookie", reply.Cookie,
		)
		return
	}

	select {
	case pending.replyCh <- reply:
	default:
	}
}

// sendEvilReply sends a FedEvilReply back to the requesting peer.
func (m *Manager) sendEvilReply(pc *PeerConnection, cookie uint64, deltaApplied uint16, updatedLevel uint16, errorCode uint16) {
	select {
	case pc.sendCh <- wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedEvilReply,
		},
		Body: wire.SNAC_0x0100_0x0010_FedEvilReply{
			Cookie:           cookie,
			EvilDeltaApplied: deltaApplied,
			UpdatedEvilValue: updatedLevel,
			ErrorCode:        errorCode,
		},
	}:
	default:
	}
}

func newICBMErr(requestID uint32, errCode uint16) *wire.SNACMessage {
	return &wire.SNACMessage{
		Frame: wire.SNACFrame{
			FoodGroup: wire.ICBM,
			SubGroup:  wire.ICBMErr,
			RequestID: requestID,
		},
		Body: wire.SNACError{
			Code: errCode,
		},
	}
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
