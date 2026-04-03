package wire

//
// Federation Food Group (0x0100) - Inter-server federation protocol
//

const (
	Federation uint16 = 0x0100
)

//
// Federation SubGroup codes
//

const (
	FedAuthRequest          uint16 = 0x0001
	FedAuthResponse         uint16 = 0x0002
	FedAuthResult           uint16 = 0x0003
	FedMessage              uint16 = 0x0004
	FedMessageAck           uint16 = 0x0005
	FedMessageErr           uint16 = 0x0006
	FedPresenceSubscribe    uint16 = 0x0007
	FedPresenceUnsubscribe  uint16 = 0x0008
	FedPresenceNotify       uint16 = 0x0009
	FedTypingEvent          uint16 = 0x000A
	FedKeepAlive            uint16 = 0x000B
	FedPresenceSubscribeAck uint16 = 0x000C
	FedUserInfoQuery        uint16 = 0x000D
	FedUserInfoReply        uint16 = 0x000E
	FedEvilRequest          uint16 = 0x000F
	FedEvilReply            uint16 = 0x0010
	FedGossipDigest         uint16 = 0x0011
	FedGossipDigestAck      uint16 = 0x0012
	FedGossipDigestAck2     uint16 = 0x0013
	FedForward              uint16 = 0x0014
)

//
// Federation TLV tags
//

const (
	FedTLVNetworkName uint16 = 0x0001
	FedTLVChallenge   uint16 = 0x0002
	FedTLVDigest      uint16 = 0x0003
	FedTLVVersion     uint16 = 0x0004
	FedTLVErrorCode   uint16 = 0x0005
)

//
// Federation auth result codes
//

const (
	FedAuthResultSuccess uint16 = 0x0000
	FedAuthResultFailed  uint16 = 0x0001
	FedAuthResultUnknown uint16 = 0x0002
)

//
// Federation SNAC message types
//

// SNAC_0x0100_0x0001_FedAuthRequest is sent during federation handshake to
// initiate authentication. Contains the requesting server's network name and
// an HMAC challenge.
type SNAC_0x0100_0x0001_FedAuthRequest struct {
	NetworkName string `oscar:"len_prefix=uint16"`
	Challenge   [32]byte
	Version     uint16
}

// SNAC_0x0100_0x0002_FedAuthResponse is sent in response to FedAuthRequest,
// containing the HMAC-SHA256 digest computed over the challenge and shared secret.
type SNAC_0x0100_0x0002_FedAuthResponse struct {
	NetworkName string `oscar:"len_prefix=uint16"`
	Digest      [32]byte
}

// SNAC_0x0100_0x0003_FedAuthResult indicates the result of federation authentication.
type SNAC_0x0100_0x0003_FedAuthResult struct {
	Code uint16
}

// SNAC_0x0100_0x0004_FedMessage relays an instant message from a user on the
// sending server to a user on the receiving server.
type SNAC_0x0100_0x0004_FedMessage struct {
	Cookie    uint64
	FromUser  string `oscar:"len_prefix=uint8"`
	ToUser    string `oscar:"len_prefix=uint8"`
	ChannelID uint16
	TLVRestBlock
}

// SNAC_0x0100_0x0005_FedMessageAck acknowledges successful delivery of a
// federated message.
type SNAC_0x0100_0x0005_FedMessageAck struct {
	Cookie uint64
}

// SNAC_0x0100_0x0006_FedMessageErr indicates failure to deliver a federated message.
type SNAC_0x0100_0x0006_FedMessageErr struct {
	Cookie uint64
	Code   uint16
}

// SNAC_0x0100_0x0007_FedPresenceSubscribe requests presence notifications for
// a remote user. FromUser is the local user who wants to watch; ToUser is the
// user on the remote server.
type SNAC_0x0100_0x0007_FedPresenceSubscribe struct {
	FromUser string `oscar:"len_prefix=uint8"`
	ToUser   string `oscar:"len_prefix=uint8"`
}

// SNAC_0x0100_0x0008_FedPresenceUnsubscribe cancels a presence subscription.
type SNAC_0x0100_0x0008_FedPresenceUnsubscribe struct {
	FromUser string `oscar:"len_prefix=uint8"`
	ToUser   string `oscar:"len_prefix=uint8"`
}

// SNAC_0x0100_0x0009_FedPresenceNotify sends a buddy arrival or departure
// notification for a user to a subscribing peer.
type SNAC_0x0100_0x0009_FedPresenceNotify struct {
	ScreenName string `oscar:"len_prefix=uint8"`
	Online     uint8  // 1 = arrived, 0 = departed
	TLVRestBlock
}

// SNAC_0x0100_0x000A_FedTypingEvent relays a typing notification from a local
// user to a remote user.
type SNAC_0x0100_0x000A_FedTypingEvent struct {
	Cookie    uint64
	FromUser  string `oscar:"len_prefix=uint8"`
	ToUser    string `oscar:"len_prefix=uint8"`
	ChannelID uint16
	Event     uint16
}

// SNAC_0x0100_0x000B_FedKeepAlive is a keepalive ping/pong for the federation link.
type SNAC_0x0100_0x000B_FedKeepAlive struct{}

// SNAC_0x0100_0x000C_FedPresenceSubscribeAck acknowledges a presence
// subscription with the current online status.
type SNAC_0x0100_0x000C_FedPresenceSubscribeAck struct {
	ScreenName string `oscar:"len_prefix=uint8"`
	Online     uint8
	TLVRestBlock
}

// SNAC_0x0100_0x000D_FedUserInfoQuery requests profile and/or away message
// data for a user on the remote server. Cookie correlates the reply.
type SNAC_0x0100_0x000D_FedUserInfoQuery struct {
	Cookie uint64
	ToUser string `oscar:"len_prefix=uint8"`
	Type   uint16 // bitmask: LocateTypeSig, LocateTypeUnavailable
}

// SNAC_0x0100_0x000E_FedUserInfoReply returns profile and/or away message
// data for a user in response to a FedUserInfoQuery.
type SNAC_0x0100_0x000E_FedUserInfoReply struct {
	Cookie     uint64
	ScreenName string `oscar:"len_prefix=uint8"`
	Flags      uint16
	TLVRestBlock
}

// SNAC_0x0100_0x000F_FedEvilRequest relays a warning (evil) request from a
// user on the sending server to a user on the receiving server.
type SNAC_0x0100_0x000F_FedEvilRequest struct {
	Cookie   uint64
	FromUser string `oscar:"len_prefix=uint8"`
	ToUser   string `oscar:"len_prefix=uint8"`
	SendAs   uint16 // 0 = identified, 1 = anonymous
}

// SNAC_0x0100_0x0010_FedEvilReply is sent by the remote server in response to
// a FedEvilRequest, containing the result of the warning operation.
type SNAC_0x0100_0x0010_FedEvilReply struct {
	Cookie           uint64
	EvilDeltaApplied uint16
	UpdatedEvilValue uint16
	ErrorCode        uint16 // 0 = success
}

//
// Gossip protocol messages
//

// FedNetworkDigest is a compact summary of a known network's state version,
// used in gossip digest exchanges.
type FedNetworkDigest struct {
	NetworkName string `oscar:"len_prefix=uint16"`
	MaxVersion  uint64
	IsAlive     uint8 // 1 = alive, 0 = suspected dead
}

// FedNetworkState is the full state of a known network, exchanged during
// gossip reconciliation.
type FedNetworkState struct {
	NetworkName string `oscar:"len_prefix=uint16"`
	Version     uint64
	IsAlive     uint8
	PeerList    []FedNetworkPeer `oscar:"count_prefix=uint16"`
	UserCount   uint32
}

// FedNetworkPeer is a single entry in a network's peer list.
type FedNetworkPeer struct {
	NetworkName string `oscar:"len_prefix=uint16"`
}

// SNAC_0x0100_0x0011_FedGossipDigest is sent periodically to random peers.
// Contains a compact version vector of all known networks.
type SNAC_0x0100_0x0011_FedGossipDigest struct {
	Generation uint64
	Digests    []FedNetworkDigest `oscar:"count_prefix=uint16"`
}

// SNAC_0x0100_0x0012_FedGossipDigestAck is sent in response to a gossip
// digest. Contains full state for entries where the digest sender is behind,
// and a list of entries where the responder needs updates.
type SNAC_0x0100_0x0012_FedGossipDigestAck struct {
	Updates  []FedNetworkState  `oscar:"count_prefix=uint16"`
	NeedFrom []FedNetworkDigest `oscar:"count_prefix=uint16"`
}

// SNAC_0x0100_0x0013_FedGossipDigestAck2 completes the three-way gossip
// handshake. Contains state for entries the original responder needed.
type SNAC_0x0100_0x0013_FedGossipDigestAck2 struct {
	Updates []FedNetworkState `oscar:"count_prefix=uint16"`
}

//
// Multi-hop forwarding
//

// SNAC_0x0100_0x0014_FedForward wraps any federation SNAC for multi-hop
// transit through intermediate servers. The InnerSNAC field contains a
// serialized SNACFrame + body that is unwrapped at the destination.
type SNAC_0x0100_0x0014_FedForward struct {
	OriginNetwork string `oscar:"len_prefix=uint16"`
	TargetNetwork string `oscar:"len_prefix=uint16"`
	TTL           uint8
	InnerSNAC     []byte `oscar:"len_prefix=uint16"`
}
