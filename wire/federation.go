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
