package federation

import (
	"context"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

// Transport defines the outbound federation operations used by the decorator
// layer. The Manager implements this interface.
type Transport interface {
	// RouteToRemote sends a SNAC message to a remote user on a federated
	// network. It inspects the SNAC type to construct the appropriate
	// federation wire message (FedMessage, FedPresenceNotify, FedTypingEvent).
	RouteToRemote(ctx context.Context, recipient state.IdentScreenName, msg wire.SNACMessage) error

	// SubscribePresence requests presence notifications for a remote user.
	// localUser is the local user subscribing; remoteUser is on the peer server.
	SubscribePresence(ctx context.Context, localUser, remoteUser state.IdentScreenName) error

	// UnsubscribePresence cancels presence notifications for a remote user.
	UnsubscribePresence(ctx context.Context, localUser, remoteUser state.IdentScreenName) error

	// NotifyPresenceToSubscribers notifies all remote peers that have
	// subscribed to this local user's presence.
	NotifyPresenceToSubscribers(ctx context.Context, localUser state.IdentScreenName, online bool, userInfo wire.TLVUserInfo) error

	// QueryUserInfo queries a remote server for a user's profile/away message.
	// requestType is a bitmask of LocateType* constants (uint32 in the wire
	// spec, but only the low 16 bits are used in the federation protocol).
	QueryUserInfo(ctx context.Context, remoteUser state.IdentScreenName, requestType uint32) (*wire.SNAC_0x0100_0x000E_FedUserInfoReply, error)

	// NotifySessionUpdate pushes a local user's current session data to all
	// connected federation peers. Called when session data changes (profile,
	// away message, idle, warning).
	NotifySessionUpdate(ctx context.Context, screenName state.IdentScreenName) error
}
