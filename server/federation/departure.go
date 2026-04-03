package federation

import (
	"context"
	"log/slog"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

// DepartureNotifier is the interface for broadcasting buddy presence changes.
type DepartureNotifier interface {
	BroadcastBuddyArrived(ctx context.Context, screenName state.IdentScreenName, userInfo wire.TLVUserInfo) error
	BroadcastBuddyDeparted(ctx context.Context, screenName state.IdentScreenName) error
}

// FederatedDepartureNotifier wraps a local DepartureNotifier and additionally
// notifies federation peers about local user presence changes.
type FederatedDepartureNotifier struct {
	local        DepartureNotifier
	transport    Transport
	localNetwork string
	logger       *slog.Logger
}

// NewFederatedDepartureNotifier creates a new FederatedDepartureNotifier.
func NewFederatedDepartureNotifier(
	local DepartureNotifier,
	transport Transport,
	localNetwork string,
	logger *slog.Logger,
) *FederatedDepartureNotifier {
	return &FederatedDepartureNotifier{
		local:        local,
		transport:    transport,
		localNetwork: localNetwork,
		logger:       logger,
	}
}

func (f *FederatedDepartureNotifier) BroadcastBuddyArrived(ctx context.Context, screenName state.IdentScreenName, userInfo wire.TLVUserInfo) error {
	if err := f.local.BroadcastBuddyArrived(ctx, screenName, userInfo); err != nil {
		return err
	}
	if err := f.transport.NotifyPresenceToSubscribers(ctx, screenName, true, userInfo); err != nil {
		f.logger.ErrorContext(ctx, "failed to notify federation peers of buddy arrival",
			"screen_name", screenName,
			"err", err,
		)
	}
	return nil
}

func (f *FederatedDepartureNotifier) BroadcastBuddyDeparted(ctx context.Context, screenName state.IdentScreenName) error {
	if err := f.local.BroadcastBuddyDeparted(ctx, screenName); err != nil {
		return err
	}
	if err := f.transport.NotifyPresenceToSubscribers(ctx, screenName, false, wire.TLVUserInfo{}); err != nil {
		f.logger.ErrorContext(ctx, "failed to notify federation peers of buddy departure",
			"screen_name", screenName,
			"err", err,
		)
	}
	return nil
}
