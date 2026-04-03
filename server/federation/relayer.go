package federation

import (
	"context"
	"log/slog"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

// MessageRelayer is the interface for message relay operations.
type MessageRelayer interface {
	RelayToScreenNames(ctx context.Context, screenNames []state.IdentScreenName, msg wire.SNACMessage)
	RelayToScreenName(ctx context.Context, screenName state.IdentScreenName, msg wire.SNACMessage)
	RelayToOtherInstances(ctx context.Context, instance *state.SessionInstance, msg wire.SNACMessage)
	RelayToScreenNameActiveOnly(ctx context.Context, screenName state.IdentScreenName, msg wire.SNACMessage)
	RelayToSelf(ctx context.Context, instance *state.SessionInstance, msg wire.SNACMessage)
}

// FederatedMessageRelayer wraps a local MessageRelayer and routes messages to
// remote servers when the recipient is on a federated network.
type FederatedMessageRelayer struct {
	local        MessageRelayer
	transport    Transport
	localNetwork string
	logger       *slog.Logger
}

// NewFederatedMessageRelayer creates a new FederatedMessageRelayer.
func NewFederatedMessageRelayer(
	local MessageRelayer,
	transport Transport,
	localNetwork string,
	logger *slog.Logger,
) *FederatedMessageRelayer {
	return &FederatedMessageRelayer{
		local:        local,
		transport:    transport,
		localNetwork: localNetwork,
		logger:       logger,
	}
}

func (f *FederatedMessageRelayer) RelayToScreenName(ctx context.Context, screenName state.IdentScreenName, msg wire.SNACMessage) {
	if screenName.IsLocal(f.localNetwork) {
		f.local.RelayToScreenName(ctx, screenName, msg)
		return
	}
	if err := f.transport.RouteToRemote(ctx, screenName, msg); err != nil {
		f.logger.ErrorContext(ctx, "failed to route message to federated user",
			"recipient", screenName,
			"foodgroup", wire.FoodGroupName(msg.Frame.FoodGroup),
			"subgroup", wire.SubGroupName(msg.Frame.FoodGroup, msg.Frame.SubGroup),
			"err", err,
		)
	}
}

func (f *FederatedMessageRelayer) RelayToScreenNames(ctx context.Context, screenNames []state.IdentScreenName, msg wire.SNACMessage) {
	var localNames []state.IdentScreenName
	for _, sn := range screenNames {
		if sn.IsLocal(f.localNetwork) {
			localNames = append(localNames, sn)
		} else {
			if err := f.transport.RouteToRemote(ctx, sn, msg); err != nil {
				f.logger.ErrorContext(ctx, "failed to route message to federated user",
					"recipient", sn,
					"err", err,
				)
			}
		}
	}
	if len(localNames) > 0 {
		f.local.RelayToScreenNames(ctx, localNames, msg)
	}
}

func (f *FederatedMessageRelayer) RelayToScreenNameActiveOnly(ctx context.Context, screenName state.IdentScreenName, msg wire.SNACMessage) {
	if screenName.IsLocal(f.localNetwork) {
		f.local.RelayToScreenNameActiveOnly(ctx, screenName, msg)
		return
	}
	// Remote users: route via federation (no granular active tracking for remote)
	if err := f.transport.RouteToRemote(ctx, screenName, msg); err != nil {
		f.logger.ErrorContext(ctx, "failed to route active-only message to federated user",
			"recipient", screenName,
			"err", err,
		)
	}
}

// RelayToOtherInstances always delegates to local — these are always the
// current user's own instances.
func (f *FederatedMessageRelayer) RelayToOtherInstances(ctx context.Context, instance *state.SessionInstance, msg wire.SNACMessage) {
	f.local.RelayToOtherInstances(ctx, instance, msg)
}

// RelayToSelf always delegates to local — this is always the current user's
// own session.
func (f *FederatedMessageRelayer) RelayToSelf(ctx context.Context, instance *state.SessionInstance, msg wire.SNACMessage) {
	f.local.RelayToSelf(ctx, instance, msg)
}
