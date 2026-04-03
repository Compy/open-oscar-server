package federation

import (
	"context"
	"log/slog"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

// BuddyListRegistry is the interface for buddy list registration.
type BuddyListRegistry interface {
	ClearBuddyListRegistry(ctx context.Context) error
	RegisterBuddyList(ctx context.Context, user state.IdentScreenName) error
	UnregisterBuddyList(ctx context.Context, user state.IdentScreenName) error
}

// FederatedBuddyListRegistry wraps a local BuddyListRegistry and manages
// federation presence subscriptions when users register/unregister.
type FederatedBuddyListRegistry struct {
	local        BuddyListRegistry
	transport    Transport
	feedbag      FeedbagManager
	localNetwork string
	logger       *slog.Logger
}

// NewFederatedBuddyListRegistry creates a new FederatedBuddyListRegistry.
func NewFederatedBuddyListRegistry(
	local BuddyListRegistry,
	transport Transport,
	feedbag FeedbagManager,
	localNetwork string,
	logger *slog.Logger,
) *FederatedBuddyListRegistry {
	return &FederatedBuddyListRegistry{
		local:        local,
		transport:    transport,
		feedbag:      feedbag,
		localNetwork: localNetwork,
		logger:       logger,
	}
}

func (f *FederatedBuddyListRegistry) ClearBuddyListRegistry(ctx context.Context) error {
	return f.local.ClearBuddyListRegistry(ctx)
}

func (f *FederatedBuddyListRegistry) RegisterBuddyList(ctx context.Context, user state.IdentScreenName) error {
	if err := f.local.RegisterBuddyList(ctx, user); err != nil {
		return err
	}
	// Subscribe to presence for all federated buddies in the user's feedbag
	items, err := f.feedbag.Feedbag(ctx, user)
	if err != nil {
		f.logger.ErrorContext(ctx, "failed to load feedbag for federation subscriptions",
			"screen_name", user, "err", err)
		return nil // non-fatal: local registration succeeded
	}
	for _, item := range items {
		if item.ClassID != wire.FeedbagClassIdBuddy {
			continue
		}
		buddySN := state.NewIdentScreenName(item.Name)
		if buddySN.IsLocal(f.localNetwork) {
			continue
		}
		if err := f.transport.SubscribePresence(ctx, user, buddySN); err != nil {
			f.logger.ErrorContext(ctx, "failed to subscribe to federated buddy presence",
				"local_user", user, "remote_user", buddySN, "err", err)
		}
	}
	return nil
}

func (f *FederatedBuddyListRegistry) UnregisterBuddyList(ctx context.Context, user state.IdentScreenName) error {
	if err := f.local.UnregisterBuddyList(ctx, user); err != nil {
		return err
	}
	// Unsubscribe from all federated buddies
	items, err := f.feedbag.Feedbag(ctx, user)
	if err != nil {
		f.logger.ErrorContext(ctx, "failed to load feedbag for federation unsubscriptions",
			"screen_name", user, "err", err)
		return nil
	}
	for _, item := range items {
		if item.ClassID != wire.FeedbagClassIdBuddy {
			continue
		}
		buddySN := state.NewIdentScreenName(item.Name)
		if buddySN.IsLocal(f.localNetwork) {
			continue
		}
		if err := f.transport.UnsubscribePresence(ctx, user, buddySN); err != nil {
			f.logger.ErrorContext(ctx, "failed to unsubscribe from federated buddy presence",
				"local_user", user, "remote_user", buddySN, "err", err)
		}
	}
	return nil
}
