package federation

import (
	"context"
	"log/slog"
	"time"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

// FeedbagManager is the interface for managing server-side buddy lists.
type FeedbagManager interface {
	Feedbag(ctx context.Context, screenName state.IdentScreenName) ([]wire.FeedbagItem, error)
	FeedbagDelete(ctx context.Context, screenName state.IdentScreenName, items []wire.FeedbagItem) error
	FeedbagLastModified(ctx context.Context, screenName state.IdentScreenName) (time.Time, error)
	FeedbagUpsert(ctx context.Context, screenName state.IdentScreenName, items []wire.FeedbagItem) error
	UseFeedbag(ctx context.Context, screenName state.IdentScreenName) error
}

// FederatedFeedbagManager wraps a local FeedbagManager and manages federation
// presence subscriptions when buddies are added or removed.
type FederatedFeedbagManager struct {
	local        FeedbagManager
	transport    Transport
	localNetwork string
	logger       *slog.Logger
}

// NewFederatedFeedbagManager creates a new FederatedFeedbagManager.
func NewFederatedFeedbagManager(
	local FeedbagManager,
	transport Transport,
	localNetwork string,
	logger *slog.Logger,
) *FederatedFeedbagManager {
	return &FederatedFeedbagManager{
		local:        local,
		transport:    transport,
		localNetwork: localNetwork,
		logger:       logger,
	}
}

func (f *FederatedFeedbagManager) Feedbag(ctx context.Context, screenName state.IdentScreenName) ([]wire.FeedbagItem, error) {
	return f.local.Feedbag(ctx, screenName)
}

func (f *FederatedFeedbagManager) FeedbagDelete(ctx context.Context, screenName state.IdentScreenName, items []wire.FeedbagItem) error {
	if err := f.local.FeedbagDelete(ctx, screenName, items); err != nil {
		return err
	}
	for _, item := range items {
		if item.ClassID != wire.FeedbagClassIdBuddy {
			continue
		}
		buddySN := state.NewIdentScreenName(item.Name)
		if buddySN.IsLocal(f.localNetwork) {
			continue
		}
		if err := f.transport.UnsubscribePresence(ctx, screenName, buddySN); err != nil {
			f.logger.ErrorContext(ctx, "failed to unsubscribe from federated buddy on delete",
				"local_user", screenName, "remote_user", buddySN, "err", err)
		}
	}
	return nil
}

func (f *FederatedFeedbagManager) FeedbagLastModified(ctx context.Context, screenName state.IdentScreenName) (time.Time, error) {
	return f.local.FeedbagLastModified(ctx, screenName)
}

func (f *FederatedFeedbagManager) FeedbagUpsert(ctx context.Context, screenName state.IdentScreenName, items []wire.FeedbagItem) error {
	if err := f.local.FeedbagUpsert(ctx, screenName, items); err != nil {
		return err
	}
	for _, item := range items {
		if item.ClassID != wire.FeedbagClassIdBuddy {
			continue
		}
		buddySN := state.NewIdentScreenName(item.Name)
		if buddySN.IsLocal(f.localNetwork) {
			continue
		}
		if err := f.transport.SubscribePresence(ctx, screenName, buddySN); err != nil {
			f.logger.ErrorContext(ctx, "failed to subscribe to federated buddy on upsert",
				"local_user", screenName, "remote_user", buddySN, "err", err)
		}
	}
	return nil
}

func (f *FederatedFeedbagManager) UseFeedbag(ctx context.Context, screenName state.IdentScreenName) error {
	return f.local.UseFeedbag(ctx, screenName)
}
