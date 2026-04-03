package federation

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

func TestFederatedFeedbagManager_FeedbagUpsert_SubscribesRemoteBuddies(t *testing.T) {
	local := &mockFeedbagManager{}
	transport := &mockTransport{}
	logger := slog.Default()

	mgr := NewFederatedFeedbagManager(local, transport, "mynet", logger)

	user := state.NewIdentScreenName("myuser")
	items := []wire.FeedbagItem{
		{Name: "localbuddy", ClassID: wire.FeedbagClassIdBuddy},
		{Name: "remotebuddy@chivanet", ClassID: wire.FeedbagClassIdBuddy},
		{Name: "somegroup", ClassID: wire.FeedbagClassIdGroup},
	}

	err := mgr.FeedbagUpsert(context.Background(), user, items)

	assert.NoError(t, err)
	assert.Len(t, local.upserted, 3, "all items should be persisted locally")

	// Only the remote buddy should trigger a subscription
	assert.Len(t, transport.subscribedPresence, 1)
	assert.Equal(t, state.NewIdentScreenName("remotebuddy@chivanet"), transport.subscribedPresence[0].remoteUser)
}

func TestFederatedFeedbagManager_FeedbagDelete_UnsubscribesRemoteBuddies(t *testing.T) {
	local := &mockFeedbagManager{}
	transport := &mockTransport{}
	logger := slog.Default()

	mgr := NewFederatedFeedbagManager(local, transport, "mynet", logger)

	user := state.NewIdentScreenName("myuser")
	items := []wire.FeedbagItem{
		{Name: "remotebuddy@chivanet", ClassID: wire.FeedbagClassIdBuddy},
		{Name: "localbuddy", ClassID: wire.FeedbagClassIdBuddy},
	}

	err := mgr.FeedbagDelete(context.Background(), user, items)

	assert.NoError(t, err)
	assert.Len(t, local.deleted, 2)

	assert.Len(t, transport.unsubscribedPresence, 1)
	assert.Equal(t, state.NewIdentScreenName("remotebuddy@chivanet"), transport.unsubscribedPresence[0].remoteUser)
}

func TestFederatedFeedbagManager_ReadMethods_PassThrough(t *testing.T) {
	local := &mockFeedbagManager{
		items: []wire.FeedbagItem{
			{Name: "buddy1", ClassID: wire.FeedbagClassIdBuddy},
		},
	}
	transport := &mockTransport{}
	logger := slog.Default()

	mgr := NewFederatedFeedbagManager(local, transport, "mynet", logger)

	items, err := mgr.Feedbag(context.Background(), state.NewIdentScreenName("user"))
	assert.NoError(t, err)
	assert.Len(t, items, 1)

	_, err = mgr.FeedbagLastModified(context.Background(), state.NewIdentScreenName("user"))
	assert.NoError(t, err)

	err = mgr.UseFeedbag(context.Background(), state.NewIdentScreenName("user"))
	assert.NoError(t, err)

	// No transport calls for read methods
	assert.Empty(t, transport.subscribedPresence)
	assert.Empty(t, transport.unsubscribedPresence)
}
