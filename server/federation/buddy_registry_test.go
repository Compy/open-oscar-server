package federation

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

func TestFederatedBuddyListRegistry_RegisterBuddyList(t *testing.T) {
	local := &mockBuddyListRegistry{}
	transport := &mockTransport{}
	feedbag := &mockFeedbagManager{
		items: []wire.FeedbagItem{
			{Name: "localbuddy", ClassID: wire.FeedbagClassIdBuddy},
			{Name: "remotebuddy@chivanet", ClassID: wire.FeedbagClassIdBuddy},
			{Name: "anotherremote@retra", ClassID: wire.FeedbagClassIdBuddy},
			{Name: "somegroup", ClassID: wire.FeedbagClassIdGroup}, // not a buddy
		},
	}
	logger := slog.Default()

	registry := NewFederatedBuddyListRegistry(local, transport, feedbag, "mynet", logger)

	user := state.NewIdentScreenName("myuser")
	err := registry.RegisterBuddyList(context.Background(), user)

	assert.NoError(t, err)
	assert.Contains(t, local.registered, user)

	// Should subscribe to 2 remote buddies, not the local one or the group
	assert.Len(t, transport.subscribedPresence, 2)
	assert.Equal(t, user, transport.subscribedPresence[0].localUser)
	assert.Equal(t, state.NewIdentScreenName("remotebuddy@chivanet"), transport.subscribedPresence[0].remoteUser)
	assert.Equal(t, state.NewIdentScreenName("anotherremote@retra"), transport.subscribedPresence[1].remoteUser)

	// Should notify federation peers that this user is now online
	assert.Len(t, transport.presenceNotifications, 1)
	assert.Equal(t, user, transport.presenceNotifications[0].localUser)
	assert.True(t, transport.presenceNotifications[0].online)
}

func TestFederatedBuddyListRegistry_UnregisterBuddyList(t *testing.T) {
	local := &mockBuddyListRegistry{}
	transport := &mockTransport{}
	feedbag := &mockFeedbagManager{
		items: []wire.FeedbagItem{
			{Name: "remotebuddy@chivanet", ClassID: wire.FeedbagClassIdBuddy},
		},
	}
	logger := slog.Default()

	registry := NewFederatedBuddyListRegistry(local, transport, feedbag, "mynet", logger)

	user := state.NewIdentScreenName("myuser")
	err := registry.UnregisterBuddyList(context.Background(), user)

	assert.NoError(t, err)
	assert.Contains(t, local.unregistered, user)
	assert.Len(t, transport.unsubscribedPresence, 1)
}
