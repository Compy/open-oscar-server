package federation

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

func TestFederatedDepartureNotifier_BroadcastBuddyArrived(t *testing.T) {
	local := &mockDepartureNotifier{}
	transport := &mockTransport{}
	logger := slog.Default()

	notifier := NewFederatedDepartureNotifier(local, transport, "mynet", logger)

	sn := state.NewIdentScreenName("localuser")
	userInfo := wire.TLVUserInfo{ScreenName: "LocalUser"}

	err := notifier.BroadcastBuddyArrived(context.Background(), sn, userInfo)

	assert.NoError(t, err)
	assert.Len(t, local.arrivedCalls, 1)
	assert.Equal(t, sn, local.arrivedCalls[0].screenName)

	// Also notifies federation peers
	assert.Len(t, transport.presenceNotifications, 1)
	assert.True(t, transport.presenceNotifications[0].online)
	assert.Equal(t, sn, transport.presenceNotifications[0].localUser)
}

func TestFederatedDepartureNotifier_BroadcastBuddyDeparted(t *testing.T) {
	local := &mockDepartureNotifier{}
	transport := &mockTransport{}
	logger := slog.Default()

	notifier := NewFederatedDepartureNotifier(local, transport, "mynet", logger)

	sn := state.NewIdentScreenName("localuser")

	err := notifier.BroadcastBuddyDeparted(context.Background(), sn)

	assert.NoError(t, err)
	assert.Len(t, local.departedCalls, 1)
	assert.Equal(t, sn, local.departedCalls[0])

	assert.Len(t, transport.presenceNotifications, 1)
	assert.False(t, transport.presenceNotifications[0].online)
}
