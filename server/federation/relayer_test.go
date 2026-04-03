package federation

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

func TestFederatedMessageRelayer_RelayToScreenName_Local(t *testing.T) {
	local := &mockMessageRelayer{}
	transport := &mockTransport{}
	logger := slog.Default()

	relayer := NewFederatedMessageRelayer(local, transport, "mynet", logger)

	msg := wire.SNACMessage{Frame: wire.SNACFrame{FoodGroup: wire.ICBM, SubGroup: wire.ICBMChannelMsgToClient}}
	relayer.RelayToScreenName(context.Background(), state.NewIdentScreenName("localuser"), msg)

	assert.Len(t, local.relayedToScreenName, 1)
	assert.Equal(t, state.NewIdentScreenName("localuser"), local.relayedToScreenName[0].screenName)
	assert.Empty(t, transport.routedMessages)
}

func TestFederatedMessageRelayer_RelayToScreenName_Remote(t *testing.T) {
	local := &mockMessageRelayer{}
	transport := &mockTransport{}
	logger := slog.Default()

	relayer := NewFederatedMessageRelayer(local, transport, "mynet", logger)

	msg := wire.SNACMessage{Frame: wire.SNACFrame{FoodGroup: wire.ICBM, SubGroup: wire.ICBMChannelMsgToClient}}
	relayer.RelayToScreenName(context.Background(), state.NewIdentScreenName("remoteuser@chivanet"), msg)

	assert.Empty(t, local.relayedToScreenName)
	assert.Len(t, transport.routedMessages, 1)
	assert.Equal(t, state.NewIdentScreenName("remoteuser@chivanet"), transport.routedMessages[0].recipient)
}

func TestFederatedMessageRelayer_RelayToScreenName_LocalNetwork(t *testing.T) {
	local := &mockMessageRelayer{}
	transport := &mockTransport{}
	logger := slog.Default()

	relayer := NewFederatedMessageRelayer(local, transport, "mynet", logger)

	msg := wire.SNACMessage{Frame: wire.SNACFrame{FoodGroup: wire.ICBM}}
	relayer.RelayToScreenName(context.Background(), state.NewIdentScreenName("localuser@mynet"), msg)

	assert.Len(t, local.relayedToScreenName, 1, "user@mynet should be treated as local")
	assert.Empty(t, transport.routedMessages)
}

func TestFederatedMessageRelayer_RelayToScreenNames_Mixed(t *testing.T) {
	local := &mockMessageRelayer{}
	transport := &mockTransport{}
	logger := slog.Default()

	relayer := NewFederatedMessageRelayer(local, transport, "mynet", logger)

	msg := wire.SNACMessage{Frame: wire.SNACFrame{FoodGroup: wire.Buddy, SubGroup: wire.BuddyArrived}}
	recipients := []state.IdentScreenName{
		state.NewIdentScreenName("local1"),
		state.NewIdentScreenName("remote1@chivanet"),
		state.NewIdentScreenName("local2"),
		state.NewIdentScreenName("remote2@retra"),
	}

	relayer.RelayToScreenNames(context.Background(), recipients, msg)

	// Local users should be batched in a single call
	assert.Len(t, local.relayedToScreenNames, 1)
	assert.Len(t, local.relayedToScreenNames[0].screenNames, 2)
	assert.Contains(t, local.relayedToScreenNames[0].screenNames, state.NewIdentScreenName("local1"))
	assert.Contains(t, local.relayedToScreenNames[0].screenNames, state.NewIdentScreenName("local2"))

	// Remote users should be routed individually
	assert.Len(t, transport.routedMessages, 2)
}

func TestFederatedMessageRelayer_RelayToScreenNames_AllLocal(t *testing.T) {
	local := &mockMessageRelayer{}
	transport := &mockTransport{}
	logger := slog.Default()

	relayer := NewFederatedMessageRelayer(local, transport, "mynet", logger)

	msg := wire.SNACMessage{Frame: wire.SNACFrame{FoodGroup: wire.Buddy}}
	recipients := []state.IdentScreenName{
		state.NewIdentScreenName("local1"),
		state.NewIdentScreenName("local2"),
	}

	relayer.RelayToScreenNames(context.Background(), recipients, msg)

	assert.Len(t, local.relayedToScreenNames, 1)
	assert.Len(t, local.relayedToScreenNames[0].screenNames, 2)
	assert.Empty(t, transport.routedMessages)
}

func TestFederatedMessageRelayer_RelayToScreenNames_AllRemote(t *testing.T) {
	local := &mockMessageRelayer{}
	transport := &mockTransport{}
	logger := slog.Default()

	relayer := NewFederatedMessageRelayer(local, transport, "mynet", logger)

	msg := wire.SNACMessage{Frame: wire.SNACFrame{FoodGroup: wire.Buddy}}
	recipients := []state.IdentScreenName{
		state.NewIdentScreenName("user1@chivanet"),
		state.NewIdentScreenName("user2@retra"),
	}

	relayer.RelayToScreenNames(context.Background(), recipients, msg)

	assert.Empty(t, local.relayedToScreenNames, "no local relay should happen")
	assert.Len(t, transport.routedMessages, 2)
}

func TestFederatedMessageRelayer_RelayToSelf_AlwaysLocal(t *testing.T) {
	local := &mockMessageRelayer{}
	transport := &mockTransport{}
	logger := slog.Default()

	relayer := NewFederatedMessageRelayer(local, transport, "mynet", logger)

	instance := state.NewSession().AddInstance()
	msg := wire.SNACMessage{Frame: wire.SNACFrame{FoodGroup: wire.ICBM}}

	relayer.RelayToSelf(context.Background(), instance, msg)

	assert.Len(t, local.relayedToSelf, 1)
	assert.Empty(t, transport.routedMessages)
}

func TestFederatedMessageRelayer_RelayToOtherInstances_AlwaysLocal(t *testing.T) {
	local := &mockMessageRelayer{}
	transport := &mockTransport{}
	logger := slog.Default()

	relayer := NewFederatedMessageRelayer(local, transport, "mynet", logger)

	instance := state.NewSession().AddInstance()
	msg := wire.SNACMessage{Frame: wire.SNACFrame{FoodGroup: wire.ICBM}}

	relayer.RelayToOtherInstances(context.Background(), instance, msg)

	assert.Len(t, local.relayedToOtherInstances, 1)
	assert.Empty(t, transport.routedMessages)
}
