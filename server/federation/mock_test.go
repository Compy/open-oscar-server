package federation

import (
	"context"
	"time"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

// mockTransport records calls to the Transport interface.
type mockTransport struct {
	routedMessages       []routedMessage
	subscribedPresence   []presenceSub
	unsubscribedPresence []presenceSub
	presenceNotifications []presenceNotif
	userInfoQueries      []userInfoQuery
	routeErr             error
	subscribeErr         error
	unsubscribeErr       error
	notifyErr            error
	queryResult          *wire.SNAC_0x0100_0x000E_FedUserInfoReply
	queryErr             error
}

type routedMessage struct {
	recipient state.IdentScreenName
	msg       wire.SNACMessage
}

type presenceSub struct {
	localUser  state.IdentScreenName
	remoteUser state.IdentScreenName
}

type presenceNotif struct {
	localUser state.IdentScreenName
	online    bool
	userInfo  wire.TLVUserInfo
}

type userInfoQuery struct {
	remoteUser  state.IdentScreenName
	requestType uint32
}

func (m *mockTransport) RouteToRemote(_ context.Context, recipient state.IdentScreenName, msg wire.SNACMessage) error {
	m.routedMessages = append(m.routedMessages, routedMessage{recipient: recipient, msg: msg})
	return m.routeErr
}

func (m *mockTransport) SubscribePresence(_ context.Context, localUser, remoteUser state.IdentScreenName) error {
	m.subscribedPresence = append(m.subscribedPresence, presenceSub{localUser: localUser, remoteUser: remoteUser})
	return m.subscribeErr
}

func (m *mockTransport) UnsubscribePresence(_ context.Context, localUser, remoteUser state.IdentScreenName) error {
	m.unsubscribedPresence = append(m.unsubscribedPresence, presenceSub{localUser: localUser, remoteUser: remoteUser})
	return m.unsubscribeErr
}

func (m *mockTransport) NotifyPresenceToSubscribers(_ context.Context, localUser state.IdentScreenName, online bool, userInfo wire.TLVUserInfo) error {
	m.presenceNotifications = append(m.presenceNotifications, presenceNotif{localUser: localUser, online: online, userInfo: userInfo})
	return m.notifyErr
}

func (m *mockTransport) QueryUserInfo(_ context.Context, remoteUser state.IdentScreenName, requestType uint32) (*wire.SNAC_0x0100_0x000E_FedUserInfoReply, error) {
	m.userInfoQueries = append(m.userInfoQueries, userInfoQuery{remoteUser: remoteUser, requestType: requestType})
	return m.queryResult, m.queryErr
}

// mockMessageRelayer records calls to the MessageRelayer interface.
type mockMessageRelayer struct {
	relayedToScreenName       []relayedMsg
	relayedToScreenNames      []relayedBatchMsg
	relayedToScreenNameActive []relayedMsg
	relayedToSelf             []relayedInstanceMsg
	relayedToOtherInstances   []relayedInstanceMsg
}

type relayedMsg struct {
	screenName state.IdentScreenName
	msg        wire.SNACMessage
}

type relayedBatchMsg struct {
	screenNames []state.IdentScreenName
	msg         wire.SNACMessage
}

type relayedInstanceMsg struct {
	instance *state.SessionInstance
	msg      wire.SNACMessage
}

func (m *mockMessageRelayer) RelayToScreenName(_ context.Context, screenName state.IdentScreenName, msg wire.SNACMessage) {
	m.relayedToScreenName = append(m.relayedToScreenName, relayedMsg{screenName: screenName, msg: msg})
}

func (m *mockMessageRelayer) RelayToScreenNames(_ context.Context, screenNames []state.IdentScreenName, msg wire.SNACMessage) {
	m.relayedToScreenNames = append(m.relayedToScreenNames, relayedBatchMsg{screenNames: screenNames, msg: msg})
}

func (m *mockMessageRelayer) RelayToScreenNameActiveOnly(_ context.Context, screenName state.IdentScreenName, msg wire.SNACMessage) {
	m.relayedToScreenNameActive = append(m.relayedToScreenNameActive, relayedMsg{screenName: screenName, msg: msg})
}

func (m *mockMessageRelayer) RelayToSelf(_ context.Context, instance *state.SessionInstance, msg wire.SNACMessage) {
	m.relayedToSelf = append(m.relayedToSelf, relayedInstanceMsg{instance: instance, msg: msg})
}

func (m *mockMessageRelayer) RelayToOtherInstances(_ context.Context, instance *state.SessionInstance, msg wire.SNACMessage) {
	m.relayedToOtherInstances = append(m.relayedToOtherInstances, relayedInstanceMsg{instance: instance, msg: msg})
}

// mockDepartureNotifier records calls to the DepartureNotifier interface.
type mockDepartureNotifier struct {
	arrivedCalls  []arrivalCall
	departedCalls []state.IdentScreenName
	arriveErr     error
	departErr     error
}

type arrivalCall struct {
	screenName state.IdentScreenName
	userInfo   wire.TLVUserInfo
}

func (m *mockDepartureNotifier) BroadcastBuddyArrived(_ context.Context, screenName state.IdentScreenName, userInfo wire.TLVUserInfo) error {
	m.arrivedCalls = append(m.arrivedCalls, arrivalCall{screenName: screenName, userInfo: userInfo})
	return m.arriveErr
}

func (m *mockDepartureNotifier) BroadcastBuddyDeparted(_ context.Context, screenName state.IdentScreenName) error {
	m.departedCalls = append(m.departedCalls, screenName)
	return m.departErr
}

// mockBuddyListRegistry records calls to the BuddyListRegistry interface.
type mockBuddyListRegistry struct {
	registered   []state.IdentScreenName
	unregistered []state.IdentScreenName
	cleared      bool
	registerErr  error
}

func (m *mockBuddyListRegistry) ClearBuddyListRegistry(_ context.Context) error {
	m.cleared = true
	return nil
}

func (m *mockBuddyListRegistry) RegisterBuddyList(_ context.Context, user state.IdentScreenName) error {
	m.registered = append(m.registered, user)
	return m.registerErr
}

func (m *mockBuddyListRegistry) UnregisterBuddyList(_ context.Context, user state.IdentScreenName) error {
	m.unregistered = append(m.unregistered, user)
	return nil
}

// mockFeedbagManager returns configured feedbag items.
type mockFeedbagManager struct {
	items       []wire.FeedbagItem
	upserted    []wire.FeedbagItem
	deleted     []wire.FeedbagItem
	upsertErr   error
	deleteErr   error
}

func (m *mockFeedbagManager) Feedbag(_ context.Context, _ state.IdentScreenName) ([]wire.FeedbagItem, error) {
	return m.items, nil
}

func (m *mockFeedbagManager) FeedbagDelete(_ context.Context, _ state.IdentScreenName, items []wire.FeedbagItem) error {
	m.deleted = append(m.deleted, items...)
	return m.deleteErr
}

func (m *mockFeedbagManager) FeedbagLastModified(_ context.Context, _ state.IdentScreenName) (time.Time, error) {
	return time.Time{}, nil
}

func (m *mockFeedbagManager) FeedbagUpsert(_ context.Context, _ state.IdentScreenName, items []wire.FeedbagItem) error {
	m.upserted = append(m.upserted, items...)
	return m.upsertErr
}

func (m *mockFeedbagManager) UseFeedbag(_ context.Context, _ state.IdentScreenName) error {
	return nil
}
