package federation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

// mockSessionRetriever is a simple mock for the SessionRetriever interface.
type mockSessionRetriever struct {
	sessions map[state.IdentScreenName]*state.Session
}

func (m *mockSessionRetriever) RetrieveSession(screenName state.IdentScreenName) *state.Session {
	return m.sessions[screenName]
}

func TestFederatedSessionRetriever_LocalUser(t *testing.T) {
	localSess := state.NewSession()
	localSess.SetIdentScreenName(state.NewIdentScreenName("localuser"))

	local := &mockSessionRetriever{
		sessions: map[state.IdentScreenName]*state.Session{
			state.NewIdentScreenName("localuser"): localSess,
		},
	}
	remoteStore := NewRemoteSessionStore()

	retriever := NewFederatedSessionRetriever(local, remoteStore, "mynet")

	// Local user (no @network) should be looked up in the local store
	sess := retriever.RetrieveSession(state.NewIdentScreenName("localuser"))
	assert.Equal(t, localSess, sess)
}

func TestFederatedSessionRetriever_LocalUserWithMatchingNetwork(t *testing.T) {
	localSess := state.NewSession()
	sn := state.NewIdentScreenName("localuser@mynet")
	localSess.SetIdentScreenName(sn)

	local := &mockSessionRetriever{
		sessions: map[state.IdentScreenName]*state.Session{
			sn: localSess,
		},
	}
	remoteStore := NewRemoteSessionStore()

	retriever := NewFederatedSessionRetriever(local, remoteStore, "mynet")

	// user@mynet should be treated as local
	sess := retriever.RetrieveSession(sn)
	assert.Equal(t, localSess, sess)
}

func TestFederatedSessionRetriever_RemoteUser(t *testing.T) {
	local := &mockSessionRetriever{
		sessions: map[state.IdentScreenName]*state.Session{},
	}
	remoteStore := NewRemoteSessionStore()

	remoteSN := state.NewIdentScreenName("remoteuser@chivanet")
	remoteStore.PresenceArrived(remoteSN, wire.TLVRestBlock{})

	retriever := NewFederatedSessionRetriever(local, remoteStore, "mynet")

	sess := retriever.RetrieveSession(remoteSN)
	assert.NotNil(t, sess)
	assert.Equal(t, remoteSN, sess.IdentScreenName())
}

func TestFederatedSessionRetriever_RemoteUserNotFound(t *testing.T) {
	local := &mockSessionRetriever{
		sessions: map[state.IdentScreenName]*state.Session{},
	}
	remoteStore := NewRemoteSessionStore()

	retriever := NewFederatedSessionRetriever(local, remoteStore, "mynet")

	sess := retriever.RetrieveSession(state.NewIdentScreenName("nobody@chivanet"))
	assert.Nil(t, sess)
}

func TestFederatedSessionRetriever_LocalUserNotFound(t *testing.T) {
	local := &mockSessionRetriever{
		sessions: map[state.IdentScreenName]*state.Session{},
	}
	remoteStore := NewRemoteSessionStore()

	retriever := NewFederatedSessionRetriever(local, remoteStore, "mynet")

	sess := retriever.RetrieveSession(state.NewIdentScreenName("nobody"))
	assert.Nil(t, sess)
}
