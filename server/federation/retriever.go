package federation

import (
	"github.com/mk6i/open-oscar-server/state"
)

// SessionRetriever is the interface for retrieving user sessions.
type SessionRetriever interface {
	RetrieveSession(screenName state.IdentScreenName) *state.Session
}

// FederatedSessionRetriever wraps a local SessionRetriever and checks a remote
// session store for users on federated networks.
type FederatedSessionRetriever struct {
	local        SessionRetriever
	remoteStore  *RemoteSessionStore
	localNetwork string
}

// NewFederatedSessionRetriever creates a new FederatedSessionRetriever.
func NewFederatedSessionRetriever(
	local SessionRetriever,
	remoteStore *RemoteSessionStore,
	localNetwork string,
) *FederatedSessionRetriever {
	return &FederatedSessionRetriever{
		local:        local,
		remoteStore:  remoteStore,
		localNetwork: localNetwork,
	}
}

func (f *FederatedSessionRetriever) RetrieveSession(screenName state.IdentScreenName) *state.Session {
	if screenName.IsLocal(f.localNetwork) {
		return f.local.RetrieveSession(screenName)
	}
	return f.remoteStore.Get(screenName)
}
