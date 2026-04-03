package federation

import (
	"sync"
	"time"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

// RemoteSessionStore is a thread-safe store of proxy sessions for remote
// (federated) users. Each remote user who is online on a peer server is
// represented by a lightweight *state.Session with a synthetic instance.
type RemoteSessionStore struct {
	mu    sync.RWMutex
	store map[state.IdentScreenName]*state.Session
}

// NewRemoteSessionStore creates a new RemoteSessionStore.
func NewRemoteSessionStore() *RemoteSessionStore {
	return &RemoteSessionStore{
		store: make(map[state.IdentScreenName]*state.Session),
	}
}

// Get returns the proxy session for a remote user, or nil if not found.
func (s *RemoteSessionStore) Get(screenName state.IdentScreenName) *state.Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store[screenName]
}

// PresenceArrived creates or updates a proxy session for a remote user who has
// come online. The TLV block contains user info fields (flags, status, caps)
// from the federation presence notification.
func (s *RemoteSessionStore) PresenceArrived(screenName state.IdentScreenName, tlvBlock wire.TLVRestBlock) *state.Session {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, exists := s.store[screenName]
	if !exists {
		sess = state.NewSession()
		sess.SetIdentScreenName(screenName)
		sess.SetDisplayScreenName(state.DisplayScreenName(screenName.LocalPart().String()))
		sess.SetSignonTime(time.Now())

		instance := sess.AddInstance()
		instance.SetSignonComplete()
		instance.SetUserInfoFlag(wire.OServiceUserFlagOSCARFree)

		s.store[screenName] = sess
	}

	// Update the synthetic instance with data from the presence notification.
	instances := sess.Instances()
	if len(instances) > 0 {
		inst := instances[0]
		if flags, ok := tlvBlock.Uint16BE(wire.OServiceUserInfoUserFlags); ok {
			// Clear and re-set flags by setting the bitmask directly.
			inst.SetUserInfoFlag(flags)
		}
		if status, ok := tlvBlock.Uint32BE(wire.OServiceUserInfoStatus); ok {
			inst.SetUserStatusBitmask(status)
		}
		if capData, ok := tlvBlock.Bytes(wire.OServiceUserInfoOscarCaps); ok {
			var caps [][16]byte
			for i := 0; i+16 <= len(capData); i += 16 {
				var cap [16]byte
				copy(cap[:], capData[i:i+16])
				caps = append(caps, cap)
			}
			inst.SetCaps(caps)
		}
	}

	return sess
}

// PresenceDeparted removes a proxy session for a remote user who has gone
// offline. Returns true if the session existed and was removed.
func (s *RemoteSessionStore) PresenceDeparted(screenName state.IdentScreenName) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.store[screenName]; !exists {
		return false
	}
	delete(s.store, screenName)
	return true
}

// Subscribers returns all remote screen names currently tracked in the store.
func (s *RemoteSessionStore) Subscribers() []state.IdentScreenName {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]state.IdentScreenName, 0, len(s.store))
	for sn := range s.store {
		names = append(names, sn)
	}
	return names
}
