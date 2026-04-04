package federation

import (
	"log/slog"
	"sync"
	"time"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

// RemoteSessionStore is a thread-safe store of proxy sessions for remote
// (federated) users. Each remote user who is online on a peer server is
// represented by a lightweight *state.Session with a synthetic instance.
type RemoteSessionStore struct {
	mu     sync.RWMutex
	store  map[state.IdentScreenName]*state.Session
	logger *slog.Logger
}

// NewRemoteSessionStore creates a new RemoteSessionStore.
func NewRemoteSessionStore(logger *slog.Logger) *RemoteSessionStore {
	return &RemoteSessionStore{
		store:  make(map[state.IdentScreenName]*state.Session),
		logger: logger,
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
		sess.SetDisplayScreenName(state.DisplayScreenName(screenName.String()))
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

// UpdateSessionData updates the proxy session for a remote user with extended
// session data from federation sync TLVs. If the session does not exist yet
// (presence hasn't arrived), this is a no-op.
func (s *RemoteSessionStore) UpdateSessionData(screenName state.IdentScreenName, tlvBlock wire.TLVRestBlock) {
	s.mu.RLock()
	sess, exists := s.store[screenName]
	s.mu.RUnlock()
	if !exists {
		s.logger.Debug("update session data skipped, session not found",
			"screen_name", screenName)
		return
	}

	instances := sess.Instances()
	if len(instances) == 0 {
		return
	}
	inst := instances[0]

	// Update profile
	profileText, hasProfile := tlvBlock.String(wire.FedTLVProfileText)
	profileMIME, hasMIME := tlvBlock.String(wire.FedTLVProfileMIME)
	if hasProfile || hasMIME {
		s.logger.Debug("updating remote session profile",
			"screen_name", screenName,
			"profile_len", len(profileText),
			"mime_type", profileMIME)
		inst.SetProfile(state.UserProfile{
			ProfileText: profileText,
			MIMEType:    profileMIME,
			UpdateTime:  time.Now(),
		})
	}

	// Update away message
	if awayMsg, ok := tlvBlock.String(wire.FedTLVAwayMessage); ok {
		s.logger.Debug("updating remote session away message",
			"screen_name", screenName,
			"away", awayMsg != "")
		inst.SetAwayMessage(awayMsg)
		if awayMsg != "" {
			inst.SetUserInfoFlag(wire.OServiceUserFlagUnavailable)
		} else {
			inst.ClearUserInfoFlag(wire.OServiceUserFlagUnavailable)
		}
	}

	// Update warning level
	if warning, ok := tlvBlock.Uint16BE(wire.FedTLVWarningLevel); ok {
		s.logger.Debug("updating remote session warning level",
			"screen_name", screenName,
			"warning", warning)
		sess.SetWarning(warning)
	}

	// Update idle time
	if idleSecs, ok := tlvBlock.Uint32BE(wire.FedTLVIdleSeconds); ok {
		s.logger.Debug("updating remote session idle time",
			"screen_name", screenName,
			"idle_seconds", idleSecs)
		inst.SetIdle(time.Duration(idleSecs) * time.Second)
	}

	// Update signon time
	if signonTime, ok := tlvBlock.Uint32BE(wire.FedTLVSignonTime); ok {
		sess.SetSignonTime(time.Unix(int64(signonTime), 0))
	}

	// Update flags and status (same logic as PresenceArrived)
	if flags, ok := tlvBlock.Uint16BE(wire.OServiceUserInfoUserFlags); ok {
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

// AllSessions returns a snapshot map of all tracked remote sessions.
func (s *RemoteSessionStore) AllSessions() map[state.IdentScreenName]*state.Session {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[state.IdentScreenName]*state.Session, len(s.store))
	for sn, sess := range s.store {
		result[sn] = sess
	}
	return result
}

// ClearNetwork removes all remote sessions belonging to the given network.
// Returns the list of screen names that were removed.
func (s *RemoteSessionStore) ClearNetwork(networkName string) []state.IdentScreenName {
	s.mu.Lock()
	defer s.mu.Unlock()

	var removed []state.IdentScreenName
	for sn := range s.store {
		if sn.Network() == networkName {
			removed = append(removed, sn)
			delete(s.store, sn)
		}
	}
	return removed
}
