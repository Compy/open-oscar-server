package federation

import (
	"context"
	"log/slog"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

// ProfileManager is the interface for managing user profiles.
type ProfileManager interface {
	FindByAIMEmail(ctx context.Context, email string) (state.User, error)
	FindByAIMKeyword(ctx context.Context, keyword string) ([]state.User, error)
	FindByAIMNameAndAddr(ctx context.Context, info state.AIMNameAndAddr) ([]state.User, error)
	InterestList(ctx context.Context) ([]wire.ODirKeywordListItem, error)
	Profile(ctx context.Context, screenName state.IdentScreenName) (state.UserProfile, error)
	SetDirectoryInfo(ctx context.Context, screenName state.IdentScreenName, info state.AIMNameAndAddr) error
	SetKeywords(ctx context.Context, screenName state.IdentScreenName, keywords [5]string) error
	SetProfile(ctx context.Context, screenName state.IdentScreenName, profile state.UserProfile) error
	User(ctx context.Context, screenName state.IdentScreenName) (*state.User, error)
}

// FederatedProfileManager wraps a local ProfileManager and queries remote
// servers for profiles of federated users.
type FederatedProfileManager struct {
	local        ProfileManager
	transport    Transport
	remoteStore  *RemoteSessionStore
	localNetwork string
	logger       *slog.Logger
}

// NewFederatedProfileManager creates a new FederatedProfileManager.
func NewFederatedProfileManager(
	local ProfileManager,
	transport Transport,
	remoteStore *RemoteSessionStore,
	localNetwork string,
	logger *slog.Logger,
) *FederatedProfileManager {
	return &FederatedProfileManager{
		local:        local,
		transport:    transport,
		remoteStore:  remoteStore,
		localNetwork: localNetwork,
		logger:       logger,
	}
}

func (f *FederatedProfileManager) Profile(ctx context.Context, screenName state.IdentScreenName) (state.UserProfile, error) {
	if screenName.IsLocal(f.localNetwork) {
		return f.local.Profile(ctx, screenName)
	}
	// Query remote server for profile
	reply, err := f.transport.QueryUserInfo(ctx, screenName, wire.LocateTypeSig)
	if err != nil {
		f.logger.ErrorContext(ctx, "failed to query federated user profile",
			"screen_name", screenName, "err", err)
		return state.UserProfile{}, nil
	}
	if reply == nil {
		return state.UserProfile{}, nil
	}
	profileText, _ := reply.String(wire.LocateTLVTagsInfoSigData)
	mimeType, _ := reply.String(wire.LocateTLVTagsInfoSigMime)
	return state.UserProfile{
		ProfileText: profileText,
		MIMEType:    mimeType,
	}, nil
}

func (f *FederatedProfileManager) User(ctx context.Context, screenName state.IdentScreenName) (*state.User, error) {
	if screenName.IsLocal(f.localNetwork) {
		return f.local.User(ctx, screenName)
	}
	// For remote users, return a synthetic User if we have a proxy session
	if sess := f.remoteStore.Get(screenName); sess != nil {
		return &state.User{
			IdentScreenName:   screenName,
			DisplayScreenName: sess.DisplayScreenName(),
		}, nil
	}
	return nil, nil
}

// Search/write methods always operate on local data only.

func (f *FederatedProfileManager) FindByAIMEmail(ctx context.Context, email string) (state.User, error) {
	return f.local.FindByAIMEmail(ctx, email)
}

func (f *FederatedProfileManager) FindByAIMKeyword(ctx context.Context, keyword string) ([]state.User, error) {
	return f.local.FindByAIMKeyword(ctx, keyword)
}

func (f *FederatedProfileManager) FindByAIMNameAndAddr(ctx context.Context, info state.AIMNameAndAddr) ([]state.User, error) {
	return f.local.FindByAIMNameAndAddr(ctx, info)
}

func (f *FederatedProfileManager) InterestList(ctx context.Context) ([]wire.ODirKeywordListItem, error) {
	return f.local.InterestList(ctx)
}

func (f *FederatedProfileManager) SetDirectoryInfo(ctx context.Context, screenName state.IdentScreenName, info state.AIMNameAndAddr) error {
	return f.local.SetDirectoryInfo(ctx, screenName, info)
}

func (f *FederatedProfileManager) SetKeywords(ctx context.Context, screenName state.IdentScreenName, keywords [5]string) error {
	return f.local.SetKeywords(ctx, screenName, keywords)
}

func (f *FederatedProfileManager) SetProfile(ctx context.Context, screenName state.IdentScreenName, profile state.UserProfile) error {
	return f.local.SetProfile(ctx, screenName, profile)
}
