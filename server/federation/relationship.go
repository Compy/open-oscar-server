package federation

import (
	"context"
	"log/slog"

	"github.com/mk6i/open-oscar-server/state"
	"github.com/mk6i/open-oscar-server/wire"
)

// RelationshipFetcher is the interface for fetching user relationships.
type RelationshipFetcher interface {
	AllRelationships(ctx context.Context, me state.IdentScreenName, filter []state.IdentScreenName) ([]state.Relationship, error)
	Relationship(ctx context.Context, me state.IdentScreenName, them state.IdentScreenName) (state.Relationship, error)
}

// FederatedRelationshipFetcher wraps a local RelationshipFetcher and
// synthesizes relationships for remote users from local feedbag data.
type FederatedRelationshipFetcher struct {
	local        RelationshipFetcher
	feedbag      FeedbagManager
	localNetwork string
	logger       *slog.Logger
}

// NewFederatedRelationshipFetcher creates a new FederatedRelationshipFetcher.
func NewFederatedRelationshipFetcher(
	local RelationshipFetcher,
	feedbag FeedbagManager,
	localNetwork string,
	logger *slog.Logger,
) *FederatedRelationshipFetcher {
	return &FederatedRelationshipFetcher{
		local:        local,
		feedbag:      feedbag,
		localNetwork: localNetwork,
		logger:       logger,
	}
}

func (f *FederatedRelationshipFetcher) Relationship(ctx context.Context, me state.IdentScreenName, them state.IdentScreenName) (state.Relationship, error) {
	if them.IsLocal(f.localNetwork) {
		return f.local.Relationship(ctx, me, them)
	}
	// For remote users, synthesize a relationship from local feedbag data.
	// We know whether we have them on our list and whether we block them,
	// but we can't know their list or block settings.
	return f.synthesizeRemoteRelationship(ctx, me, them)
}

func (f *FederatedRelationshipFetcher) AllRelationships(ctx context.Context, me state.IdentScreenName, filter []state.IdentScreenName) ([]state.Relationship, error) {
	// Get local relationships from the database.
	localRels, err := f.local.AllRelationships(ctx, me, filter)
	if err != nil {
		return nil, err
	}

	// If a specific filter was provided, check for remote users in it and
	// synthesize relationships for them.
	if filter != nil {
		for _, sn := range filter {
			if sn.IsLocal(f.localNetwork) {
				continue
			}
			rel, err := f.synthesizeRemoteRelationship(ctx, me, sn)
			if err != nil {
				f.logger.ErrorContext(ctx, "failed to synthesize remote relationship",
					"me", me, "them", sn, "err", err)
				continue
			}
			localRels = append(localRels, rel)
		}
		return localRels, nil
	}

	// No filter means "all relationships." Scan the feedbag for remote
	// buddies and include them. This is the path taken by
	// BroadcastVisibility during sign-on.
	items, err := f.feedbag.Feedbag(ctx, me)
	if err != nil {
		f.logger.ErrorContext(ctx, "failed to load feedbag for remote relationships",
			"me", me, "err", err)
		return localRels, nil // non-fatal: return local relationships
	}

	for _, item := range items {
		if item.ClassID != wire.FeedbagClassIdBuddy {
			continue
		}
		buddySN := state.NewIdentScreenName(item.Name)
		if buddySN.IsLocal(f.localNetwork) {
			continue
		}
		localRels = append(localRels, state.Relationship{
			User:         buddySN,
			IsOnYourList: true,
			// We can't know the remote user's list or block settings.
			IsOnTheirList: false,
			YouBlock:      false,
			BlocksYou:     false,
		})
	}

	return localRels, nil
}

// synthesizeRemoteRelationship creates a relationship for a remote user based
// on local feedbag data. We can determine IsOnYourList (from feedbag), but
// cannot know IsOnTheirList or BlocksYou for remote users.
func (f *FederatedRelationshipFetcher) synthesizeRemoteRelationship(ctx context.Context, me state.IdentScreenName, them state.IdentScreenName) (state.Relationship, error) {
	rel := state.Relationship{
		User: them,
	}
	items, err := f.feedbag.Feedbag(ctx, me)
	if err != nil {
		return rel, err
	}
	for _, item := range items {
		switch {
		case item.ClassID == wire.FeedbagClassIdBuddy && state.NewIdentScreenName(item.Name) == them:
			rel.IsOnYourList = true
		case item.ClassID == wire.FeedbagClassIDDeny && state.NewIdentScreenName(item.Name) == them:
			rel.YouBlock = true
		}
	}
	return rel, nil
}
