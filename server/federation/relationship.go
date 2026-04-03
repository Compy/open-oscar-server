package federation

import (
	"context"

	"github.com/mk6i/open-oscar-server/state"
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
	localNetwork string
}

// NewFederatedRelationshipFetcher creates a new FederatedRelationshipFetcher.
func NewFederatedRelationshipFetcher(
	local RelationshipFetcher,
	localNetwork string,
) *FederatedRelationshipFetcher {
	return &FederatedRelationshipFetcher{
		local:        local,
		localNetwork: localNetwork,
	}
}

func (f *FederatedRelationshipFetcher) Relationship(ctx context.Context, me state.IdentScreenName, them state.IdentScreenName) (state.Relationship, error) {
	return f.local.Relationship(ctx, me, them)
}

func (f *FederatedRelationshipFetcher) AllRelationships(ctx context.Context, me state.IdentScreenName, filter []state.IdentScreenName) ([]state.Relationship, error) {
	return f.local.AllRelationships(ctx, me, filter)
}
