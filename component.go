package combind

import (
	"context"

	"github.com/reveald/reveald"
)

type QueryBuilder = func(builder *reveald.QueryBuilder)

type Component interface {
	Type() string
	Children() []Component
	Build(ctx context.Context, rebuild bool) ([]*SearchBox, error)
	BuildQuery(builder *reveald.QueryBuilder)
}

type SearchableComponent interface {
	Find(context.Context, Key) (*SearchBox, error)
}
