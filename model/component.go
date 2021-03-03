package model

import (
	"context"

	"github.com/ourstudio-se/combind/persistence"

	"github.com/reveald/reveald"
)

type QueryBuilder = func(builder *reveald.QueryBuilder)

type Component interface {
	Type() string
	Children() []Component
	Build(context.Context) ([]*persistence.SearchBox, error)
	BuildQuery(builder *reveald.QueryBuilder)
}

type SearchableComponent interface {
	Find(context.Context, persistence.Key) (*persistence.SearchBox, error)
}
