package persistence

import (
	"context"
)

//SearchBoxStorage interface
type SearchBoxStorage interface {
	Find(ctx context.Context, boxType string) ([]SearchBox, error)
	Save(ctx context.Context, dn ...*SearchBox) error
	Init(ctx context.Context) error
}

//ComponentStorage interface
type ComponentStorage interface {
	Find(ctx context.Context, componentType string) ([]Component, error)
	Save(ctx context.Context, c ...*Component) error
}

// Component ...
type Component struct {
	Code     string                 `json:"code"`
	Type     string                 `json:"type"`
	Name     string                 `json:"name"`
	LongName string                 `json:"longName"`
	Props    map[string]interface{} `json:"props"`
}

//Value represents low level value for real components, might need to be extended
type Value struct {
	Description string `json:"description"`
	Key         string `json:"key"`
}

//Key to be used in matches, might need to be extended later
type Key = map[string]string

//SearchBox is the searchable model to be used
type SearchBox struct {
	Key       string                 `json:"key"`
	Type      string                 `json:"type"`
	Props     map[string]interface{} `json:"props"`
	Matches   []Key
	Match     Key    `json:"match"`
	HashMatch string `json:"hash_match"`
}
