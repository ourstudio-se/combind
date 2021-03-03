package persistence

import (
	"context"
	"crypto/md5"
	"fmt"
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
type Key struct {
	Policy     string `json:"POLICY,omitempty"`
	SpecMarket string `json:"SPEC_MARKET,omitempty"`
	Pno12      string `json:"PNO12,omitempty"`
	Week       string `json:"WEEK,omitempty"`
}

//SearchBox is the searchable model to be used
type SearchBox struct {
	Key       string                 `json:"key"`
	Type      string                 `json:"type"`
	Props     map[string]interface{} `json:"props"`
	Matches   []Key                  `json:"matches"`
	Match     Key                    `json:"match"`
	HashMatch string                 `json:"hash_match"`
}

// Hash generates a hash of this key object
func (k *Key) Hash() string {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("%v", k)))

	return fmt.Sprintf("%x", h.Sum(nil))
}
