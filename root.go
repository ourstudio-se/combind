package combind

import (
	"context"
	"encoding/json"
	"log"

	"github.com/reveald/reveald"
)

type Modifier func(*SearchBox)

type ResultModifier func([]*SearchBox)

type RootComponent struct {
	storage         ComponentStorage
	typ             string
	KeyType         string
	build           []*SearchBox
	modifiers       []Modifier
	resultModifiers []ResultModifier
	queryBuilder    QueryBuilder
}

type RootConfiguration func(*RootComponent)

func WithModifier(modifier Modifier) RootConfiguration {
	return func(rc *RootComponent) {
		rc.modifiers = append(rc.modifiers, modifier)
	}
}

func WithResultModifier(resultModifier ResultModifier) RootConfiguration {
	return func(rc *RootComponent) {
		rc.resultModifiers = append(rc.resultModifiers, resultModifier)
	}
}

func WithRootQuery(queryBuilder QueryBuilder) RootConfiguration {
	return func(rc *RootComponent) {
		rc.queryBuilder = queryBuilder
	}
}

func NewRoot(typ string, storage ComponentStorage, config ...RootConfiguration) *RootComponent {
	rc := &RootComponent{
		storage:   storage,
		typ:       typ,
		KeyType:   typ,
		modifiers: []Modifier{},
		queryBuilder: func(builder *reveald.QueryBuilder) {
			//default do nothing, i.e does not expose
		},
	}

	for _, cfg := range config {
		cfg(rc)
	}

	return rc
}

func (rc *RootComponent) Type() string {
	return rc.typ
}

func (rc *RootComponent) Children() []Component {
	return []Component{}
}

func (rc *RootComponent) Build(ctx context.Context, rebuild bool) ([]*SearchBox, error) {

	if rc.build != nil && !rebuild {
		return rc.build, nil
	}

	values, err := rc.storage.Find(ctx, rc.typ)
	if err != nil {
		return nil, err
	}

	addOrUpdate := []*SearchBox{}

	for _, value := range values {
		k := Key{}

		b, err := json.Marshal(map[string]string{
			rc.KeyType: value.Code,
		})

		if err != nil {
			log.Fatalf("Could not marshal %v", rc.typ)
		}

		if err := json.Unmarshal(b, &k); err != nil {
			log.Fatalf("Could not unmarshal %s", b)
		}

		sb := &SearchBox{
			Type: rc.typ,
			Key:  value.Code,
			Props: merge(map[string]interface{}{
				"name": value.Name,
			}, value.Props),
			Matches: []Key{k},
		}

		for _, modifier := range rc.modifiers {
			modifier(sb)
		}

		addOrUpdate = append(addOrUpdate, sb)
	}
	for _, c := range addOrUpdate {
		c.Matches = DedupKeys(c.Matches)
	}
	rc.build = addOrUpdate
	for _, rm := range rc.resultModifiers {
		rm(rc.build)
	}
	return rc.build, nil
}

func (rc *RootComponent) BuildQuery(builder *reveald.QueryBuilder) {
	rc.queryBuilder(builder)
}

func merge(m1 map[string]interface{}, m2 map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m1 {
		result[k] = v
	}
	for k, v := range m2 {
		result[k] = v
	}
	return result
}
