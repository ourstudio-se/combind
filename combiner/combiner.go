package graph

import (
	"context"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/ourstudio-se/combind/model"
	"github.com/ourstudio-se/combind/persistence"
	"github.com/reveald/reveald"
	log "github.com/sirupsen/logrus"
)

//Combiner for the current relationships
type Combiner struct {
	components       map[string]model.Component
	componentStorage persistence.ComponentStorage
	metadataStorage  persistence.SearchBoxStorage
	roots            map[string][]string
	// eventstream      eventstream.KafkaEventWriter
}

type CombinerBuilder interface {
	Update(ctx context.Context, comp ...*persistence.Component) ([]*persistence.SearchBox, error)
	Save(ctx context.Context) ([]*persistence.SearchBox, error)
}

// New for modeling the metadata
func New(
	componentStorage persistence.ComponentStorage,
	combindStorage persistence.SearchBoxStorage,
	components ...model.Component) *Combiner {

	g := &Combiner{
		componentStorage: componentStorage,
		metadataStorage:  combindStorage,
		components:       map[string]model.Component{},
		roots:            map[string][]string{},
	}

	g.roots = map[string][]string{}

	for _, c := range components {
		g.roots[c.Type()] = getComponentRoots(c)
		g.components[c.Type()] = c
	}

	return g
}

func (g *Combiner) ComponentTypes() []string {
	res := []string{}
	for _, c := range g.components {
		res = append(res, c.Type())
	}

	return res
}

//Save the graph to the provided storage
func (g *Combiner) Save(ctx context.Context) ([]*persistence.SearchBox, error) {

	logTime := func(part string, start time.Time) {
		end := time.Now()
		log.Debugf("%s took %d MS", part, end.Sub(start).Milliseconds())
	}

	defer logTime("Total runtime", time.Now())
	results := []*persistence.SearchBox{}
	for typ, comp := range g.components {
		start := time.Now()
		log.Debugf("Running %s", typ)
		result, err := comp.Build(ctx, true)
		if err != nil {
			return nil, err
		}

		logTime(typ, start)
		results = append(results, result...)
	}
	return results, nil
}

// Update recomputes the updates for the graph
func (combiner *Combiner) Update(ctx context.Context, comps ...*persistence.Component) ([]*persistence.SearchBox, error) {
	deletedBoxes := []*persistence.SearchBox{}
	updatedBoxes := []*persistence.SearchBox{}
	createdBoxes := []*persistence.SearchBox{}

	for key, component := range combiner.components {
		roots := getComponentRoots(component)
		for _, comp := range comps {
			for _, root := range roots {

				if root == comp.Type {

					boxes, err := combiner.metadataStorage.Find(ctx, key)
					if err != nil {
						return nil, err
					}
					builds, err := component.Build(ctx, true)
					if err != nil {
						return nil, err
					}

					builtIndex := map[string]*persistence.SearchBox{}
					existingIndex := map[string]*persistence.SearchBox{}

					for _, builtBox := range builds {
						builtIndex[builtBox.Key] = builtBox
					}

					for _, existingBox := range boxes {
						existingIndex[existingBox.Key] = &existingBox
					}

					for k, build := range builtIndex {
						if existingIndex[k] == nil {
							createdBoxes = append(createdBoxes, build)
						} else if !cmp.Equal(existingIndex[k], build) {
							updatedBoxes = append(updatedBoxes, build)
						}
					}

					for k, existingBox := range existingIndex {
						if builtIndex[k] == nil {
							deletedBoxes = append(deletedBoxes, existingBox)
						}
					}
				}
			}
		}
	}

	return append(updatedBoxes, createdBoxes...), nil
}

func getComponentRoots(comp model.Component) []string {

	if _, ok := comp.(*model.RootComponent); ok {
		return []string{comp.Type()}
	}

	roots := []string{}
	for _, c := range comp.Children() {
		if root, ok := c.(*model.RootComponent); ok {
			roots = append(roots, root.Type())
		} else {
			roots = append(roots, getComponentRoots(c)...)
		}
	}

	removeDuplicates := func(keys []string) []string {
		dedup := map[string]model.Component{}
		result := []string{}
		for _, k := range keys {
			if _, ok := dedup[k]; !ok {
				result = append(result, k)
				dedup[k] = nil
			}
		}

		return result
	}

	return removeDuplicates(roots)
}

func (combiner *Combiner) Name() string {
	return "Graph"
}

func (combiner *Combiner) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {

	for _, c := range combiner.components {
		c.BuildQuery(builder)
	}

	builder.Selection().Update(reveald.WithPageSize(9999))
	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return combiner.handle(r)
}

func (combiner *Combiner) handle(result *reveald.Result) (*reveald.Result, error) {
	return result, nil
}
