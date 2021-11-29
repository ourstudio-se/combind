package combind

import (
	"context"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/reveald/reveald"
	log "github.com/sirupsen/logrus"
)

//Combind for the current relationships
type Combind struct {
	components       map[string]Component
	componentStorage ComponentStorage
	searchStorage    SearchBoxStorage
	roots            map[string][]string
}

type CombinerBuilder interface {
	Update(ctx context.Context, withProps map[string]interface{}, index string, comps ...*BackendComponent) ([]*SearchBox, error)
	Save(ctx context.Context, withProps map[string]interface{}, index string) error
}

// New for modeling the metadata
func New(
	componentStorage ComponentStorage,
	combindStorage SearchBoxStorage,
	components ...Component) *Combind {

	g := &Combind{
		componentStorage: componentStorage,
		searchStorage:    combindStorage,
		components:       map[string]Component{},
		roots:            map[string][]string{},
	}

	g.roots = map[string][]string{}

	for _, c := range components {
		g.roots[c.Type()] = getComponentRoots(c)
		g.components[c.Type()] = c
	}

	return g
}

func (g *Combind) ComponentTypes() []string {
	res := []string{}
	for _, c := range g.components {
		res = append(res, c.Type())
	}

	return res
}

//Save the graph to the provided storage
func (g *Combind) Save(ctx context.Context, withProps map[string]interface{}, index string) error {

	logTime := func(part string, start time.Time) {
		end := time.Now()
		log.Debugf("%s took %d MS", part, end.Sub(start).Milliseconds())
	}

	defer logTime("Total runtime", time.Now())
	results := []*SearchBox{}
	for typ, comp := range g.components {
		start := time.Now()
		log.Debugf("Running %s", typ)
		result, err := comp.Build(ctx, true, withProps)
		if err != nil {
			return err
		}

		logTime(typ, start)
		results = append(results, result...)
	}

	if err := g.searchStorage.Save(ctx, index, results...); err != nil {
		log.Error("Error saving", err)
		return err
	}
	return nil
}

// Update recomputes the updates for the graph
func (combiner *Combind) Update(ctx context.Context, withProps map[string]interface{}, index string, comps ...*BackendComponent) ([]*SearchBox, error) {
	deletedBoxes := []*SearchBox{}
	updatedBoxes := []*SearchBox{}
	createdBoxes := []*SearchBox{}

	for key, component := range combiner.components {
		roots := getComponentRoots(component)
		for _, comp := range comps {
			for _, root := range roots {

				if root == comp.Type {

					boxes, err := combiner.searchStorage.Find(ctx, key, index)
					if err != nil {
						return nil, err
					}
					builds, err := component.Build(ctx, true, withProps)
					if err != nil {
						return nil, err
					}

					builtIndex := map[string]*SearchBox{}
					existingIndex := map[string]*SearchBox{}

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
							//todo: handle this
							_ = append(deletedBoxes, existingBox)
						}
					}
				}
			}
		}
	}

	return append(updatedBoxes, createdBoxes...), nil
}

func getComponentRoots(comp Component) []string {

	if _, ok := comp.(*RootComponent); ok {
		return []string{comp.Type()}
	}

	roots := []string{}
	for _, c := range comp.Children() {
		if root, ok := c.(*RootComponent); ok {
			roots = append(roots, root.Type())
		} else {
			roots = append(roots, getComponentRoots(c)...)
		}
	}

	removeDuplicates := func(keys []string) []string {
		dedup := map[string]Component{}
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

func (combiner *Combind) Name() string {
	return "Graph"
}

func (combiner *Combind) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {

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

func (combiner *Combind) handle(result *reveald.Result) (*reveald.Result, error) {
	baseResult := result
	for _, c := range combiner.components {
		br, err := c.Handle(baseResult)
		baseResult = br
		if err != nil {
			return nil, err
		}
	}
	return baseResult, nil
}
