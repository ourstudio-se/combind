package combind

import (
	"context"
	"math"
	"sync"

	"github.com/reveald/reveald"
	log "github.com/sirupsen/logrus"
)

type VirtualComponent struct {
	typ           string
	combiner      Combiner
	dependencies  map[string]Component
	rules         []Rule
	noMappingRule Rule
	result        []*SearchBox
	maxNrMatches  int
	props         map[string]interface{}
	queryBuilder  QueryBuilder
	handler       Handler
}

type Combination struct {
	Types   map[string]*SearchBox
	Matches []Key
}

// type Combination map[string]*SearchBox

type Rule func(combination *Combination) (*SearchBox, bool)

type Combiner func(dependency map[string][]*SearchBox) chan *Combination

type VirtualComponentConfiguration func(*VirtualComponent)

type VirtualComponentProperties interface{}

func WithDependency(comp ...Component) VirtualComponentConfiguration {
	return func(vc *VirtualComponent) {
		for _, c := range comp {
			vc.dependencies[c.Type()] = c
		}
	}
}

func WithRule(rule ...Rule) VirtualComponentConfiguration {
	return func(vc *VirtualComponent) {
		vc.rules = append(vc.rules, rule...)
	}
}

func WithMaxRulesHits(maxRuleHits int) VirtualComponentConfiguration {
	return func(vc *VirtualComponent) {
		vc.maxNrMatches = maxRuleHits
	}
}

func WithNoMappingRule(rule Rule) VirtualComponentConfiguration {
	return func(vc *VirtualComponent) {
		vc.noMappingRule = rule
	}
}

func WithQuery(queryBuilder QueryBuilder) VirtualComponentConfiguration {
	return func(vc *VirtualComponent) {
		vc.queryBuilder = queryBuilder
	}
}

type Handler = func(result *reveald.Result) (*reveald.Result, error)

func WithHandler(handler Handler) VirtualComponentConfiguration {
	return func(vc *VirtualComponent) {
		vc.handler = handler
	}
}

func (vc *VirtualComponent) defaultNoMappingRule(combination *Combination) (*SearchBox, bool) {

	return &SearchBox{
		Key:     "not-mapped",
		Type:    vc.typ,
		Props:   map[string]interface{}{},
		Matches: combination.Matches,
	}, true
}

func NewVirtualComponent(typ string, combiner Combiner, cfg ...VirtualComponentConfiguration) *VirtualComponent {
	vc := &VirtualComponent{
		typ:          typ,
		combiner:     combiner,
		rules:        []Rule{},
		dependencies: map[string]Component{},
		maxNrMatches: math.MaxInt32,
		props:        map[string]interface{}{},
		queryBuilder: func(builder *reveald.QueryBuilder) {

		},
		handler: func(result *reveald.Result) (*reveald.Result, error) {
			return result, nil
		},
	}

	vc.noMappingRule = vc.defaultNoMappingRule

	for _, c := range cfg {
		c(vc)
	}

	return vc
}

func (vc *VirtualComponent) Type() string {
	return vc.typ
}

func (vc *VirtualComponent) Children() []Component {
	children := []Component{}

	for _, c := range vc.dependencies {
		children = append(children, c)
	}

	return children
}

func (vc *VirtualComponent) Build(ctx context.Context, rebuild bool, withProps map[string]interface{}) ([]*SearchBox, error) {

	if vc.result != nil && !rebuild {
		return vc.result, nil
	}

	builtDependencies := map[string][]*SearchBox{}
	for typ, dependency := range vc.dependencies {
		dependencyBuild, err := dependency.Build(ctx, false, withProps)
		if err != nil {
			return nil, err
		}
		builtDependencies[typ] = dependencyBuild
	}

	results := map[string]*SearchBox{}
	resultMutex := sync.RWMutex{}

	mappedKeys := map[string]bool{}
	unmatchedCombinations := []*Combination{}
	unmatchedLock := sync.RWMutex{}
	counter := 0

	worker := func(combinations <-chan *Combination) {
		for combination := range combinations {
			counter++
			if counter%10 == 0 {
				log.Debugf("Processed %d items", counter)
			}
			nrMatches := 0
			for _, rule := range vc.rules {
				result, didMatch := rule(combination)
				if !didMatch {
					continue
				}

				result.Props = Merge(vc.props, result.Props)

				nrMatches = nrMatches + 1
				resultMutex.Lock()
				if _, ok := results[result.Key]; !ok {
					results[result.Key] = result
				}
				results[result.Key].Matches = append(results[result.Key].Matches, result.Matches...)

				for _, k := range results[result.Key].Matches {
					mappedKeys[Hash(k)] = true
				}
				resultMutex.Unlock()

				if nrMatches >= vc.maxNrMatches {
					break
				}
			}

			if nrMatches == 0 {
				unmatchedLock.Lock()
				unmatchedCombinations = append(unmatchedCombinations, combination)
				unmatchedLock.Unlock()
			}
		}
	}
	whg := sync.WaitGroup{}
	ch := vc.combiner(builtDependencies)
	for i := 0; i < 50; i++ {
		whg.Add(1)
		go func() {
			defer whg.Done()
			worker(ch)
		}()
	}

	whg.Wait()

	for _, uc := range unmatchedCombinations {
		result, ok := vc.noMappingRule(uc)
		if !ok {
			log.Warnf("Default rule not matched, this must be an error. Check the default handler for type %s...", vc.typ)
			continue
		}
		result.Props = Merge(vc.props, result.Props)

		if _, ok := results[result.Key]; !ok {
			results[result.Key] = result
		}

		ummappedKeys := []Key{}
		for _, m := range result.Matches {
			if _, ok := mappedKeys[Hash(m)]; !ok {
				ummappedKeys = append(ummappedKeys, m)
			}
		}

		results[result.Key].Matches = append(results[result.Key].Matches, ummappedKeys...)
	}

	buildResults := []*SearchBox{}

	for _, c := range results {
		c.Matches = DedupKeys(c.Matches)
		buildResults = append(buildResults, c)
	}

	vc.result = buildResults

	return buildResults, nil
}

func (vc *VirtualComponent) BuildQuery(builder *reveald.QueryBuilder) {
	vc.queryBuilder(builder)
}

func (vc *VirtualComponent) Handle(result *reveald.Result) (*reveald.Result, error) {
	return vc.handler(result)
}
