package model

import (
	"context"
	"math"
	"sync"

	"github.com/ourstudio-se/combind/persistence"

	"github.com/ourstudio-se/combind/utils/arrayutils"
	"github.com/ourstudio-se/combind/utils/keyutils"
	"github.com/ourstudio-se/combind/utils/maputils"
	"github.com/reveald/reveald"
	log "github.com/sirupsen/logrus"
)

type VirtualComponent struct {
	typ           string
	combiner      Combiner
	dependencies  map[string]Component
	rules         []Rule
	noMappingRule Rule
	result        []*persistence.SearchBox
	maxNrMatches  int
	props         map[string]interface{}
	queryBuilder  QueryBuilder
}

type Combination struct {
	Types   map[string]*persistence.SearchBox
	Matches []persistence.Key
}

// type Combination map[string]*persistence.SearchBox

type Rule func(combination *Combination) (*persistence.SearchBox, bool)

type Combiner func(dependency map[string][]*persistence.SearchBox) chan *Combination

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
		for _, r := range rule {
			vc.rules = append(vc.rules, r)
		}
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

func (vc *VirtualComponent) defaultNoMappingRule(combination *Combination) (*persistence.SearchBox, bool) {

	return &persistence.SearchBox{
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

func (vc *VirtualComponent) Build(ctx context.Context, rebuild bool) ([]*persistence.SearchBox, error) {

	if vc.result != nil && !rebuild {
		return vc.result, nil
	}

	builtDependencies := map[string][]*persistence.SearchBox{}
	for typ, dependency := range vc.dependencies {
		dependencyBuild, err := dependency.Build(ctx, false)
		if err != nil {
			return nil, err
		}
		builtDependencies[typ] = dependencyBuild
	}

	results := map[string]*persistence.SearchBox{}
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

				result.Props = maputils.Merge(vc.props, result.Props)

				nrMatches = nrMatches + 1
				resultMutex.Lock()
				if _, ok := results[result.Key]; !ok {
					results[result.Key] = result
				}
				results[result.Key].Matches = append(results[result.Key].Matches, result.Matches...)

				for _, k := range results[result.Key].Matches {
					mappedKeys[keyutils.Hash(k)] = true
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
		result.Props = maputils.Merge(vc.props, result.Props)

		if _, ok := results[result.Key]; !ok {
			results[result.Key] = result
		}

		ummappedKeys := []persistence.Key{}
		for _, m := range result.Matches {
			if _, ok := mappedKeys[keyutils.Hash(m)]; !ok {
				ummappedKeys = append(ummappedKeys, m)
			}
		}

		results[result.Key].Matches = append(results[result.Key].Matches, ummappedKeys...)
	}

	buildResults := []*persistence.SearchBox{}

	for _, c := range results {
		c.Matches = arrayutils.DedupKeys(c.Matches)
		buildResults = append(buildResults, c)
	}

	vc.result = buildResults

	return buildResults, nil
}

func (vc *VirtualComponent) BuildQuery(builder *reveald.QueryBuilder) {
	vc.queryBuilder(builder)
}
