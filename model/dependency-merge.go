package model

import (
	"reflect"
	"sync"

	"github.com/ourstudio-se/combind/persistence"

	"github.com/ourstudio-se/combind/utils/arrayutils"
	log "github.com/sirupsen/logrus"
)

func DependencyMerge(results chan *Combination, dependencies ...[]*persistence.SearchBox) chan *Combination {
	if len(dependencies) == 0 {
		return results
	}
	if len(dependencies) == 1 {
		resultChan := make(chan *Combination)
		go func() {
			defer close(resultChan)
			wg := sync.WaitGroup{}
			for r := range results {
				for _, d := range dependencies[0] {
					wg.Add(1)
					go func(r *Combination, d *persistence.SearchBox) {
						defer wg.Done()
						ts := map[string]*persistence.SearchBox{
							d.Type: d,
						}
						for _, t := range r.Types {
							ts[t.Type] = t
						}
						matches := arrayutils.MergeArr(r.Matches, d.Matches)

						if len(matches) == 0 {
							return
						}

						resultChan <- &Combination{
							Types:   ts,
							Matches: matches,
						}
					}(r, d)
				}
			}
			wg.Wait()
		}()
		return resultChan
	}

	theseResults := make(chan *Combination)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, sbi := range dependencies[0] {
			for _, sbj := range dependencies[1] {
				wg.Add(1)
				go func(sbi *persistence.SearchBox, sbj *persistence.SearchBox) {
					defer wg.Done()
					theseResults <- &Combination{
						Types: map[string]*persistence.SearchBox{
							sbi.Type: sbi,
							sbj.Type: sbj,
						},
						Matches: arrayutils.MergeArr(sbi.Matches, sbj.Matches),
					}
				}(sbi, sbj)
			}
		}
	}()
	go func() {
		wg.Wait()
		close(theseResults)
	}()

	newResults := make(chan *Combination)
	tResults := make(chan *Combination)

	go func() {
		count := 0
		defer close(tResults)
		defer close(newResults)
		for tr := range theseResults {
			if count%10000 == 0 {
				log.Debugf("Ran %d", count)
			}
			count++
			tResults <- tr
			for pr := range results {
				types := map[string]*persistence.SearchBox{}
				for k, v := range tr.Types {
					types[k] = v
				}
				for k, v := range pr.Types {
					types[k] = v
				}
				newResults <- &Combination{
					Types:   types,
					Matches: arrayutils.MergeArr(tr.Matches, pr.Matches),
				}
			}
		}
	}()
	if len(newResults) == 0 {
		return DependencyMerge(tResults, dependencies[2:]...)
	}

	return DependencyMerge(newResults, dependencies[2:]...)
}

func MutuallyExclusive(dependencies ...[]*persistence.SearchBox) bool {
	exists := make(map[string]bool)

	for _, dependency := range dependencies {
		fields := Dependencies(dependency[0].Matches[0])
		for _, f := range fields {
			if exists[f] {
				return false
			}
			exists[f] = true
		}

	}

	return true
}

func recursiveComb(op persistence.SearchBox, ch chan Combination, dep2 ...[]*persistence.SearchBox) {}

func createMECombinations(op persistence.SearchBox, ch chan Combination, dep2 ...[]*persistence.SearchBox) {

	keys := op.Matches
	types1 := map[string]*persistence.SearchBox{}
	types1[op.Type] = &op

	if len(dep2) == 1 {
		for _, t2 := range dep2 {
			for _, t3 := range t2 {
				keys = arrayutils.MergeArr(keys, t3.Matches)
				types1[t3.Type] = t3
			}
		}
		ch <- Combination{
			Types:   types1,
			Matches: keys,
		}
	}
	if len(dep2) == 2 {
		for _, t2 := range dep2[0] {
			types := map[string]*persistence.SearchBox{}
			types[op.Type] = &op
			t2cop := &t2
			innerKeys := arrayutils.MergeArr(keys, t2.Matches)
			types[t2.Type] = *t2cop
			for _, t3 := range dep2[1] {
				k := arrayutils.MergeArr(innerKeys, t3.Matches)
				t3cop := &t3
				types[t3.Type] = *t3cop
				ch <- Combination{
					Types:   types,
					Matches: k,
				}
			}
		}

	}

	if len(dep2) > 2 {
		return

	}

}

func createNonMECombinations(op persistence.SearchBox, ch chan Combination, dep2 ...[]*persistence.SearchBox) {

	keys := op.Matches

	types := map[string]*persistence.SearchBox{}

	types[op.Type] = &op

	for _, t2 := range dep2 {
		for _, t3 := range t2 {
			keys = arrayutils.IntersectArr(keys, t3.Matches)
			types[t3.Type] = t3
		}

	}
	ch <- Combination{
		Types:   types,
		Matches: keys,
	}

}

func Dependencies(mat persistence.Key) []string {
	result := []string{}

	val := reflect.ValueOf(mat)

	for i := 0; i < val.NumField(); i++ {
		f := val.Field(i)

		if f.Interface() != "" {
			result = append(result, val.Type().Field(i).Name)
		}
	}

	return result
}
