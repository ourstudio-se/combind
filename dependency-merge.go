package combind

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

func DependencyMerge(results chan *Combination, dependencies ...[]*SearchBox) chan *Combination {
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
					go func(r *Combination, d *SearchBox) {
						defer wg.Done()
						ts := map[string]*SearchBox{
							d.Type: d,
						}
						for _, t := range r.Types {
							ts[t.Type] = t
						}
						matches := MergeArr(r.Matches, d.Matches)

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
				go func(sbi *SearchBox, sbj *SearchBox) {
					defer wg.Done()
					theseResults <- &Combination{
						Types: map[string]*SearchBox{
							sbi.Type: sbi,
							sbj.Type: sbj,
						},
						Matches: MergeArr(sbi.Matches, sbj.Matches),
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
				types := map[string]*SearchBox{}
				for k, v := range tr.Types {
					types[k] = v
				}
				for k, v := range pr.Types {
					types[k] = v
				}
				newResults <- &Combination{
					Types:   types,
					Matches: MergeArr(tr.Matches, pr.Matches),
				}
			}
		}
	}()
	if len(newResults) == 0 {
		return DependencyMerge(tResults, dependencies[2:]...)
	}

	return DependencyMerge(newResults, dependencies[2:]...)
}
