package arrayutils

import (
	"encoding/json"
	"sync"

	"github.com/ourstudio-se/combind/persistence"
	"github.com/ourstudio-se/combind/utils/keyutils"
)

func MergeArr(m1 []persistence.Key, m2 []persistence.Key) []persistence.Key {
	result := []persistence.Key{}

	for _, mv1 := range m1 {
		for _, mv2 := range m2 {
			if k, ok := MergeKey(mv1, mv2); ok {
				result = append(result, k)
			}
			if k, ok := MergeKey(mv2, mv1); ok {
				result = append(result, k)
			}
		}
	}

	return DedupKeys(result)
}

func MergeKey(m1 persistence.Key, m2 persistence.Key) (persistence.Key, bool) {

	b1, err := json.Marshal(m1)
	if err != nil {
		return persistence.Key{}, false
	}
	b2, err := json.Marshal(m2)
	if err != nil {
		return persistence.Key{}, false
	}

	m1Map := map[string]string{}
	m2Map := map[string]string{}

	if err := json.Unmarshal(b1, &m1Map); err != nil {
		return persistence.Key{}, false
	}
	if err := json.Unmarshal(b2, &m2Map); err != nil {
		return persistence.Key{}, false
	}

	for k, v1 := range m1Map {
		if v2, ok := m2Map[k]; ok && v1 != v2 {
			return persistence.Key{}, false
		}
	}
	for k, v2 := range m2Map {
		if v1, ok := m1Map[k]; ok && v1 != v2 {
			return persistence.Key{}, false
		}
	}

	for k, v2 := range m2Map {
		m1Map[k] = v2
	}

	var resultKey persistence.Key
	rb, err := json.Marshal(m1Map)
	if err != nil {
		return persistence.Key{}, false
	}

	if err := json.Unmarshal(rb, &resultKey); err != nil {
		return persistence.Key{}, false
	}

	return resultKey, true
}

func DedupKeys(d []persistence.Key) []persistence.Key {
	dedupMatches := map[string]map[string]string{}
	for _, match := range d {
		dedupMatches[keyutils.Hash(match)] = match
	}
	matches := []persistence.Key{}
	for _, key := range dedupMatches {
		matches = append(matches, key)
	}

	return matches
}

type SafeArray struct {
	v   []persistence.Key
	mux sync.Mutex
}

// IntersectArr Merges list of keys only if they share they same property on the key they av in common
func IntersectArr(a []persistence.Key, b []persistence.Key) []persistence.Key {
	//c := []persistence.Key{}
	combinations := make(chan persistence.Key, len(a))
	var wg sync.WaitGroup

	for _, item1 := range a {
		wg.Add(1)
		go func(item1 persistence.Key) {
			defer wg.Done()
			for _, item2 := range b {

				// If keys are not shared, do not merge

				match := false
				for k1, v1 := range item1 {
					for k2, v2 := range item2 {
						if k1 != k2 {
							continue
						}
						if v1 != "" && v1 == v2 {
							match = true
						}
					}
				}

				if match == true {
					if k, ok := MergeKey(item1, item2); ok {
						combinations <- k
					}

				}
			}
		}(item1)
	}
	go func() {
		wg.Wait()
		close(combinations)
	}()

	result := []persistence.Key{}
	for c := range combinations {
		result = append(result, c)
	}
	return DedupKeys(result)
}
