package combind

import (
	"encoding/json"
)

func MergeArr(m1 []Key, m2 []Key) []Key {
	result := []Key{}

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

func MergeKey(m1 Key, m2 Key) (Key, bool) {

	b1, err := json.Marshal(m1)
	if err != nil {
		return Key{}, false
	}
	b2, err := json.Marshal(m2)
	if err != nil {
		return Key{}, false
	}

	m1Map := map[string]string{}
	m2Map := map[string]string{}

	if err := json.Unmarshal(b1, &m1Map); err != nil {
		return Key{}, false
	}
	if err := json.Unmarshal(b2, &m2Map); err != nil {
		return Key{}, false
	}

	for k, v1 := range m1Map {
		if v2, ok := m2Map[k]; ok && v1 != v2 {
			return Key{}, false
		}
	}
	for k, v2 := range m2Map {
		if v1, ok := m1Map[k]; ok && v1 != v2 {
			return Key{}, false
		}
	}

	for k, v2 := range m2Map {
		m1Map[k] = v2
	}

	var resultKey Key
	rb, err := json.Marshal(m1Map)
	if err != nil {
		return Key{}, false
	}

	if err := json.Unmarshal(rb, &resultKey); err != nil {
		return Key{}, false
	}

	return resultKey, true
}

func DedupKeys(d []Key) []Key {
	dedupMatches := map[string]map[string]string{}
	for _, match := range d {
		dedupMatches[Hash(match)] = match
	}
	matches := []Key{}
	for _, key := range dedupMatches {
		matches = append(matches, key)
	}

	return matches
}
