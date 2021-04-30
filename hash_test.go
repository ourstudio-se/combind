package combind_test

import (
	"fmt"
	"testing"

	"github.com/ourstudio-se/combind"
	"github.com/stretchr/testify/assert"
)

func TestWith2ValuesEquals(t *testing.T) {
	map1 := map[string]interface{}{
		"k1": "v1",
		"k2": "v2",
	}
	map2 := map[string]interface{}{
		"k1": "v1",
		"k2": "v2",
	}

	h1 := combind.Hash(map1)
	h2 := combind.Hash(map2)
	assert.Equal(t, h1, h2)
}

func TestWithAnyNumberOfValuesEquals(t *testing.T) {
	map1 := map[string]interface{}{}
	map2 := map[string]interface{}{}

	for i := 0; i < 1000; i++ {
		map1[fmt.Sprintf("%d", i)] = fmt.Sprintf("%d", i)
		map2[fmt.Sprintf("%d", i)] = fmt.Sprintf("%d", i)

	}

	h1 := combind.Hash(map1)
	h2 := combind.Hash(map2)
	assert.Equal(t, h1, h2)
}

func TestNotEqualWhenDifferentNumberOfParams(t *testing.T) {
	map1 := map[string]interface{}{
		"k1": "v1",
		"k2": "v2",
	}
	map2 := map[string]interface{}{
		"k1": "v1",
	}

	h1 := combind.Hash(map1)
	h2 := combind.Hash(map2)
	assert.NotEqual(t, h1, h2)
}

func TestNotEqualWhenSameNumberOfParams(t *testing.T) {
	map1 := map[string]interface{}{
		"k1": "v1",
		"k2": "v2",
	}
	map2 := map[string]interface{}{
		"k1": "v1",
		"k2": "v22",
	}

	h1 := combind.Hash(map1)
	h2 := combind.Hash(map2)
	assert.NotEqual(t, h1, h2)
}
