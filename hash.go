package combind

import (
	"crypto/md5"
	"fmt"
)

// Hash generates a hash of this key object
func Hash(key map[string]string) string {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("%v", key)))

	return fmt.Sprintf("%x", h.Sum(nil))
}
