package combind

import (
	"crypto/md5"
	"fmt"

	log "github.com/sirupsen/logrus"
)

// Hash generates a hash of this key object
func Hash(key map[string]interface{}) string {
	h := md5.New()
	if _, err := h.Write([]byte(fmt.Sprintf("%v", key))); err != nil {
		log.Errorf("error generating has for ket %v", key)
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}
