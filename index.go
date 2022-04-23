package default_cache

import (
	"github.com/chefsgo/cache"
)

func Driver() cache.Driver {
	return &defaultDriver{}
}

func init() {
	cache.Register("default", Driver())
}
