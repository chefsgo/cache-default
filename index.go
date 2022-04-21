package default_cache

import (
	"github.com/chefsgo/chef"
)

func Driver() chef.CacheDriver {
	return &defaultCacheDriver{}
}

func init() {
	chef.Register("default", Driver())
}
