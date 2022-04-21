package default_cache

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/chefsgo/base"
	"github.com/chefsgo/chef"
)

var (
	errInvalidCacheConnection = errors.New("Invalid cache connection.")
)

type (
	defaultCacheDriver  struct{}
	defaultCacheConnect struct {
		mutex   sync.RWMutex
		name    string
		config  chef.CacheConfig
		setting defaultCacheSetting
		caches  sync.Map
	}
	defaultCacheSetting struct {
		Expiry time.Duration
	}
	defaultCacheValue struct {
		Value  Any
		Expiry time.Time
	}
)

//连接
func (driver *defaultCacheDriver) Connect(name string, config chef.CacheConfig) (chef.CacheConnect, error) {
	setting := defaultCacheSetting{}

	return &defaultCacheConnect{
		name: name, config: config, setting: setting,
		caches: sync.Map{},
	}, nil
}

//打开连接
func (connect *defaultCacheConnect) Open() error {
	return nil
}

//关闭连接
func (connect *defaultCacheConnect) Close() error {
	return nil
}

//查询缓存，
func (connect *defaultCacheConnect) Read(key string) (Any, error) {
	if value, ok := connect.caches.Load(key); ok {
		if vv, ok := value.(defaultCacheValue); ok {
			if vv.Expiry.Unix() > time.Now().Unix() {
				return vv.Value, nil
			} else {
				//过期了就删除
				connect.Delete(key)
			}
		}
	}
	return nil, errInvalidCacheConnection
}

//更新缓存
func (connect *defaultCacheConnect) Write(key string, val Any, expiry time.Duration) error {
	now := time.Now()

	value := defaultCacheValue{
		Value: val, Expiry: now.Add(connect.setting.Expiry),
	}

	connect.caches.Store(key, value)

	return nil
}

//查询缓存，
func (connect *defaultCacheConnect) Exists(key string) (bool, error) {
	if _, ok := connect.caches.Load(key); ok {
		return ok, nil
	}
	return false, errors.New("缓存读取失败")
}

//删除缓存
func (connect *defaultCacheConnect) Delete(key string) error {
	connect.caches.Delete(key)
	return nil
}

func (connect *defaultCacheConnect) Serial(key string, start, step int64) (int64, error) {
	value := start

	if val, err := connect.Read(key); err == nil {
		if vv, ok := val.(float64); ok {
			value = int64(vv)
		} else if vv, ok := val.(int64); ok {
			value = vv
		}
	}

	value += step

	//写入值
	err := connect.Write(key, value, 0)
	if err != nil {
		return int64(0), err
	}

	return value, nil
}

func (connect *defaultCacheConnect) Keys(prefix string) ([]string, error) {
	keys := []string{}

	connect.caches.Range(func(k, _ Any) bool {
		key := fmt.Sprintf("%v", k)

		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return true
	})
	return keys, nil
}
func (connect *defaultCacheConnect) Clear(prefix string) error {
	if keys, err := connect.Keys(prefix); err == nil {
		for _, key := range keys {
			connect.caches.Delete(key)
		}
		return nil
	} else {
		return err
	}
}
