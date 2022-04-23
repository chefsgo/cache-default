package default_cache

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/chefsgo/base"
	"github.com/chefsgo/cache"
)

var (
	errInvalidCacheConnection = errors.New("Invalid cache connection.")
	errInvalidCacheData       = errors.New("Invalid cache data.")
)

type (
	defaultDriver  struct{}
	defaultConnect struct {
		mutex   sync.RWMutex
		name    string
		config  cache.Config
		setting defaultSetting
		caches  sync.Map
	}
	defaultSetting struct {
	}
	defaultValue struct {
		Value  Any
		Expiry time.Time
	}
)

//连接
func (driver *defaultDriver) Connect(name string, config cache.Config) (cache.Connect, error) {
	setting := defaultSetting{}

	return &defaultConnect{
		name: name, config: config, setting: setting,
		caches: sync.Map{},
	}, nil
}

//打开连接
func (connect *defaultConnect) Open() error {
	return nil
}

//关闭连接
func (connect *defaultConnect) Close() error {
	return nil
}

//查询缓存，
func (connect *defaultConnect) Read(key string) (Any, error) {
	if value, ok := connect.caches.Load(key); ok {
		if vv, ok := value.(defaultValue); ok {
			if vv.Expiry.Unix() > time.Now().Unix() {
				return vv.Value, nil
			} else {
				//过期了就删除
				connect.Delete(key)
			}
		}
	}
	return nil, errInvalidCacheData
}

//更新缓存
func (connect *defaultConnect) Write(key string, val Any, expiry time.Duration) error {
	now := time.Now()

	value := defaultValue{
		Value: val, Expiry: now.Add(expiry),
	}

	connect.caches.Store(key, value)

	return nil
}

//查询缓存，
func (connect *defaultConnect) Exists(key string) (bool, error) {
	if _, ok := connect.caches.Load(key); ok {
		return ok, nil
	}
	return false, errors.New("缓存读取失败")
}

//删除缓存
func (connect *defaultConnect) Delete(key string) error {
	connect.caches.Delete(key)
	return nil
}

func (connect *defaultConnect) Serial(key string, start, step int64) (int64, error) {
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

func (connect *defaultConnect) Keys(prefix string) ([]string, error) {
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
func (connect *defaultConnect) Clear(prefix string) error {
	if keys, err := connect.Keys(prefix); err == nil {
		for _, key := range keys {
			connect.caches.Delete(key)
		}
		return nil
	} else {
		return err
	}
}
