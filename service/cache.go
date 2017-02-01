// vim:fileencoding=utf-8
package service

import (
	"github.com/hashicorp/golang-lru"
)

type Cache interface {
	Get(key interface{}) (interface{}, bool)
	Add(key, value interface{})
	Len() int
	Keys() []interface{}
	Remove(key interface{})
	Purge()
	Contains(key interface{}) bool
	Peek(key interface{}) (interface{}, bool)
}

// A wrapper for lrc.Cache since its method set is a bit different.
type lru_cache struct {
	*lru.Cache
}

func (c lru_cache) Add(key, value interface{}) {
	c.Cache.Add(key, value)
}

func (c lru_cache) Remove(key interface{}) {
	c.Cache.Remove(key)
}
