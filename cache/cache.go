package cache

import (
	"context"
	"math"
	"sync"
	"time"
)

const DefaultExpiration time.Duration = 0
const NoExpiration time.Duration = -1

type Cache struct {
	mu      sync.RWMutex
	data    map[string]Item
	maxSize int
}

type Item struct {
	Value     interface{}
	ExpiresAt int64
}

func New(ctx context.Context, cleanupInterval time.Duration, maxSize int) *Cache {
	if maxSize < 0 {
		maxSize = 0
	}

	c := &Cache{
		data:    make(map[string]Item, maxSize),
		maxSize: maxSize,
	}

	if cleanupInterval > 0 {
		go c.cleanup(ctx, cleanupInterval)
	}

	return c
}

func (c *Cache) Set(
	key string,
	value interface{},
	ttl time.Duration,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiresAt int64

	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	} else {
		expiresAt = math.MaxInt64
	}

	if c.maxSize > 0 && len(c.data) >= c.maxSize {
		if _, exists := c.data[key]; !exists {
			c.freeSpace()
		}
	}

	c.data[key] = Item{
		Value:     value,
		ExpiresAt: expiresAt,
	}
}

func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	value, ok := c.data[key]
	if !ok {
		return nil, false
	}

	if time.Now().UnixNano() > value.ExpiresAt {
		return nil, false
	}

	return value.Value, true
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.data, key)
}

func (c *Cache) Exists(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	value, ok := c.data[key]
	if !ok {
		return false
	}

	if time.Now().UnixNano() > value.ExpiresAt {
		return false
	}

	return true
}

func (c *Cache) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]string, 0, len(c.data))
	now := time.Now().UnixNano()

	for k, v := range c.data {
		if now <= v.ExpiresAt {
			keys = append(keys, k)
		}
	}

	return keys
}

func (c *Cache) freeSpace() {

	var oldKey string
	var minTime int64 = math.MaxInt64
	var i int = 0

	now := time.Now().UnixNano()

	for k, v := range c.data {
		if now > v.ExpiresAt {
			delete(c.data, k)
			return
		}

		if v.ExpiresAt < minTime {
			minTime = v.ExpiresAt
			oldKey = k
		}

		i++
		if i >= 5 {
			break
		}
	}

	if oldKey != "" {
		delete(c.data, oldKey)
	}
}
