package cache

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestCache_SetGetDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := New(ctx, 1*time.Minute, 10)
	c.Set("k1", "v1", 1*time.Minute)

	val, ok := c.Get("k1")
	if !ok || val != "v1" {
		t.Errorf("Ожидали получить v1, получили %v", val)
	}

	if !c.Exists("k1") {
		t.Errorf("Ожидали, что ключ k1 существует")
	}

	c.Delete("k1")
	if c.Exists("k1") {
		t.Errorf("Ожидали, что ключ k1 удален")
	}
}

func TestCache_Expiration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := New(ctx, 1*time.Minute, 10)

	c.Set("k1", "v1", 50*time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	_, ok := c.Get("k1")
	if ok {
		t.Errorf("Ожидали, что ключ k1 протухнет, но он доступен")
	}
}

func TestCache_MaxSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := New(ctx, 1*time.Minute, 3)

	c.Set("k1", "v1", 1*time.Minute)
	c.Set("k2", "v2", 1*time.Minute)
	c.Set("k3", "v3", 1*time.Minute)

	c.Set("k4", "v4", 1*time.Minute)

	keys := c.Keys()
	if len(keys) != 3 {
		t.Errorf("Ожидали, что в кэше останется 3 элемента, получили %d", len(keys))
	}
}

func TestCache_BackgroundCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := New(ctx, 100*time.Millisecond, 10)

	c.Set("k1", "v1", 50*time.Millisecond)

	time.Sleep(250 * time.Millisecond)

	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.data) != 0 {
		t.Errorf("Ожидали, что мапа будет пустой, но там %d элементов", len(c.data))
	}
}

func TestCache_ConcurrentAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	c := New(ctx, 1*time.Minute, 1000)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			key := "key" + strconv.Itoa(i%10)

			c.Set(key, i, 1*time.Minute)
			c.Get(key)
			c.Exists(key)
		}(i)
	}

	wg.Wait()
}

func TestCache_CancelContext(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    
    c := New(ctx, 10*time.Millisecond, 1000)
    
    c.Set("k1", "v1", 20*time.Millisecond)

    cancel()

    time.Sleep(50 * time.Millisecond)

    c.mu.RLock()
    defer c.mu.RUnlock()
    if len(c.data) != 1 {
        t.Errorf("Ожидали, что мапа останется с 1 элементом, но там %d элементов", len(c.data))
    }
}
