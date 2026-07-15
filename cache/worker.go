package cache

import "time"

func (c *Cache) cleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now().UnixNano()

		c.mu.Lock()

		for k, v := range c.data {
			if now > v.ExpiresAt {
				delete(c.data, k)
			}
		}

		c.mu.Unlock()
	}
}
