package cache

import (
	"context"
	"time"
)

func (c *Cache) cleanup(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
        return 
    }
	
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now().UnixNano()

			c.mu.Lock()

			for k, v := range c.data {
				if now > v.ExpiresAt {
					delete(c.data, k)
				}
			}

			c.mu.Unlock()
		case <-ctx.Done():
			return
		}
	}

}
