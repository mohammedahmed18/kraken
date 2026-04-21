// Copyright (c) 2016-2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"container/list"
	"sync"
	"time"
)

// lruEntry stores the key and expiration for a cache entry.
type lruEntry struct {
	key       string
	expireAt  time.Time
}

// LRUCache provides a simple LRU cache with both size and TTL limits.
// It uses RWMutex for optimal concurrent access patterns - multiple
// concurrent reads are allowed while writes get exclusive access.
// Internally it uses a doubly-linked list for O(1) LRU operations.
type LRUCache struct {
	mu      sync.RWMutex
	config  LRUCacheConfig
	items   map[string]*list.Element // key -> list element
	order   *list.List               // front = oldest, back = newest
}

// NewLRUCache creates a new LRU cache from the provided configuration.
func NewLRUCache(cfg LRUCacheConfig) *LRUCache {
	cfg = cfg.applyDefaults()
	return &LRUCache{
		config: cfg,
		items:  make(map[string]*list.Element, cfg.Size),
		order:  list.New(),
	}
}

// Has checks if key exists and hasn't expired.
// This operation uses a read lock, allowing concurrent access.
func (c *LRUCache) Has(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	elem, exists := c.items[key]
	if !exists {
		return false
	}
	return time.Now().Before(elem.Value.(*lruEntry).expireAt)
}

// Add marks a key as cached. This operation uses a write lock
// for exclusive access. If the key already exists, its expiration
// time is updated and it is moved to the back (most-recent) of
// the LRU order. If the cache exceeds config.Size, oldest entries
// are evicted.
func (c *LRUCache) Add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	expireTime := now.Add(c.config.TTL)

	// If key already exists, update expiration and move to back.
	if elem, exists := c.items[key]; exists {
		elem.Value.(*lruEntry).expireAt = expireTime
		c.order.MoveToBack(elem)
		return
	}

	// Add new entry at the back (most-recent).
	entry := &lruEntry{key: key, expireAt: expireTime}
	elem := c.order.PushBack(entry)
	c.items[key] = elem

	// Evict expired and oldest entries if needed.
	c.evict(now)
}

// Delete removes a key from the cache.
func (c *LRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, exists := c.items[key]
	if !exists {
		return
	}
	c.order.Remove(elem)
	delete(c.items, key)
}

// Size returns the current number of entries in the cache.
func (c *LRUCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Clear removes all entries from the cache.
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element, c.config.Size)
	c.order.Init()
}

// evict removes expired entries and enforces size limit.
// Caller must hold the write lock.
func (c *LRUCache) evict(now time.Time) {
	// Remove expired entries from the front (oldest first).
	for c.order.Len() > 0 {
		front := c.order.Front()
		entry := front.Value.(*lruEntry)
		if now.Before(entry.expireAt) {
			break
		}
		c.order.Remove(front)
		delete(c.items, entry.key)
	}

	// Enforce size limit by removing oldest entries.
	for c.order.Len() > c.config.Size {
		front := c.order.Front()
		entry := front.Value.(*lruEntry)
		c.order.Remove(front)
		delete(c.items, entry.key)
	}
}
