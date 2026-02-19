package cache

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"
)

// Cache implements an LRU cache
type Cache struct {
	mu       sync.RWMutex
	items    map[string]*list.Element
	lru      *list.List
	maxSize  int
	ttl      time.Duration
	stats    *Stats
}

// Item represents a cache item
type Item struct {
	Key        string
	Value      interface{}
	Expiration time.Time
}

// Stats holds cache statistics
type Stats struct {
	Hits       int64
	Misses     int64
	Evictions  int64
	ItemsCount int64
	BytesSize  int64
	mu         sync.RWMutex
}

// NewCache creates a new LRU cache
func NewCache(maxSize int, ttl time.Duration) *Cache {
	return &Cache{
		items:   make(map[string]*list.Element),
		lru:     list.New(),
		maxSize: maxSize,
		ttl:     ttl,
		stats:   &Stats{},
	}
}

// Get retrieves an item from the cache
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	element, ok := c.items[key]
	if !ok {
		c.stats.mu.Lock()
		c.stats.Misses++
		c.stats.mu.Unlock()
		return nil, false
	}

	item := element.Value.(*Item)

	// Check if expired
	if time.Now().After(item.Expiration) {
		c.mu.RUnlock()
		c.mu.Lock()
		c.removeElement(element)
		c.mu.Unlock()
		c.stats.mu.Lock()
		c.stats.Misses++
		c.stats.mu.Unlock()
		return nil, false
	}

	// Move to front (most recently used)
	c.lru.MoveToFront(element)

	c.stats.mu.Lock()
	c.stats.Hits++
	c.stats.mu.Unlock()

	return item.Value, true
}

// Set stores an item in the cache
func (c *Cache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if key already exists
	if element, ok := c.items[key]; ok {
		c.lru.Remove(element)
	}

	// Create new item
	item := &Item{
		Key:        key,
		Value:      value,
		Expiration: time.Now().Add(c.ttl),
	}

	// Add to front of list
	element := c.lru.PushFront(item)
	c.items[key] = element

	// Evict if over max size
	for c.lru.Len() > c.maxSize {
		c.evictOldest()
	}

	c.stats.mu.Lock()
	c.stats.ItemsCount = int64(c.lru.Len())
	c.stats.mu.Unlock()
}

// SetWithTTL stores an item with custom TTL
func (c *Cache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if key already exists
	if element, ok := c.items[key]; ok {
		c.lru.Remove(element)
	}

	// Create new item
	item := &Item{
		Key:        key,
		Value:      value,
		Expiration: time.Now().Add(ttl),
	}

	// Add to front of list
	element := c.lru.PushFront(item)
	c.items[key] = element

	// Evict if over max size
	for c.lru.Len() > c.maxSize {
		c.evictOldest()
	}

	c.stats.mu.Lock()
	c.stats.ItemsCount = int64(c.lru.Len())
	c.stats.mu.Unlock()
}

// Delete removes an item from the cache
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, ok := c.items[key]; ok {
		c.removeElement(element)
	}
}

// Clear removes all items from the cache
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.lru = list.New()

	c.stats.mu.Lock()
	c.stats.ItemsCount = 0
	c.stats.mu.Unlock()
}

// removeElement removes an element from the cache
func (c *Cache) removeElement(element *list.Element) {
	c.lru.Remove(element)
	delete(c.items, element.Value.(*Item).Key)

	c.stats.mu.Lock()
	c.stats.Evictions++
	c.stats.ItemsCount = int64(c.lru.Len())
	c.stats.mu.Unlock()
}

// evictOldest removes the oldest item from the cache
func (c *Cache) evictOldest() {
	if element := c.lru.Back(); element != nil {
		c.removeElement(element)
	}
}

// Len returns the number of items in the cache
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lru.Len()
}

// Stats returns cache statistics
func (c *Cache) Stats() (int64, int64, int64) {
	c.stats.mu.RLock()
	defer c.stats.mu.RUnlock()
	return c.stats.Hits, c.stats.Misses, c.stats.Evictions
}

// ObjectCache is a specialized cache for objects
type ObjectCache struct {
	*Cache
}

// NewObjectCache creates a new object cache
func NewObjectCache(maxSize int) *ObjectCache {
	return &ObjectCache{
		Cache: NewCache(maxSize, 5*time.Minute), // 5 minute TTL
	}
}

// GetObject retrieves an object from cache
func (oc *ObjectCache) GetObject(bucket, key string) ([]byte, bool) {
	cacheKey := fmt.Sprintf("%s/%s", bucket, key)
	val, ok := oc.Cache.Get(cacheKey)
	if !ok {
		return nil, false
	}
	data, ok := val.([]byte)
	return data, ok
}

// SetObject stores an object in cache
func (oc *ObjectCache) SetObject(bucket, key string, data []byte) {
	cacheKey := fmt.Sprintf("%s/%s", bucket, key)
	oc.Cache.Set(cacheKey, data)
}

// DeleteObject removes an object from cache
func (oc *ObjectCache) DeleteObject(bucket, key string) {
	cacheKey := fmt.Sprintf("%s/%s", bucket, key)
	oc.Cache.Delete(cacheKey)
}

// BucketCache is a specialized cache for buckets
type BucketCache struct {
	*Cache
}

// NewBucketCache creates a new bucket cache
func NewBucketCache() *BucketCache {
	return &BucketCache{
		Cache: NewCache(1000, 10*time.Minute), // 10 minute TTL
	}
}

// GetBuckets retrieves buckets from cache
func (bc *BucketCache) GetBuckets() ([]string, bool) {
	val, ok := bc.Cache.Get("buckets:list")
	if !ok {
		return nil, false
	}
	buckets, ok := val.([]string)
	return buckets, ok
}

// SetBuckets stores bucket list in cache
func (bc *BucketCache) SetBuckets(buckets []string) {
	bc.Cache.Set("buckets:list", buckets)
}

// InvalidateBucketList invalidates the bucket list cache
func (bc *BucketCache) InvalidateBucketList() {
	bc.Cache.Delete("buckets:list")
}

// MetadataCache is a specialized cache for object metadata
type MetadataCache struct {
	*Cache
}

// NewMetadataCache creates a new metadata cache
func NewMetadataCache(maxSize int) *MetadataCache {
	return &MetadataCache{
		Cache: NewCache(maxSize, 2*time.Minute), // 2 minute TTL
	}
}

// GetMetadata retrieves metadata from cache
func (mc *MetadataCache) GetMetadata(bucket, key string) (interface{}, bool) {
	cacheKey := fmt.Sprintf("%s/%s:meta", bucket, key)
	return mc.Cache.Get(cacheKey)
}

// SetMetadata stores metadata in cache
func (mc *MetadataCache) SetMetadata(bucket, key string, meta interface{}) {
	cacheKey := fmt.Sprintf("%s/%s:meta", bucket, key)
	mc.Cache.Set(cacheKey, meta)
}

// DeleteMetadata removes metadata from cache
func (mc *MetadataCache) DeleteMetadata(bucket, key string) {
	cacheKey := fmt.Sprintf("%s/%s:meta", bucket, key)
	mc.Cache.Delete(cacheKey)
}

// StartCleanup starts the cache cleanup goroutine
func (c *Cache) StartCleanup(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.cleanup()
		}
	}
}

// cleanup removes expired items
func (c *Cache) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for element := c.lru.Back(); element != nil; element = element.Prev() {
		item := element.Value.(*Item)
		if now.After(item.Expiration) {
			c.removeElement(element)
		}
	}
}
