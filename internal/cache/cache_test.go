package cache

import (
	"context"
	"testing"
	"time"
)

func TestNewCache(t *testing.T) {
	cache := NewCache(100, time.Minute)
	if cache == nil {
		t.Fatal("Cache should not be nil")
	}
}

func TestCache_Set(t *testing.T) {
	cache := NewCache(10, time.Minute)

	cache.Set("key1", []byte("value1"))

	value, ok := cache.Get("key1")
	if !ok {
		t.Fatal("Key should exist after set")
	}
	if string(value.([]byte)) != "value1" {
		t.Errorf("Value = %v, want value1", value)
	}
}

func TestCache_Get(t *testing.T) {
	cache := NewCache(10, time.Minute)

	cache.Set("key1", []byte("value1"))

	value, ok := cache.Get("key1")
	if !ok {
		t.Fatal("Key should exist")
	}

	if string(value.([]byte)) != "value1" {
		t.Errorf("Value = %v, want value1", value)
	}
}

func TestCache_Get_NotFound(t *testing.T) {
	cache := NewCache(10, time.Minute)

	_, ok := cache.Get("non-existent")
	if ok {
		t.Error("Key should not exist")
	}
}

func TestCache_Delete(t *testing.T) {
	cache := NewCache(10, time.Minute)

	cache.Set("key1", []byte("value1"))
	cache.Delete("key1")

	_, ok := cache.Get("key1")
	if ok {
		t.Error("Key should be deleted")
	}
}

func TestCache_Clear(t *testing.T) {
	cache := NewCache(10, time.Minute)

	cache.Set("key1", []byte("value1"))
	cache.Set("key2", []byte("value2"))
	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("Len after clear = %d, want 0", cache.Len())
	}
}

func TestCache_Eviction(t *testing.T) {
	cache := NewCache(3, time.Minute) // Only 3 items

	cache.Set("key1", []byte("value1"))
	cache.Set("key2", []byte("value2"))
	cache.Set("key3", []byte("value3"))
	cache.Set("key4", []byte("value4")) // Should trigger eviction

	if cache.Len() > 3 {
		t.Errorf("Len = %d, should be at most 3", cache.Len())
	}
}

func TestCache_LRUEviction(t *testing.T) {
	cache := NewCache(3, time.Minute)

	cache.Set("key1", []byte("value1"))
	cache.Set("key2", []byte("value2"))
	cache.Set("key3", []byte("value3"))

	// Access key1 to make it recently used
	cache.Get("key1")

	// Add new key, should evict key2 (least recently used)
	cache.Set("key4", []byte("value4"))

	// key1 should still exist
	if _, ok := cache.Get("key1"); !ok {
		t.Error("key1 should still exist after LRU eviction")
	}
}

func TestCache_Len(t *testing.T) {
	cache := NewCache(10, time.Minute)

	if cache.Len() != 0 {
		t.Error("New cache should have len 0")
	}

	cache.Set("key1", []byte("value1"))
	if cache.Len() != 1 {
		t.Errorf("Len = %d, want 1", cache.Len())
	}
}

func TestCache_TTL(t *testing.T) {
	cache := NewCache(10, time.Minute)

	cache.SetWithTTL("key1", []byte("value1"), 100*time.Millisecond)

	value, ok := cache.Get("key1")
	if !ok || string(value.([]byte)) != "value1" {
		t.Error("Key should exist immediately after set")
	}

	time.Sleep(150 * time.Millisecond)

	_, ok = cache.Get("key1")
	if ok {
		t.Error("Key should be expired")
	}
}

func TestCache_Concurrent(t *testing.T) {
	cache := NewCache(100, time.Minute)
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := string(rune('A'+id)) + string(rune('0'+j))
				cache.Set(key, []byte("value"))
				cache.Get(key)
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestCache_Stats(t *testing.T) {
	cache := NewCache(10, time.Minute)

	cache.Set("key1", []byte("value1"))
	cache.Get("key1") // hit
	cache.Get("key2") // miss

	hits, misses, evictions := cache.Stats()

	if hits != 1 {
		t.Errorf("Hits = %d, want 1", hits)
	}
	if misses != 1 {
		t.Errorf("Misses = %d, want 1", misses)
	}
	if evictions != 0 {
		t.Errorf("Evictions = %d, want 0", evictions)
	}
}

func TestCache_SetWithTTL(t *testing.T) {
	cache := NewCache(10, time.Minute)

	cache.SetWithTTL("key1", []byte("value1"), time.Second)

	value, ok := cache.Get("key1")
	if !ok {
		t.Fatal("Key should exist")
	}
	if string(value.([]byte)) != "value1" {
		t.Errorf("Value = %v, want value1", value)
	}
}

func TestObjectCache(t *testing.T) {
	oc := NewObjectCache(10)

	oc.SetObject("bucket1", "key1", []byte("data1"))

	data, ok := oc.GetObject("bucket1", "key1")
	if !ok {
		t.Fatal("Object should exist")
	}
	if string(data) != "data1" {
		t.Errorf("Data = %s, want data1", string(data))
	}

	oc.DeleteObject("bucket1", "key1")

	_, ok = oc.GetObject("bucket1", "key1")
	if ok {
		t.Error("Object should be deleted")
	}
}

func TestBucketCache(t *testing.T) {
	bc := NewBucketCache()

	buckets := []string{"bucket1", "bucket2", "bucket3"}
	bc.SetBuckets(buckets)

	result, ok := bc.GetBuckets()
	if !ok {
		t.Fatal("Buckets should exist")
	}
	if len(result) != 3 {
		t.Errorf("Buckets count = %d, want 3", len(result))
	}

	bc.InvalidateBucketList()

	_, ok = bc.GetBuckets()
	if ok {
		t.Error("Buckets should be invalidated")
	}
}

func TestMetadataCache(t *testing.T) {
	mc := NewMetadataCache(10)

	mc.SetMetadata("bucket1", "key1", map[string]string{"content-type": "text/plain"})

	meta, ok := mc.GetMetadata("bucket1", "key1")
	if !ok {
		t.Fatal("Metadata should exist")
	}
	if m, ok := meta.(map[string]string); !ok || m["content-type"] != "text/plain" {
		t.Error("Metadata content-type mismatch")
	}

	mc.DeleteMetadata("bucket1", "key1")

	_, ok = mc.GetMetadata("bucket1", "key1")
	if ok {
		t.Error("Metadata should be deleted")
	}
}

func TestCache_Expiration(t *testing.T) {
	cache := NewCache(10, 50*time.Millisecond)

	cache.Set("key1", []byte("value1"))

	time.Sleep(100 * time.Millisecond)

	_, ok := cache.Get("key1")
	if ok {
		t.Error("Key should be expired")
	}
}

func TestCache_Set_ExistingKey(t *testing.T) {
	cache := NewCache(10, time.Minute)

	cache.Set("key1", []byte("value1"))
	cache.Set("key1", []byte("value2"))

	value, ok := cache.Get("key1")
	if !ok {
		t.Fatal("Key should exist")
	}
	if string(value.([]byte)) != "value2" {
		t.Errorf("Value = %v, want value2", value)
	}
	if cache.Len() != 1 {
		t.Errorf("Len = %d, want 1", cache.Len())
	}
}

func TestCache_SetWithTTL_ExistingKey(t *testing.T) {
	cache := NewCache(10, time.Minute)

	cache.SetWithTTL("key1", []byte("value1"), time.Minute)
	cache.SetWithTTL("key1", []byte("value2"), time.Minute)

	value, ok := cache.Get("key1")
	if !ok {
		t.Fatal("Key should exist")
	}
	if string(value.([]byte)) != "value2" {
		t.Errorf("Value = %v, want value2", value)
	}
	if cache.Len() != 1 {
		t.Errorf("Len = %d, want 1", cache.Len())
	}
}

func TestCache_SetWithTTL_Eviction(t *testing.T) {
	cache := NewCache(3, time.Minute)

	cache.SetWithTTL("key1", []byte("value1"), time.Minute)
	cache.SetWithTTL("key2", []byte("value2"), time.Minute)
	cache.SetWithTTL("key3", []byte("value3"), time.Minute)
	cache.SetWithTTL("key4", []byte("value4"), time.Minute)

	if cache.Len() > 3 {
		t.Errorf("Len = %d, should be at most 3", cache.Len())
	}
}

func TestCache_StartCleanup(t *testing.T) {
	cache := NewCache(10, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		cache.StartCleanup(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("StartCleanup should return when context is cancelled")
	}
}

func TestCache_StartCleanupWithInterval_Ticker(t *testing.T) {
	cache := NewCache(10, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cache.Set("key1", []byte("value1"))

	go cache.StartCleanupWithInterval(ctx, 50*time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	if cache.Len() != 0 {
		t.Errorf("Cache should be empty after ticker cleanup, got %d items", cache.Len())
	}
}

func TestCache_Cleanup(t *testing.T) {
	cache := NewCache(10, 50*time.Millisecond)

	cache.Set("key1", []byte("value1"))
	cache.SetWithTTL("key2", []byte("value2"), 200*time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	cache.cleanup()

	_, ok := cache.Get("key2")
	if !ok {
		t.Error("key2 should still exist (not expired)")
	}
}
