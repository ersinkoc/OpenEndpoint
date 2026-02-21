package cache

import (
	"testing"
	"time"
)

func TestNewCache(t *testing.T) {
	cache := NewCache(100)
	if cache == nil {
		t.Fatal("Cache should not be nil")
	}
}

func TestCache_Set(t *testing.T) {
	cache := NewCache(10)

	err := cache.Set("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
}

func TestCache_Get(t *testing.T) {
	cache := NewCache(10)

	cache.Set("key1", []byte("value1"))

	value, ok := cache.Get("key1")
	if !ok {
		t.Fatal("Key should exist")
	}

	if string(value) != "value1" {
		t.Errorf("Value = %s, want value1", string(value))
	}
}

func TestCache_Get_NotFound(t *testing.T) {
	cache := NewCache(10)

	_, ok := cache.Get("non-existent")
	if ok {
		t.Error("Key should not exist")
	}
}

func TestCache_Delete(t *testing.T) {
	cache := NewCache(10)

	cache.Set("key1", []byte("value1"))
	cache.Delete("key1")

	_, ok := cache.Get("key1")
	if ok {
		t.Error("Key should be deleted")
	}
}

func TestCache_Clear(t *testing.T) {
	cache := NewCache(10)

	cache.Set("key1", []byte("value1"))
	cache.Set("key2", []byte("value2"))
	cache.Clear()

	if cache.Size() != 0 {
		t.Errorf("Size after clear = %d, want 0", cache.Size())
	}
}

func TestCache_Eviction(t *testing.T) {
	cache := NewCache(3) // Only 3 items

	cache.Set("key1", []byte("value1"))
	cache.Set("key2", []byte("value2"))
	cache.Set("key3", []byte("value3"))
	cache.Set("key4", []byte("value4")) // Should trigger eviction

	if cache.Size() > 3 {
		t.Errorf("Size = %d, should be at most 3", cache.Size())
	}
}

func TestCache_LRUEviction(t *testing.T) {
	cache := NewCache(3)

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

func TestCache_Size(t *testing.T) {
	cache := NewCache(10)

	if cache.Size() != 0 {
		t.Error("New cache should have size 0")
	}

	cache.Set("key1", []byte("value1"))
	if cache.Size() != 1 {
		t.Errorf("Size = %d, want 1", cache.Size())
	}
}

func TestCache_TTL(t *testing.T) {
	cache := NewCache(10)

	cache.SetWithTTL("key1", []byte("value1"), 100*time.Millisecond)

	value, ok := cache.Get("key1")
	if !ok || string(value) != "value1" {
		t.Error("Key should exist immediately after set")
	}

	time.Sleep(150 * time.Millisecond)

	_, ok = cache.Get("key1")
	if ok {
		t.Error("Key should be expired")
	}
}

func TestCache_Concurrent(t *testing.T) {
	cache := NewCache(100)
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

func TestCache_Has(t *testing.T) {
	cache := NewCache(10)

	cache.Set("key1", []byte("value1"))

	if !cache.Has("key1") {
		t.Error("Has should return true for existing key")
	}

	if cache.Has("non-existent") {
		t.Error("Has should return false for non-existent key")
	}
}

func TestCache_Keys(t *testing.T) {
	cache := NewCache(10)

	cache.Set("key1", []byte("value1"))
	cache.Set("key2", []byte("value2"))
	cache.Set("key3", []byte("value3"))

	keys := cache.Keys()
	if len(keys) != 3 {
		t.Errorf("Keys count = %d, want 3", len(keys))
	}
}
