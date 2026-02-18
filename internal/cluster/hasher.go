package cluster

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// VirtualNodeCount is the number of virtual nodes per physical node
const VirtualNodeCount = 150

// HashRing represents a consistent hash ring
type HashRing struct {
	hashFunction func([]byte) uint32
	virtualNodes map[uint32]string // hash -> node ID
	physicalNodes map[string]int   // node ID -> virtual node count
	nodes        map[string]*Node  // node ID -> node info
	sortedHashes []uint32
	lock         sync.RWMutex
}

// NewHashRing creates a new consistent hash ring
func NewHashRing() *HashRing {
	return &HashRing{
		hashFunction:   defaultHashFunction,
		virtualNodes:   make(map[uint32]string),
		physicalNodes:  make(map[string]int),
		nodes:          make(map[string]*Node),
		sortedHashes:   make([]uint32, 0),
	}
}

// SetHashFunction sets a custom hash function
func (r *HashRing) SetHashFunction(fn func([]byte) uint32) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.hashFunction = fn
	r.rebuild()
}

// AddNode adds a node to the ring
func (r *HashRing) AddNode(node *Node) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Skip if already exists
	if _, ok := r.nodes[node.ID]; ok {
		return
	}

	r.nodes[node.ID] = node

	// Add virtual nodes
	for i := 0; i < VirtualNodeCount; i++ {
		key := r.getVirtualNodeKey(node.ID, i)
		hash := r.hashFunction([]byte(key))
		r.virtualNodes[hash] = node.ID
		r.sortedHashes = append(r.sortedHashes, hash)
	}

	r.physicalNodes[node.ID] = VirtualNodeCount
	sort.Slice(r.sortedHashes, func(i, j int) bool {
		return r.sortedHashes[i] < r.sortedHashes[j]
	})
}

// RemoveNode removes a node from the ring
func (r *HashRing) RemoveNode(nodeID string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Check if node exists
	if _, ok := r.nodes[nodeID]; !ok {
		return
	}

	delete(r.nodes, nodeID)

	// Remove virtual nodes
	for i := 0; i < VirtualNodeCount; i++ {
		key := r.getVirtualNodeKey(nodeID, i)
		hash := r.hashFunction([]byte(key))
		delete(r.virtualNodes, hash)
	}

	delete(r.physicalNodes, nodeID)
	r.rebuild()
}

// GetNode returns the primary node for a key
func (r *HashRing) GetNode(key string) (string, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if len(r.nodes) == 0 {
		return "", false
	}

	hash := r.hashFunction([]byte(key))
	idx := r.search(hash)

	// Get the node ID from virtual node
	virtualHash := r.sortedHashes[idx]
	nodeID := r.virtualNodes[virtualHash]

	return nodeID, true
}

// GetNNodes returns N nodes for replication
func (r *HashRing) GetNNodes(key string, n int) []string {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if len(r.nodes) == 0 {
		return nil
	}

	hash := r.hashFunction([]byte(key))
	idx := r.search(hash)

	result := make([]string, 0, n)
	seen := make(map[string]bool)

	// Walk through the ring
	for i := 0; i < len(r.sortedHashes) && len(result) < n; i++ {
		virtualHash := r.sortedHashes[(idx+i)%len(r.sortedHashes)]
		nodeID := r.virtualNodes[virtualHash]

		if !seen[nodeID] {
			seen[nodeID] = true
			result = append(result, nodeID)
		}
	}

	return result
}

// GetNodesInRange returns nodes in a key range
func (r *HashRing) GetNodesInRange(startKey, endKey string, n int) []string {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if len(r.nodes) == 0 {
		return nil
	}

	startHash := r.hashFunction([]byte(startKey))
	endHash := r.hashFunction([]byte(endKey))

	// Handle wrap-around
	if endHash < startHash {
		// Range spans the wrap-around point
		result := make([]string, 0)
		seen := make(map[string]bool)

		// Get nodes from start to end of ring
		for _, hash := range r.sortedHashes {
			if hash >= startHash {
				nodeID := r.virtualNodes[hash]
				if !seen[nodeID] {
					seen[nodeID] = true
					result = append(result, nodeID)
					if len(result) >= n {
						return result
					}
				}
			}
		}

		// Get nodes from start of ring to end
		for _, hash := range r.sortedHashes {
			if hash <= endHash {
				nodeID := r.virtualNodes[hash]
				if !seen[nodeID] {
					seen[nodeID] = true
					result = append(result, nodeID)
					if len(result) >= n {
						return result
					}
				}
			}
		}

		return result
	}

	// Normal range
	result := make([]string, 0, n)
	seen := make(map[string]bool)

	startIdx := r.search(startHash)
	for i := 0; i < len(r.sortedHashes) && len(result) < n; i++ {
		hash := r.sortedHashes[(startIdx+i)%len(r.sortedHashes)]
		if hash > endHash {
			break
		}

		nodeID := r.virtualNodes[hash]
		if !seen[nodeID] {
			seen[nodeID] = true
			result = append(result, nodeID)
		}
	}

	return result
}

// NodeCount returns the number of physical nodes
func (r *HashRing) NodeCount() int {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return len(r.nodes)
}

// GetNodes returns all nodes
func (r *HashRing) GetNodes() map[string]*Node {
	r.lock.RLock()
	defer r.lock.RUnlock()

	result := make(map[string]*Node, len(r.nodes))
	for id, node := range r.nodes {
		result[id] = node
	}
	return result
}

// GetNodeDistribution returns distribution statistics
func (r *HashRing) GetNodeDistribution() map[string]int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	distribution := make(map[string]int)
	for _, nodeID := range r.virtualNodes {
		distribution[nodeID]++
	}
	return distribution
}

// search finds the index of the first hash >= key
func (r *HashRing) search(key uint32) int {
	idx := sort.Search(len(r.sortedHashes), func(i int) bool {
		return r.sortedHashes[i] >= key
	})

	// Wrap around if not found
	if idx >= len(r.sortedHashes) {
		idx = 0
	}

	return idx
}

// rebuild rebuilds the sorted hashes slice
func (r *HashRing) rebuild() {
	r.sortedHashes = make([]uint32, 0, len(r.virtualNodes))
	for hash := range r.virtualNodes {
		r.sortedHashes = append(r.sortedHashes, hash)
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool {
		return r.sortedHashes[i] < r.sortedHashes[j]
	})
}

// getVirtualNodeKey generates the key for a virtual node
func (r *HashRing) getVirtualNodeKey(nodeID string, virtualIndex int) string {
	return fmt.Sprintf("%d-%d", r.hashFunction([]byte(nodeID)), virtualIndex)
}

// defaultHashFunction is the default hash function
func defaultHashFunction(key []byte) uint32 {
	return crc32.ChecksumIEEE(key)
}

// HashFunc is a type for hash functions
type HashFunc func([]byte) uint32

// KetamaHashFunction creates a ketama-style hash function
func KetamaHashFunction() HashFunc {
	return func(key []byte) uint32 {
		// Use CRC32 with different seed for better distribution
		hash := crc32.ChecksumIEEE(key)
		// Rotate bits for better distribution
		return (hash >> 16) | (hash << 16)
	}
}

// MD5HashFunction creates an MD5-based hash function
func MD5HashFunction() HashFunc {
	return func(key []byte) uint32 {
		// Simple hash based on FNV-1a
		hash := uint32(2166136261)
		for _, b := range key {
			hash ^= uint32(b)
			hash *= 16777619
		}
		return hash
	}
}

// MurmurHashFunction creates a MurmurHash-style hash function
func MurmurHashFunction() HashFunc {
	return func(key []byte) uint32 {
		const (
			c1 = 0xcc9e2d51
			c2 = 0x1b873593
			r1 = 15
			r2 = 13
			m  = 5
			n  = 0xe6546b64
		)

		hash := uint32(2166136261)
		nBlocks := len(key) / 4

		for i := 0; i < nBlocks; i++ {
			k := uint32(key[i*4]) | uint32(key[i*4+1])<<8 | uint32(key[i*4+2])<<16 | uint32(key[i*4+3])<<24
			k *= c1
			k = (k << r1) | (k >> (32 - r1))
			k *= c2
			hash ^= k
			hash = (hash << r2) | (hash >> (32 - r2))
			hash = hash*m + n
		}

		// Handle remaining bytes
		tail := len(key) % 4
		if tail > 0 {
			k := uint32(0)
			for i := 0; i < tail; i++ {
				k ^= uint32(key[nBlocks*4+i]) << (i * 8)
			}
			k *= c1
			k = (k << r1) | (k >> (32 - r1))
			k *= c2
			hash ^= k
		}

		hash ^= uint32(len(key))
		hash ^= hash >> 16
		hash *= 0x85ebca6b
		hash ^= hash >> 13
		hash *= 0xc2b2ae35
		hash ^= hash >> 16

		return hash
	}
}
