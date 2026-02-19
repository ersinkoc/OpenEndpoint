package packed

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Volume represents a packed volume file
type Volume struct {
	id      uint64
	path    string
	file    *os.File
	writer  *bufio.Writer
	index   *Index
	mu      sync.RWMutex
	size    int64
}

// Index stores needle metadata in memory
type Index struct {
	mu      sync.RWMutex
	entries map[uint64]*Needle
}

// Needle represents a single object in the volume
type Needle struct {
	Key         string
	Offset      int64
	Size        int64
	Cookie      uint32
	LastModified int64
}

// VolumeManager manages multiple volumes
type VolumeManager struct {
	rootDir    string
	volumes    map[uint64]*Volume
	currentID  uint64
	mu         sync.RWMutex
	maxSize    int64
}

// NewVolumeManager creates a new volume manager
func NewVolumeManager(rootDir string, maxSize int64) (*VolumeManager, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}

	vm := &VolumeManager{
		rootDir:  rootDir,
		volumes:  make(map[uint64]*Volume),
		maxSize:  maxSize,
	}

	// Load existing volumes
	if err := vm.loadVolumes(); err != nil {
		return nil, err
	}

	return vm, nil
}

// loadVolumes loads existing volume files
func (vm *VolumeManager) loadVolumes() error {
	entries, err := os.ReadDir(vm.rootDir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".dat" {
			continue
		}

		var id uint64
		if _, err := fmt.Sscanf(e.Name(), "%d.dat", &id); err != nil {
			continue
		}

		vol, err := vm.openVolume(id)
		if err != nil {
			continue
		}

		vm.volumes[id] = vol
		if id > vm.currentID {
			vm.currentID = id
		}
	}

	// Start with volume 1 if none exist
	if vm.currentID == 0 {
		vm.currentID = 1
	}

	return nil
}

// openVolume opens an existing volume
func (vm *VolumeManager) openVolume(id uint64) (*Volume, error) {
	path := filepath.Join(vm.rootDir, fmt.Sprintf("%d.dat", id))
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	vol := &Volume{
		id:     id,
		path:   path,
		file:   file,
		writer: bufio.NewWriter(file),
		index:  NewIndex(),
		size:   info.Size(),
	}

	// Load index
	if err := vol.loadIndex(); err != nil {
		return nil, err
	}

	return vol, nil
}

// NewIndex creates a new index
func NewIndex() *Index {
	return &Index{
		entries: make(map[uint64]*Needle),
	}
}

// Write writes data to a volume
func (v *Volume) Write(key string, data []byte) (int64, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Get current offset
	offset := v.size

	// Create needle
	needle := &Needle{
		Key:          key,
		Offset:       offset,
		Size:         int64(len(data)),
		Cookie:       crc32.ChecksumIEEE([]byte(key)),
		LastModified: time.Now().Unix(),
	}

	// Write header
	header := make([]byte, 24)
	binary.LittleEndian.PutUint64(header[0:8], uint64(needle.Cookie))
	binary.LittleEndian.PutUint64(header[8:16], uint64(needle.Size))
	binary.LittleEndian.PutUint64(header[16:24], uint64(needle.LastModified))

	if _, err := v.writer.Write(header); err != nil {
		return 0, err
	}

	// Write data
	if _, err := v.writer.Write(data); err != nil {
		return 0, err
	}

	// Flush to disk
	if err := v.writer.Flush(); err != nil {
		return 0, err
	}

	// Update index
	v.index.entries[uint64(needle.Cookie)] = needle
	v.size += int64(len(header) + len(data))

	return offset, nil
}

// Read reads data from a volume
func (v *Volume) Read(key string) ([]byte, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	cookie := crc32.ChecksumIEEE([]byte(key))
	needle, ok := v.index.entries[uint64(cookie)]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	// Seek to offset
	if _, err := v.file.Seek(needle.Offset, io.SeekStart); err != nil {
		return nil, err
	}

	// Read header
	header := make([]byte, 24)
	if _, err := io.ReadFull(v.file, header); err != nil {
		return nil, err
	}

	// Read data
	size := int64(binary.LittleEndian.Uint64(header[8:16]))
	data := make([]byte, size)
	if _, err := io.ReadFull(v.file, data); err != nil {
		return nil, err
	}

	return data, nil
}

// Delete marks a key as deleted
func (v *Volume) Delete(key string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	cookie := crc32.ChecksumIEEE([]byte(key))
	delete(v.index.entries, uint64(cookie))
	return nil
}

// Close closes the volume
func (v *Volume) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.writer != nil {
		v.writer.Flush()
	}
	if v.file != nil {
		return v.file.Close()
	}
	return nil
}

// loadIndex loads the index from disk
func (v *Volume) loadIndex() error {
	// For now, rebuild index by scanning
	// In production, you'd have a separate index file
	if _, err := v.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	offset := int64(0)
	for {
		header := make([]byte, 24)
		n, err := io.ReadFull(v.file, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if n < 24 {
			break
		}

		cookie := binary.LittleEndian.Uint64(header[0:8])
		size := int64(binary.LittleEndian.Uint64(header[8:16]))
		lastMod := int64(binary.LittleEndian.Uint64(header[16:24]))

		v.index.entries[cookie] = &Needle{
			Offset:      offset,
			Size:        size,
			Cookie:      uint32(cookie),
			LastModified: lastMod,
		}

		offset += 24 + size
	}

	return nil
}

// Write stores data in a volume
func (vm *VolumeManager) Write(key string, data []byte) (uint64, int64, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	// Find a volume with space or create new one
	vol := vm.getWritableVolume()

	offset, err := vol.Write(key, data)
	if err != nil {
		return 0, 0, err
	}

	return vol.id, offset, nil
}

// Read retrieves data from volumes
func (vm *VolumeManager) Read(key string) ([]byte, error) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	for _, vol := range vm.volumes {
		data, err := vol.Read(key)
		if err == nil {
			return data, nil
		}
	}

	return nil, fmt.Errorf("key not found: %s", key)
}

// Delete removes data from volumes
func (vm *VolumeManager) Delete(key string) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	for _, vol := range vm.volumes {
		if err := vol.Delete(key); err == nil {
			return nil
		}
	}

	return fmt.Errorf("key not found: %s", key)
}

// getWritableVolume returns a volume that can accept new data
func (vm *VolumeManager) getWritableVolume() *Volume {
	for _, vol := range vm.volumes {
		if vol.size < vm.maxSize {
			return vol
		}
	}

	// Create new volume
	vm.currentID++
	vol, _ := vm.openVolume(vm.currentID)
	vm.volumes[vm.currentID] = vol
	return vol
}

// Close closes all volumes
func (vm *VolumeManager) Close() error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	for _, vol := range vm.volumes {
		vol.Close()
	}
	return nil
}

// Stats returns volume statistics
func (vm *VolumeManager) Stats() map[string]interface{} {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["volume_count"] = len(vm.volumes)
	stats["current_volume"] = vm.currentID

	var totalSize int64
	var totalObjects int
	for id, vol := range vm.volumes {
		totalSize += vol.size
		totalObjects += len(vol.index.entries)
		stats[fmt.Sprintf("volume_%d_size", id)] = vol.size
		stats[fmt.Sprintf("volume_%d_objects", id)] = len(vol.index.entries)
	}

	stats["total_size"] = totalSize
	stats["total_objects"] = totalObjects

	return stats
}
