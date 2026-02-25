package settings

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Manager manages application settings
type Manager struct {
	mu       sync.RWMutex
	settings map[string]interface{}
	filePath string
}

// NewManager creates a new settings manager
func NewManager(filePath string) *Manager {
	m := &Manager{
		settings: make(map[string]interface{}),
		filePath: filePath,
	}
	// Load settings from file if exists
	m.load()
	return m
}

// Get returns a setting value
func (m *Manager) Get(key string) (interface{}, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.settings[key]
	return val, ok
}

// GetString returns a string setting
func (m *Manager) GetString(key string, defaultValue string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if val, ok := m.settings[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return defaultValue
}

// GetBool returns a bool setting
func (m *Manager) GetBool(key string, defaultValue bool) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if val, ok := m.settings[key]; ok {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return defaultValue
}

// Set sets a setting value
func (m *Manager) Set(key string, value interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.settings[key] = value
}

// SetMultiple sets multiple settings at once
func (m *Manager) SetMultiple(settings map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range settings {
		m.settings[k] = v
	}
}

// GetAll returns all settings
func (m *Manager) GetAll() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]interface{})
	for k, v := range m.settings {
		result[k] = v
	}
	return result
}

// Save saves settings to file
func (m *Manager) Save() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, err := json.MarshalIndent(m.settings, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal settings: %w", err)
	}

	if err := os.WriteFile(m.filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write settings file: %w", err)
	}

	return nil
}

// load loads settings from file
func (m *Manager) load() {
	data, err := os.ReadFile(m.filePath)
	if err != nil {
		// Use defaults if file doesn't exist
		m.settings = m.defaults()
		return
	}

	var newSettings map[string]interface{}
	if err := json.Unmarshal(data, &newSettings); err != nil {
		// Use defaults if parsing fails
		m.settings = m.defaults()
		return
	}
	m.settings = newSettings
}

// defaults returns default settings
func (m *Manager) defaults() map[string]interface{} {
	return map[string]interface{}{
		"region":            "us-east-1",
		"storageClass":      "STANDARD",
		"objectLock":        false,
		"publicAccessBlock": true,
		"serverEncryption":  true,
		"auditLogging":      true,
	}
}

// Load loads settings from file (public method for reloading)
func (m *Manager) Load() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.load()
}
