package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// Validate validates the configuration
func (c *Config) Validate() error {
	// Validate server config
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}

	// Validate storage config
	if c.Storage.DataDir == "" {
		return fmt.Errorf("storage data directory is required")
	}

	// Check if data directory is writable
	if err := isWritable(c.Storage.DataDir); err != nil {
		return fmt.Errorf("storage data directory is not writable: %w", err)
	}

	// Validate auth config
	if c.Auth.SecretKey == "" {
		return fmt.Errorf("auth secret key is required")
	}
	if len(c.Auth.SecretKey) < 8 {
		return fmt.Errorf("auth secret key must be at least 8 characters")
	}

	// Validate cluster config
	if c.Cluster.Enabled {
		if c.Cluster.NodeID == "" {
			return fmt.Errorf("cluster node ID is required when clustering is enabled")
		}
		if c.Cluster.ReplicationFactor < 1 || c.Cluster.ReplicationFactor > 7 {
			return fmt.Errorf("cluster replication factor must be between 1 and 7")
		}
	}

	return nil
}

// isWritable checks if a directory is writable
func isWritable(path string) error {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}

	// Test write permission
	testFile := filepath.Join(path, ".write_test")
	if err := os.WriteFile(testFile, []byte(""), 0644); err != nil {
		return err
	}
	os.Remove(testFile)

	return nil
}

// GetDataDir returns the absolute path to the data directory
func (c *Config) GetDataDir() string {
	if filepath.IsAbs(c.Storage.DataDir) {
		return c.Storage.DataDir
	}

	// Relative path - make it absolute based on current working directory
	absPath, _ := filepath.Abs(c.Storage.DataDir)
	return absPath
}

// GetAddr returns the server address
func (c *Config) GetAddr() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}

// GetMetricsAddr returns the metrics server address
func (c *Config) GetMetricsAddr() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Metrics.Port)
}

// SetDefaults sets default values
func (c *Config) SetDefaults() {
	// Server defaults
	if c.Server.Host == "" {
		c.Server.Host = "0.0.0.0"
	}
	if c.Server.Port == 0 {
		c.Server.Port = 9000
	}
	if c.Server.ReadTimeout == 0 {
		c.Server.ReadTimeout = 30
	}
	if c.Server.WriteTimeout == 0 {
		c.Server.WriteTimeout = 30
	}
	if c.Server.IdleTimeout == 0 {
		c.Server.IdleTimeout = 60
	}

	// Storage defaults
	if c.Storage.DataDir == "" {
		c.Storage.DataDir = "/var/lib/openendpoint"
	}
	if c.Storage.MaxObjectSize == 0 {
		c.Storage.MaxObjectSize = 5 * 1024 * 1024 * 1024 // 5GB
	}
	if c.Storage.MaxBuckets == 0 {
		c.Storage.MaxBuckets = 100
	}
	if c.Storage.StorageBackend == "" {
		c.Storage.StorageBackend = "flatfile"
	}

	// Metrics defaults
	if c.Metrics.Port == 0 {
		c.Metrics.Port = 9090
	}
	if c.Metrics.Path == "" {
		c.Metrics.Path = "/metrics"
	}

	// Log level defaults
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
}

// Normalize normalizes configuration values
func (c *Config) Normalize() {
	// Normalize paths
	c.Storage.DataDir = filepath.Clean(c.Storage.DataDir)

	// Normalize storage backend
	c.Storage.StorageBackend = strings.ToLower(c.Storage.StorageBackend)

	// Normalize log level
	c.LogLevel = strings.ToLower(c.LogLevel)
}

// Save saves the configuration to a file
func (c *Config) Save(path string) error {
	v := viper.New()
	v.SetConfigFile(path)
	v.Set("server", c.Server)
	v.Set("storage", c.Storage)
	v.Set("auth", c.Auth)
	v.Set("cluster", c.Cluster)
	v.Set("metrics", c.Metrics)
	v.Set("tls", c.TLS)
	v.Set("log_level", c.LogLevel)

	return v.WriteConfig()
}

// applyEnvOverrides applies environment variable overrides
func applyEnvOverrides(cfg *Config) {
	// Server
	if v := os.Getenv("OPENEP_SERVER_HOST"); v != "" {
		cfg.Server.Host = v
	}
	if v := os.Getenv("OPENEP_SERVER_PORT"); v != "" {
		cfg.Server.Port = parsePort(v)
	}

	// Storage
	if v := os.Getenv("OPENEP_STORAGE_DATA_DIR"); v != "" {
		cfg.Storage.DataDir = v
	}

	// Auth
	if v := os.Getenv("OPENEP_AUTH_SECRET_KEY"); v != "" {
		cfg.Auth.SecretKey = v
	}
	if v := os.Getenv("OPENEP_AUTH_ACCESS_KEY"); v != "" {
		cfg.Auth.AccessKey = v
	}

	// Log level
	if v := os.Getenv("OPENEP_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
}

// parsePort parses a port string
func parsePort(s string) int {
	var port int
	fmt.Sscanf(s, "%d", &port)
	return port
}
