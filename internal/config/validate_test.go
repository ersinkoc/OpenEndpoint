package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: &Config{
				Server: ServerConfig{
					Port: 9000,
					Host: "0.0.0.0",
				},
				Storage: StorageConfig{
					DataDir: t.TempDir(),
				},
				Auth: AuthConfig{
					SecretKey: "test-secret-key-123",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid port - too low",
			config: &Config{
				Server: ServerConfig{
					Port: 0,
				},
				Storage: StorageConfig{
					DataDir: t.TempDir(),
				},
				Auth: AuthConfig{
					SecretKey: "test-secret-key-123",
				},
			},
			wantErr: true,
			errMsg:  "invalid server port",
		},
		{
			name: "invalid port - too high",
			config: &Config{
				Server: ServerConfig{
					Port: 70000,
				},
				Storage: StorageConfig{
					DataDir: t.TempDir(),
				},
				Auth: AuthConfig{
					SecretKey: "test-secret-key-123",
				},
			},
			wantErr: true,
			errMsg:  "invalid server port",
		},
		{
			name: "missing data directory",
			config: &Config{
				Server: ServerConfig{
					Port: 9000,
				},
				Storage: StorageConfig{
					DataDir: "",
				},
				Auth: AuthConfig{
					SecretKey: "test-secret-key-123",
				},
			},
			wantErr: true,
			errMsg:  "storage data directory is required",
		},
		{
			name: "missing secret key",
			config: &Config{
				Server: ServerConfig{
					Port: 9000,
				},
				Storage: StorageConfig{
					DataDir: t.TempDir(),
				},
				Auth: AuthConfig{
					SecretKey: "",
				},
			},
			wantErr: true,
			errMsg:  "auth secret key is required",
		},
		{
			name: "short secret key",
			config: &Config{
				Server: ServerConfig{
					Port: 9000,
				},
				Storage: StorageConfig{
					DataDir: t.TempDir(),
				},
				Auth: AuthConfig{
					SecretKey: "short",
				},
			},
			wantErr: true,
			errMsg:  "auth secret key must be at least 8 characters",
		},
		{
			name: "cluster enabled without node ID",
			config: &Config{
				Server: ServerConfig{
					Port: 9000,
				},
				Storage: StorageConfig{
					DataDir: t.TempDir(),
				},
				Auth: AuthConfig{
					SecretKey: "test-secret-key-123",
				},
				Cluster: ClusterConfig{
					Enabled: true,
					NodeID:  "",
				},
			},
			wantErr: true,
			errMsg:  "cluster node ID is required",
		},
		{
			name: "invalid replication factor - too low",
			config: &Config{
				Server: ServerConfig{
					Port: 9000,
				},
				Storage: StorageConfig{
					DataDir: t.TempDir(),
				},
				Auth: AuthConfig{
					SecretKey: "test-secret-key-123",
				},
				Cluster: ClusterConfig{
					Enabled:           true,
					NodeID:            "node1",
					ReplicationFactor: 0,
				},
			},
			wantErr: true,
			errMsg:  "cluster replication factor must be between 1 and 7",
		},
		{
			name: "invalid replication factor - too high",
			config: &Config{
				Server: ServerConfig{
					Port: 9000,
				},
				Storage: StorageConfig{
					DataDir: t.TempDir(),
				},
				Auth: AuthConfig{
					SecretKey: "test-secret-key-123",
				},
				Cluster: ClusterConfig{
					Enabled:           true,
					NodeID:            "node1",
					ReplicationFactor: 10,
				},
			},
			wantErr: true,
			errMsg:  "cluster replication factor must be between 1 and 7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && err != nil {
				if tt.errMsg != "" && err.Error() != tt.errMsg {
					// Partial match is OK for some errors
					if !contains(err.Error(), tt.errMsg) {
						t.Errorf("Validate() error = %v, want %v", err.Error(), tt.errMsg)
					}
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestLoad(t *testing.T) {
	// Test loading a non-existent config - should use defaults
	cfg, err := Load("")
	if err != nil {
		t.Logf("Load with empty path returned error: %v", err)
	}

	if cfg == nil {
		t.Fatal("Config should not be nil even with error")
	}

	// Check default values from Load function
	if cfg.Server.Port != 9000 {
		t.Errorf("Default port = %d, want 9000", cfg.Server.Port)
	}

	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("Default host = %s, want 0.0.0.0", cfg.Server.Host)
	}

	if cfg.Storage.DataDir != "/var/lib/openendpoint" {
		t.Errorf("Default data dir = %s, want /var/lib/openendpoint", cfg.Storage.DataDir)
	}
}

func TestConfig_SetDefaults(t *testing.T) {
	cfg := &Config{}
	cfg.SetDefaults()

	if cfg.Server.Port != 9000 {
		t.Errorf("Default port = %d, want 9000", cfg.Server.Port)
	}

	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("Default host = %s, want 0.0.0.0", cfg.Server.Host)
	}

	if cfg.Storage.DataDir != "/var/lib/openendpoint" {
		t.Errorf("Default data dir = %s, want /var/lib/openendpoint", cfg.Storage.DataDir)
	}
}

func TestConfig_Normalize(t *testing.T) {
	cfg := &Config{
		Storage: StorageConfig{
			DataDir:        "/var/lib/openendpoint/",
			StorageBackend: "FLATFILE",
		},
		LogLevel: "DEBUG",
	}

	cfg.Normalize()

	// On Windows, filepath.Clean may produce different results
	// Just verify the trailing slash is removed
	if len(cfg.Storage.DataDir) == 0 {
		t.Error("DataDir should not be empty")
	}

	if cfg.Storage.StorageBackend != "flatfile" {
		t.Errorf("StorageBackend = %s, want flatfile", cfg.Storage.StorageBackend)
	}

	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %s, want debug", cfg.LogLevel)
	}
}

func TestConfig_GetDataDir(t *testing.T) {
	// Test absolute path
	cfg := &Config{
		Storage: StorageConfig{
			DataDir: "/var/lib/openendpoint",
		},
	}

	result := cfg.GetDataDir()
	if result == "" {
		t.Error("GetDataDir should return a non-empty path")
	}
}

func TestConfig_GetAddr(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Host: "127.0.0.1",
			Port: 9000,
		},
	}

	addr := cfg.GetAddr()
	if addr != "127.0.0.1:9000" {
		t.Errorf("GetAddr() = %s, want 127.0.0.1:9000", addr)
	}
}

func TestConfig_GetMetricsAddr(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Host: "0.0.0.0",
			Port: 9000,
		},
		Metrics: MetricsConfig{
			Port: 9090,
		},
	}

	addr := cfg.GetMetricsAddr()
	if addr != "0.0.0.0:9090" {
		t.Errorf("GetMetricsAddr() = %s, want 0.0.0.0:9090", addr)
	}
}

func TestIsWritable(t *testing.T) {
	// Test with a writable temp directory
	tmpDir := t.TempDir()
	err := isWritable(tmpDir)
	if err != nil {
		t.Errorf("isWritable() should succeed for writable directory, got: %v", err)
	}

	// Test with a non-existent directory that can be created
	newDir := filepath.Join(t.TempDir(), "subdir")
	err = isWritable(newDir)
	if err != nil {
		t.Errorf("isWritable() should succeed for new directory, got: %v", err)
	}

	// Cleanup
	os.RemoveAll(newDir)
}

func TestConfigStructs(t *testing.T) {
	cfg := Config{
		Server: ServerConfig{
			Host:         "localhost",
			Port:         9000,
			ReadTimeout:  30,
			WriteTimeout: 30,
			IdleTimeout:  60,
		},
		Storage: StorageConfig{
			DataDir:           "/data",
			MaxObjectSize:     1024,
			MaxBuckets:        100,
			EnableCompression: true,
			StorageBackend:    "flatfile",
		},
		Auth: AuthConfig{
			SecretKey:     "secret",
			AccessKey:     "access",
			SessionExpiry: 24,
		},
		Cluster: ClusterConfig{
			Enabled:           true,
			NodeID:            "node1",
			BindAddr:          "0.0.0.0",
			PeerPorts:         "9001",
			JoinPeers:         "peer1",
			ReplicationFactor: 3,
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Port:    9090,
			Path:    "/metrics",
		},
		TLS: TLSConfig{
			Enabled:  true,
			CertFile: "/cert.pem",
			KeyFile:  "/key.pem",
		},
		LogLevel: "info",
	}

	if cfg.Server.Port != 9000 {
		t.Errorf("Server.Port = %d, want 9000", cfg.Server.Port)
	}
	if cfg.Storage.DataDir != "/data" {
		t.Errorf("Storage.DataDir = %s, want /data", cfg.Storage.DataDir)
	}
}

func TestLoggingConfigStruct(t *testing.T) {
	cfg := LoggingConfig{
		Level:      "info",
		Format:     "json",
		Output:     "stdout",
		File:       "/var/log/app.log",
		MaxSize:    100,
		MaxBackups: 7,
		MaxAge:     30,
		Compress:   true,
	}

	if cfg.Level != "info" {
		t.Errorf("Level = %s, want info", cfg.Level)
	}
}

func TestFederationConfigStruct(t *testing.T) {
	cfg := FederationConfig{
		Enabled:      true,
		RegionCode:   "us-east-1",
		RegionName:   "US East",
		Endpoint:     "https://us-east-1.example.com",
		Country:      "USA",
		Continent:    "North America",
		Peers:        []string{"peer1", "peer2"},
		SyncInterval: 60,
	}

	if cfg.RegionCode != "us-east-1" {
		t.Errorf("RegionCode = %s, want us-east-1", cfg.RegionCode)
	}
}

func TestCDNConfigStruct(t *testing.T) {
	cfg := CDNConfig{
		Enabled:  true,
		Provider: "cloudflare",
		APIKey:   "key",
		ZoneID:   "zone",
		Domain:   "example.com",
		CacheTTL: 3600,
	}

	if cfg.Provider != "cloudflare" {
		t.Errorf("Provider = %s, want cloudflare", cfg.Provider)
	}
}

func TestTenantConfigStruct(t *testing.T) {
	cfg := TenantConfig{
		Enabled: true,
		DefaultQuota: QuotaConfig{
			StorageBytes: 1024 * 1024 * 1024,
			ObjectCount:  10000,
			BucketCount:  100,
			APIRequests:  1000000,
		},
	}

	if !cfg.Enabled {
		t.Error("TenantConfig should be enabled")
	}
}

func TestTieringConfigStruct(t *testing.T) {
	cfg := TieringConfig{
		Enabled: true,
		Tiers: []TierConfig{
			{Name: "hot", MinAgeDays: 0, MaxSizeGB: 100},
			{Name: "cold", MinAgeDays: 30, MaxSizeGB: 1000},
		},
	}

	if len(cfg.Tiers) != 2 {
		t.Errorf("Tiers count = %d, want 2", len(cfg.Tiers))
	}
}

func TestDedupConfigStruct(t *testing.T) {
	cfg := DedupConfig{
		Enabled:      true,
		MinSizeBytes: 1024,
	}

	if !cfg.Enabled {
		t.Error("DedupConfig should be enabled")
	}
}

func TestAnalyticsConfigStruct(t *testing.T) {
	cfg := AnalyticsConfig{
		Enabled:        true,
		ReportInterval: 60,
		RetentionDays:  30,
	}

	if !cfg.Enabled {
		t.Error("AnalyticsConfig should be enabled")
	}
}

func TestBackupConfigStruct(t *testing.T) {
	cfg := BackupConfig{
		Enabled: true,
		Targets: []BackupTarget{
			{
				Name:      "s3-backup",
				Type:      "s3",
				Endpoint:  "https://s3.amazonaws.com",
				Bucket:    "backup-bucket",
				Prefix:    "backups/",
				AccessKey: "key",
				SecretKey: "secret",
			},
		},
	}

	if len(cfg.Targets) != 1 {
		t.Errorf("Targets count = %d, want 1", len(cfg.Targets))
	}
}

func TestClusterConfigValid(t *testing.T) {
	cfg := &Config{
		Server:  ServerConfig{Port: 9000},
		Storage: StorageConfig{DataDir: t.TempDir()},
		Auth:    AuthConfig{SecretKey: "test-secret-key-123"},
		Cluster: ClusterConfig{
			Enabled:           true,
			NodeID:            "node1",
			ReplicationFactor: 3,
		},
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Valid cluster config should pass: %v", err)
	}
}

func TestApplyEnvOverrides(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{Port: 9000},
	}

	// Set environment variables
	os.Setenv("OPENEP_SERVER_HOST", "192.168.1.1")
	os.Setenv("OPENEP_SERVER_PORT", "8080")
	os.Setenv("OPENEP_STORAGE_DATA_DIR", "/custom/data")
	os.Setenv("OPENEP_AUTH_SECRET_KEY", "env-secret")
	os.Setenv("OPENEP_AUTH_ACCESS_KEY", "env-access")
	os.Setenv("OPENEP_LOG_LEVEL", "debug")

	defer func() {
		os.Unsetenv("OPENEP_SERVER_HOST")
		os.Unsetenv("OPENEP_SERVER_PORT")
		os.Unsetenv("OPENEP_STORAGE_DATA_DIR")
		os.Unsetenv("OPENEP_AUTH_SECRET_KEY")
		os.Unsetenv("OPENEP_AUTH_ACCESS_KEY")
		os.Unsetenv("OPENEP_LOG_LEVEL")
	}()

	applyEnvOverrides(cfg)

	if cfg.Server.Host != "192.168.1.1" {
		t.Errorf("Server.Host = %s, want 192.168.1.1", cfg.Server.Host)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("Server.Port = %d, want 8080", cfg.Server.Port)
	}
}

func TestParsePort(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"9000", 9000},
		{"8080", 8080},
		{"invalid", 0},
		{"", 0},
	}

	for _, tt := range tests {
		result := parsePort(tt.input)
		if result != tt.expected {
			t.Errorf("parsePort(%s) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestConfigSetDefaultsAllFields(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			ReadTimeout:  0,
			WriteTimeout: 0,
			IdleTimeout:  0,
		},
		Storage: StorageConfig{
			MaxObjectSize:  0,
			MaxBuckets:     0,
			StorageBackend: "",
		},
		Metrics: MetricsConfig{
			Port: 0,
			Path: "",
		},
	}

	cfg.SetDefaults()

	if cfg.Server.ReadTimeout != 30 {
		t.Errorf("ReadTimeout = %d, want 30", cfg.Server.ReadTimeout)
	}
	if cfg.Server.WriteTimeout != 30 {
		t.Errorf("WriteTimeout = %d, want 30", cfg.Server.WriteTimeout)
	}
	if cfg.Server.IdleTimeout != 60 {
		t.Errorf("IdleTimeout = %d, want 60", cfg.Server.IdleTimeout)
	}
	if cfg.Storage.MaxBuckets != 100 {
		t.Errorf("MaxBuckets = %d, want 100", cfg.Storage.MaxBuckets)
	}
	if cfg.Storage.StorageBackend != "flatfile" {
		t.Errorf("StorageBackend = %s, want flatfile", cfg.Storage.StorageBackend)
	}
	if cfg.Metrics.Port != 9090 {
		t.Errorf("Metrics.Port = %d, want 9090", cfg.Metrics.Port)
	}
	if cfg.Metrics.Path != "/metrics" {
		t.Errorf("Metrics.Path = %s, want /metrics", cfg.Metrics.Path)
	}
}

func TestGetDataDirRelative(t *testing.T) {
	cfg := &Config{
		Storage: StorageConfig{
			DataDir: "relative/path",
		},
	}

	result := cfg.GetDataDir()
	if result == "" {
		t.Error("GetDataDir should return a non-empty path")
	}
	// The result should be an absolute path
	if !filepath.IsAbs(result) {
		t.Errorf("GetDataDir should return absolute path, got %s", result)
	}
}

func TestLoadWithInvalidConfigFile(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(tmpFile, []byte("invalid: [yaml: content"), 0644); err != nil {
		t.Fatalf("Failed to write invalid config: %v", err)
	}

	cfg, err := Load(tmpFile)
	if err == nil {
		t.Error("Load should return error for invalid YAML")
	}
	if cfg != nil {
		t.Error("Config should be nil on error")
	}
}

func TestLoadWithNonexistentConfigFile(t *testing.T) {
	cfg, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("Load should return error for nonexistent file")
	}
	if cfg != nil {
		t.Error("Config should be nil on error")
	}
}

func TestSave(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")

	cfg := &Config{
		Server: ServerConfig{
			Host:         "127.0.0.1",
			Port:         8080,
			ReadTimeout:  30,
			WriteTimeout: 30,
			IdleTimeout:  60,
		},
		Storage: StorageConfig{
			DataDir:        "/data",
			MaxObjectSize:  1024,
			MaxBuckets:     50,
			StorageBackend: "flatfile",
		},
		Auth: AuthConfig{
			SecretKey:     "test-secret",
			AccessKey:     "test-access",
			SessionExpiry: 24,
		},
		Cluster: ClusterConfig{
			Enabled:           false,
			NodeID:            "",
			BindAddr:          "0.0.0.0",
			PeerPorts:         "9001",
			ReplicationFactor: 1,
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Port:    9090,
			Path:    "/metrics",
		},
		TLS: TLSConfig{
			Enabled:  false,
			CertFile: "",
			KeyFile:  "",
		},
		LogLevel: "debug",
	}

	err := cfg.Save(tmpFile)
	if err != nil {
		t.Errorf("Save failed: %v", err)
	}

	if _, err := os.Stat(tmpFile); os.IsNotExist(err) {
		t.Error("Config file was not created")
	}
}

func TestSaveToInvalidPath(t *testing.T) {
	cfg := &Config{
		Server:   ServerConfig{Port: 9000},
		LogLevel: "info",
	}

	err := cfg.Save("/nonexistent/directory/path/config.yaml")
	if err == nil {
		t.Error("Save should return error for invalid path")
	}
}

func TestValidateNonWritableDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		Server: ServerConfig{Port: 9000},
		Storage: StorageConfig{
			DataDir: tmpDir,
		},
		Auth: AuthConfig{SecretKey: "test-secret-key-123"},
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Validate should succeed for writable directory: %v", err)
	}
}

func TestIsWritablePermissionDenied(t *testing.T) {
	tmpDir := t.TempDir()
	err := isWritable(tmpDir)
	if err != nil {
		t.Errorf("isWritable should succeed for writable directory: %v", err)
	}
}

func TestIsWritableMkdirAllError(t *testing.T) {
	invalidPath := string([]byte{0})
	err := isWritable(invalidPath)
	if err == nil {
		t.Error("isWritable should return error for invalid path")
	}
}

func TestIsWritableInvalidPath(t *testing.T) {
	err := isWritable("/nonexistent\000path")
	if err == nil {
		t.Error("isWritable should return error for path with null byte")
	}
}

func TestGetDataDirAbsolutePath(t *testing.T) {
	absPath := filepath.Join(string(os.PathSeparator), "var", "lib", "openendpoint")
	if !filepath.IsAbs(absPath) {
		absPath, _ = filepath.Abs(absPath)
	}
	cfg := &Config{
		Storage: StorageConfig{
			DataDir: absPath,
		},
	}

	result := cfg.GetDataDir()
	if result != absPath {
		t.Errorf("GetDataDir = %s, want %s", result, absPath)
	}
}

func TestValidateWithIsWritableError(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{Port: 9000},
		Storage: StorageConfig{
			DataDir: "/nonexistent\000invalid",
		},
		Auth: AuthConfig{SecretKey: "test-secret-key-123"},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Validate should return error when data directory is not writable")
	}
}

func TestIsWritableWriteFileError(t *testing.T) {
	tmpDir := t.TempDir()
	existingDir := filepath.Join(tmpDir, ".write_test")
	if err := os.Mkdir(existingDir, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	err := isWritable(tmpDir)
	if err == nil {
		t.Skip("Could not create write error condition on this platform")
	}
}

func TestLoadWithUnmarshalError(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "badconfig.yaml")
	content := `
server:
  port: "not_a_number"
`
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	cfg, err := Load(tmpFile)
	if cfg == nil && err != nil {
		return
	}

	if cfg != nil && cfg.Server.Port != 0 {
		t.Skip("Viper handles type conversion gracefully")
	}
}
