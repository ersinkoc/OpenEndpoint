package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	Storage   StorageConfig   `mapstructure:"storage"`
	Auth      AuthConfig     `mapstructure:"auth"`
	Cluster   ClusterConfig  `mapstructure:"cluster"`
	Federation FederationConfig `mapstructure:"federation"`
	CDN      CDNConfig      `mapstructure:"cdn"`
	Tenant   TenantConfig   `mapstructure:"tenant"`
	Tiering  TieringConfig  `mapstructure:"tiering"`
	Dedup    DedupConfig    `mapstructure:"deduplication"`
	Analytics AnalyticsConfig `mapstructure:"analytics"`
	Backup   BackupConfig   `mapstructure:"backup"`
	Metrics  MetricsConfig  `mapstructure:"metrics"`
	TLS      TLSConfig      `mapstructure:"tls"`
	LogLevel string         `mapstructure:"log_level"`
}

type ServerConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	ReadTimeout  int    `mapstructure:"read_timeout"`
	WriteTimeout int    `mapstructure:"write_timeout"`
	IdleTimeout  int    `mapstructure:"idle_timeout"`
}

type StorageConfig struct {
	DataDir            string `mapstructure:"data_dir"`
	MaxObjectSize      int64  `mapstructure:"max_object_size"`
	MaxBuckets         int    `mapstructure:"max_buckets"`
	EnableCompression  bool   `mapstructure:"enable_compression"`
	StorageBackend     string `mapstructure:"storage_backend"` // flatfile, packed
}

type AuthConfig struct {
	SecretKey     string `mapstructure:"secret_key"`
	AccessKey     string `mapstructure:"access_key"`
	SessionExpiry int    `mapstructure:"session_expiry"` // in hours
}

type ClusterConfig struct {
	Enabled         bool   `mapstructure:"enabled"`
	NodeID          string `mapstructure:"node_id"`
	BindAddr        string `mapstructure:"bind_addr"`
	PeerPorts       string `mapstructure:"peer_ports"`
	JoinPeers       string `mapstructure:"join_peers"`
	ReplicationFactor int   `mapstructure:"replication_factor"`
}

type MetricsConfig struct {
	Enabled     bool   `mapstructure:"enabled"`
	Port        int    `mapstructure:"port"`
	Path        string `mapstructure:"path"`
}

type TLSConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	CertFile   string `mapstructure:"cert_file"`
	KeyFile    string `mapstructure:"key_file"`
}

type LoggingConfig struct {
	Level      string `mapstructure:"level"`       // debug, info, warn, error
	Format     string `mapstructure:"format"`       // json, text
	Output     string `mapstructure:"output"`       // stdout, file
	File       string `mapstructure:"file"`         // log file path
	MaxSize    int    `mapstructure:"max_size"`     // max size in MB before rotation
	MaxBackups int    `mapstructure:"max_backups"`  // number of backup files
	MaxAge     int    `mapstructure:"max_age"`      // days to keep backups
	Compress   bool   `mapstructure:"compress"`    // compress rotated logs
}

type FederationConfig struct {
	Enabled     bool     `mapstructure:"enabled"`
	RegionCode  string   `mapstructure:"region_code"`
	RegionName  string   `mapstructure:"region_name"`
	Endpoint    string   `mapstructure:"endpoint"`
	Country     string   `mapstructure:"country"`
	Continent   string   `mapstructure:"continent"`
	Peers       []string `mapstructure:"peers"`
	SyncInterval int     `mapstructure:"sync_interval"` // seconds
}

type CDNConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	Provider   string `mapstructure:"provider"` // cloudflare, fastly, akamai, cloudfront
	APIKey     string `mapstructure:"api_key"`
	ZoneID     string `mapstructure:"zone_id"`
	Domain     string `mapstructure:"domain"`
	CacheTTL   int    `mapstructure:"cache_ttl"` // seconds
}

type TenantConfig struct {
	Enabled        bool  `mapstructure:"enabled"`
	DefaultQuota   QuotaConfig `mapstructure:"default_quota"`
}

type QuotaConfig struct {
	StorageBytes int64 `mapstructure:"storage_bytes"`
	ObjectCount  int64 `mapstructure:"object_count"`
	BucketCount  int   `mapstructure:"bucket_count"`
	APIRequests  int64 `mapstructure:"api_requests"`
}

type TieringConfig struct {
	Enabled bool          `mapstructure:"enabled"`
	Tiers   []TierConfig `mapstructure:"tiers"`
}

type TierConfig struct {
	Name       string `mapstructure:"name"`
	MinAgeDays int   `mapstructure:"min_age_days"`
	MaxSizeGB  int64 `mapstructure:"max_size_gb"`
}

type DedupConfig struct {
	Enabled      bool  `mapstructure:"enabled"`
	MinSizeBytes int64 `mapstructure:"min_size_bytes"`
}

type AnalyticsConfig struct {
	Enabled        bool `mapstructure:"enabled"`
	ReportInterval int  `mapstructure:"report_interval"` // seconds
	RetentionDays  int  `mapstructure:"retention_days"`
}

type BackupConfig struct {
	Enabled  bool            `mapstructure:"enabled"`
	Targets  []BackupTarget `mapstructure:"targets"`
}

type BackupTarget struct {
	Name      string `mapstructure:"name"`
	Type      string `mapstructure:"type"` // s3, gcs, azure, nfs, local
	Endpoint  string `mapstructure:"endpoint"`
	Bucket    string `mapstructure:"bucket"`
	Prefix    string `mapstructure:"prefix"`
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
}

func Load(path string) (*Config, error) {
	v := viper.New()

	// Set defaults
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 9000)
	v.SetDefault("server.read_timeout", 30)
	v.SetDefault("server.write_timeout", 30)
	v.SetDefault("server.idle_timeout", 60)

	v.SetDefault("storage.data_dir", "/var/lib/openendpoint")
	v.SetDefault("storage.max_object_size", 5*1024*1024*1024) // 5GB
	v.SetDefault("storage.max_buckets", 100)
	v.SetDefault("storage.enable_compression", false)
	v.SetDefault("storage.storage_backend", "flatfile")

	v.SetDefault("auth.secret_key", "")
	v.SetDefault("auth.access_key", "")
	v.SetDefault("auth.session_expiry", 24)

	v.SetDefault("cluster.enabled", false)
	v.SetDefault("cluster.node_id", "")
	v.SetDefault("cluster.bind_addr", "0.0.0.0")
	v.SetDefault("cluster.peer_ports", "9001")
	v.SetDefault("cluster.replication_factor", 1)

	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.port", 9090)
	v.SetDefault("metrics.path", "/metrics")

	v.SetDefault("tls.enabled", false)
	v.SetDefault("tls.cert_file", "")
	v.SetDefault("tls.key_file", "")

	v.SetDefault("log_level", "info")

	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("logging.output", "stdout")
	v.SetDefault("logging.file", "/var/log/openendpoint/server.log")
	v.SetDefault("logging.max_size", 100)
	v.SetDefault("logging.max_backups", 7)
	v.SetDefault("logging.max_age", 30)
	v.SetDefault("logging.compress", true)

	// If config path provided, read from it
	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	} else {
		// Try to find config in common locations
		v.SetConfigName("openendpoint")
		v.AddConfigPath(".")
		v.AddConfigPath("$HOME/.config/openendpoint")
		v.AddConfigPath("/etc/openendpoint")

		// Allow environment variables
		v.SetEnvPrefix("OPENEP")
		v.AutomaticEnv()

		// Ignore error if no config file found
		v.ReadInConfig()
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate required fields
	if cfg.Auth.SecretKey == "" {
		cfg.Auth.SecretKey = os.Getenv("OPENEP_SECRET_KEY")
	}
	if cfg.Auth.AccessKey == "" {
		cfg.Auth.AccessKey = os.Getenv("OPENEP_ACCESS_KEY")
	}

	return &cfg, nil
}
