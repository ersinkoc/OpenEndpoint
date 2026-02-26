package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/openendpoint/openendpoint/internal/config"
	"github.com/openendpoint/openendpoint/internal/engine"
	"github.com/openendpoint/openendpoint/internal/metadata/pebble"
	"github.com/openendpoint/openendpoint/internal/storage/flatfile"
	"github.com/spf13/cobra"
)

var cfgPath string

// httpClient is used for CLI commands with timeout
var httpClient = &http.Client{
	Timeout: 10 * time.Second,
}

// Version variable - can be set via ldflags
var version = "v1.0.0"

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "openep",
	Short: "OpenEndpoint CLI",
	Long:  `OpenEndpoint is a self-hosted, S3-compatible object storage platform.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if cfgPath == "" {
			cfgPath = os.Getenv("OPENEP_CONFIG")
		}
	},
}

// ServerCmd starts the OpenEndpoint server
var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "Start OpenEndpoint server",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Starting OpenEndpoint server...")
		// This will be handled by the actual server command
		// Run with: openep serve or openep server
	},
}

// VersionCmd prints version
var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("OpenEndpoint %s\n", version)
	},
}

// BucketCmd manages buckets
var BucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "Manage buckets",
}

// BucketCreateCmd creates a new bucket
var BucketCreateCmd = &cobra.Command{
	Use:   "create [bucket-name]",
	Short: "Create a new bucket",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		bucketName := args[0]
		if err := runBucketCreate(bucketName); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Bucket '%s' created successfully\n", bucketName)
	},
}

// BucketListCmd lists all buckets
var BucketListCmd = &cobra.Command{
	Use:   "ls",
	Short: "List all buckets",
	Run: func(cmd *cobra.Command, args []string) {
		buckets, err := runBucketList()
		if err != nil {
			log.Fatal(err)
		}
		if len(buckets) == 0 {
			fmt.Println("No buckets found")
			return
		}
		fmt.Println("Buckets:")
		for _, b := range buckets {
			fmt.Printf("  %s (created: %s)\n", b.Name, time.Unix(b.CreationDate, 0).Format("2006-01-02 15:04:05"))
		}
	},
}

// BucketDeleteCmd deletes a bucket
var BucketDeleteCmd = &cobra.Command{
	Use:   "rm [bucket-name]",
	Short: "Delete a bucket",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		bucketName := args[0]
		force, _ := cmd.Flags().GetBool("force")
		if !force {
			fmt.Printf("Are you sure you want to delete bucket '%s'? ", bucketName)
			var confirm string
			fmt.Scanln(&confirm)
			if strings.ToLower(confirm) != "y" && strings.ToLower(confirm) != "yes" {
				fmt.Println("Cancelled")
				return
			}
		}
		if err := runBucketDelete(bucketName); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Bucket '%s' deleted successfully\n", bucketName)
	},
}

// BucketInfoCmd shows bucket info
var BucketInfoCmd = &cobra.Command{
	Use:   "info [bucket-name]",
	Short: "Show bucket information",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		bucketName := args[0]
		if err := runBucketInfo(bucketName); err != nil {
			log.Fatal(err)
		}
	},
}

// ObjectCmd manages objects
var ObjectCmd = &cobra.Command{
	Use:   "object",
	Short: "Manage objects",
}

// ObjectPutCmd uploads an object
var ObjectPutCmd = &cobra.Command{
	Use:   "put [source-file] s3://[bucket]/[key]",
	Short: "Upload a file to a bucket",
	Long:  `Upload a file to an S3 bucket. Use s3://bucket/key format for the target.`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		sourceFile := args[0]
		target := args[1]

		bucket, key, err := parseS3Path(target)
		if err != nil {
			log.Fatal(err)
		}

		if err := runObjectPut(sourceFile, bucket, key); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("File '%s' uploaded to s3://%s/%s\n", sourceFile, bucket, key)
	},
}

// ObjectGetCmd downloads an object
var ObjectGetCmd = &cobra.Command{
	Use:   "get s3://[bucket]/[key] [destination-file]",
	Short: "Download a file from a bucket",
	Long:  `Download a file from an S3 bucket.`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		source := args[0]
		destFile := args[1]

		bucket, key, err := parseS3Path(source)
		if err != nil {
			log.Fatal(err)
		}

		if err := runObjectGet(bucket, key, destFile); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("File downloaded to '%s'\n", destFile)
	},
}

// ObjectListCmd lists objects in a bucket
var ObjectListCmd = &cobra.Command{
	Use:   "ls s3://[bucket]",
	Short: "List objects in a bucket",
	Long:  `List objects in an S3 bucket. Use s3://bucket for bucket listing.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		path := args[0]
		bucket, prefix, err := parseS3Path(path)
		if err != nil {
			log.Fatal(err)
		}

		recursive, _ := cmd.Flags().GetBool("recursive")
		long, _ := cmd.Flags().GetBool("long")

		if err := runObjectList(bucket, prefix, recursive, long); err != nil {
			log.Fatal(err)
		}
	},
}

// ObjectDeleteCmd deletes an object
var ObjectDeleteCmd = &cobra.Command{
	Use:   "rm s3://[bucket]/[key]",
	Short: "Delete an object",
	Long:  `Delete an object from a bucket.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		path := args[0]
		bucket, key, err := parseS3Path(path)
		if err != nil {
			log.Fatal(err)
		}

		force, _ := cmd.Flags().GetBool("force")
		if !force {
			fmt.Printf("Are you sure you want to delete s3://%s/%s? ", bucket, key)
			var confirm string
			fmt.Scanln(&confirm)
			if strings.ToLower(confirm) != "y" && strings.ToLower(confirm) != "yes" {
				fmt.Println("Cancelled")
				return
			}
		}

		if err := runObjectDelete(bucket, key); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Object s3://%s/%s deleted successfully\n", bucket, key)
	},
}

// ObjectCopyCmd copies an object
var ObjectCopyCmd = &cobra.Command{
	Use:   "cp s3://[source-bucket]/[source-key] s3://[dest-bucket]/[dest-key]",
	Short: "Copy an object",
	Long:  `Copy an object within or between buckets.`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		source := args[0]
		dest := args[1]

		srcBucket, srcKey, err := parseS3Path(source)
		if err != nil {
			log.Fatal(err)
		}

		dstBucket, dstKey, err := parseS3Path(dest)
		if err != nil {
			log.Fatal(err)
		}

		if err := runObjectCopy(srcBucket, srcKey, dstBucket, dstKey); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Copied s3://%s/%s to s3://%s/%s\n", srcBucket, srcKey, dstBucket, dstKey)
	},
}

// ConfigCmd manages configuration
var ConfigCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage configuration",
}

// ConfigGetCmd gets a configuration value
var ConfigGetCmd = &cobra.Command{
	Use:   "get [key]",
	Short: "Get a configuration value",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		key := args[0]
		cfg, err := getConfig()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s = %v\n", key, getConfigValue(cfg, key))
	},
}

// ConfigSetCmd sets a configuration value
var ConfigSetCmd = &cobra.Command{
	Use:   "set [key] [value]",
	Short: "Set a configuration value",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Configuration changes require server restart")
		fmt.Printf("Set %s = %s in config file\n", args[0], args[1])
	},
}

// AdminCmd admin commands
var AdminCmd = &cobra.Command{
	Use:   "admin",
	Short: "Administrative commands",
}

// AdminInfoCmd shows server info
var AdminInfoCmd = &cobra.Command{
	Use:   "info",
	Short: "Show server information",
	Run: func(cmd *cobra.Command, args []string) {
		cfg, err := getConfig()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("OpenEndpoint Server Information:")
		fmt.Printf("  Version:     %s\n", version)
		fmt.Printf("  API Port:    %d\n", cfg.Server.Port)
		fmt.Printf("  Data Dir:    %s\n", cfg.Storage.DataDir)
		fmt.Printf("  Backend:     %s\n", cfg.Storage.StorageBackend)
		fmt.Printf("  Log Level:   %s\n", cfg.LogLevel)
	},
}

// AdminStatsCmd shows server stats
var AdminStatsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show server statistics",
	Run: func(cmd *cobra.Command, args []string) {
		stats, err := runServerStats()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Server Statistics:")
		for k, v := range stats {
			fmt.Printf("  %s: %v\n", k, v)
		}
	},
}

// MonitorCmd monitors the running server
var MonitorCmd = &cobra.Command{
	Use:   "monitor",
	Short: "Monitor running server",
}

// MonitorStatusCmd shows server status
var MonitorStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show server status from running instance",
	Run: func(cmd *cobra.Command, args []string) {
		status, err := runMonitorStatus()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Server Status:")
		for k, v := range status {
			fmt.Printf("  %s: %v\n", k, v)
		}
	},
}

// MonitorHealthCmd shows server health
var MonitorHealthCmd = &cobra.Command{
	Use:   "health",
	Short: "Show server health check",
	Run: func(cmd *cobra.Command, args []string) {
		healthy, err := runMonitorHealth()
		if err != nil {
			log.Fatal(err)
		}
		if healthy {
			fmt.Println("✓ Server is healthy")
		} else {
			fmt.Println("✗ Server is unhealthy")
		}
	},
}

// MonitorReadyCmd shows server readiness
var MonitorReadyCmd = &cobra.Command{
	Use:   "ready",
	Short: "Show server readiness",
	Run: func(cmd *cobra.Command, args []string) {
		ready, err := runMonitorReady()
		if err != nil {
			log.Fatal(err)
		}
		if ready {
			fmt.Println("✓ Server is ready")
		} else {
			fmt.Println("✗ Server is not ready")
		}
	},
}

// MonitorClusterCmd shows cluster status
var MonitorClusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Show cluster status",
	Run: func(cmd *cobra.Command, args []string) {
		cluster, err := runMonitorCluster()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Cluster Status:")
		for k, v := range cluster {
			if k == "nodes" {
				continue
			}
			fmt.Printf("  %s: %v\n", k, v)
		}
		// Show nodes
		if nodes, ok := cluster["nodes"].([]interface{}); ok && len(nodes) > 0 {
			fmt.Println("  Nodes:")
			for _, n := range nodes {
				if node, ok := n.(map[string]interface{}); ok {
					fmt.Printf("    - %s (%s): %s\n", node["name"], node["address"], node["status"])
				}
			}
		}
	},
}

// MonitorMetricsCmd shows metrics
var MonitorMetricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "Show server metrics (Prometheus format)",
	Run: func(cmd *cobra.Command, args []string) {
		metrics, err := runMonitorMetrics()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(metrics)
	},
}

// MonitorBucketsCmd shows bucket info
var MonitorBucketsCmd = &cobra.Command{
	Use:   "buckets",
	Short: "Show bucket statistics",
	Run: func(cmd *cobra.Command, args []string) {
		buckets, err := runMonitorBuckets()
		if err != nil {
			log.Fatal(err)
		}
		if len(buckets) == 0 {
			fmt.Println("No buckets found")
			return
		}
		fmt.Println("Bucket Statistics:")
		for _, b := range buckets {
			fmt.Printf("  %s: %d objects\n", b.Name, b.ObjectCount)
		}
	},
}

// MonitorWatchCmd watches server in real-time
var MonitorWatchCmd = &cobra.Command{
	Use:   "watch",
	Short: "Watch server metrics in real-time",
	Run: func(cmd *cobra.Command, args []string) {
		interval, _ := cmd.Flags().GetInt("interval")
		runMonitorWatch(interval)
	},
}

// Helper functions

func getConfig() (*config.Config, error) {
	return config.Load(cfgPath)
}

func getEngine() (*engine.ObjectService, error) {
	cfg, err := getConfig()
	if err != nil {
		return nil, err
	}

	storage, err := flatfile.New(cfg.Storage.DataDir)
	if err != nil {
		return nil, err
	}

	metadata, err := pebble.New(cfg.Storage.DataDir)
	if err != nil {
		storage.Close()
		return nil, err
	}

	eng := engine.New(storage, metadata, nil)
	return eng, nil
}

func runBucketCreate(bucket string) error {
	eng, err := getEngine()
	if err != nil {
		return err
	}
	defer eng.Close()

	return eng.CreateBucket(context.Background(), bucket)
}

func runBucketList() ([]engine.BucketInfo, error) {
	eng, err := getEngine()
	if err != nil {
		return nil, err
	}
	defer eng.Close()

	return eng.ListBuckets(context.Background())
}

func runBucketDelete(bucket string) error {
	eng, err := getEngine()
	if err != nil {
		return err
	}
	defer eng.Close()

	return eng.DeleteBucket(context.Background(), bucket)
}

func runBucketInfo(bucket string) error {
	eng, err := getEngine()
	if err != nil {
		return err
	}
	defer eng.Close()

	buckets, err := eng.ListBuckets(context.Background())
	if err != nil {
		return err
	}

	for _, b := range buckets {
		if b.Name == bucket {
			fmt.Printf("Bucket: %s\n", b.Name)
			fmt.Printf("Created: %s\n", time.Unix(b.CreationDate, 0).Format("2006-01-02 15:04:05"))

			// List objects count
			result, err := eng.ListObjects(context.Background(), bucket, engine.ListObjectsOptions{MaxKeys: 1000})
			if err == nil {
				fmt.Printf("Objects: %d\n", len(result.Objects))
			}
			return nil
		}
	}

	return fmt.Errorf("bucket not found: %s", bucket)
}

func runObjectPut(sourceFile, bucket, key string) error {
	eng, err := getEngine()
	if err != nil {
		return err
	}
	defer eng.Close()

	file, err := os.Open(sourceFile)
	if err != nil {
		return err
	}
	defer file.Close()

	opts := engine.PutObjectOptions{}
	_, err = eng.PutObject(context.Background(), bucket, key, file, opts)
	return err
}

func runObjectGet(bucket, key, destFile string) error {
	eng, err := getEngine()
	if err != nil {
		return err
	}
	defer eng.Close()

	result, err := eng.GetObject(context.Background(), bucket, key, engine.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer result.Body.Close()

	// Create directory if needed
	dir := filepath.Dir(destFile)
	os.MkdirAll(dir, 0755)

	file, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, result.Body)
	return err
}

func runObjectList(bucket, prefix string, recursive, long bool) error {
	eng, err := getEngine()
	if err != nil {
		return err
	}
	defer eng.Close()

	opts := engine.ListObjectsOptions{
		Prefix:  prefix,
		MaxKeys: 1000,
	}
	if !recursive {
		opts.Delimiter = "/"
	}

	result, err := eng.ListObjects(context.Background(), bucket, opts)
	if err != nil {
		return err
	}

	if len(result.Objects) == 0 && len(result.CommonPrefixes) == 0 {
		fmt.Printf("No objects in s3://%s/%s\n", bucket, prefix)
		return nil
	}

	// Print common prefixes (directories)
	for _, cp := range result.CommonPrefixes {
		fmt.Printf("PRE %s\n", cp)
	}

	// Print objects
	for _, obj := range result.Objects {
		if long {
			fmt.Printf("%10d %s %s/%s\n", obj.Size, time.Unix(obj.LastModified, 0).Format("2006-01-02 15:04"), bucket, obj.Key)
		} else {
			fmt.Printf("%s\n", obj.Key)
		}
	}

	return nil
}

func runObjectDelete(bucket, key string) error {
	eng, err := getEngine()
	if err != nil {
		return err
	}
	defer eng.Close()

	return eng.DeleteObject(context.Background(), bucket, key, engine.DeleteObjectOptions{})
}

func runObjectCopy(srcBucket, srcKey, dstBucket, dstKey string) error {
	eng, err := getEngine()
	if err != nil {
		return err
	}
	defer eng.Close()

	// Get source object
	result, err := eng.GetObject(context.Background(), srcBucket, srcKey, engine.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer result.Body.Close()

	// Put to destination
	_, err = eng.PutObject(context.Background(), dstBucket, dstKey, result.Body, engine.PutObjectOptions{
		ContentType: result.ContentType,
		Metadata:    result.Metadata,
	})
	return err
}

func runServerStats() (map[string]interface{}, error) {
	eng, err := getEngine()
	if err != nil {
		return nil, err
	}
	defer eng.Close()

	buckets, err := eng.ListBuckets(context.Background())
	if err != nil {
		return nil, err
	}

	stats := make(map[string]interface{})
	stats["bucket_count"] = len(buckets)

	var totalObjects int64
	for _, b := range buckets {
		result, err := eng.ListObjects(context.Background(), b.Name, engine.ListObjectsOptions{MaxKeys: 10000})
		if err == nil {
			totalObjects += int64(len(result.Objects))
		}
	}
	stats["object_count"] = totalObjects

	return stats, nil
}

func parseS3Path(path string) (bucket, key string, err error) {
	// Remove s3:// prefix
	path = strings.TrimPrefix(path, "s3://")

	// Split by first /
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 1 || parts[0] == "" {
		return "", "", fmt.Errorf("invalid S3 path: %s", path)
	}

	bucket = parts[0]
	if len(parts) > 1 {
		key = parts[1]
	}

	return bucket, key, nil
}

func getConfigValue(cfg *config.Config, key string) interface{} {
	switch key {
	case "server.host":
		return cfg.Server.Host
	case "server.port":
		return cfg.Server.Port
	case "storage.data_dir":
		return cfg.Storage.DataDir
	case "storage.backend":
		return cfg.Storage.StorageBackend
	case "log_level":
		return cfg.LogLevel
	default:
		return "unknown"
	}
}

// getServerURL returns the server URL from config
func getServerURL() string {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		// Use environment variable or empty string as fallback
		if url := os.Getenv("OPENEP_SERVER_URL"); url != "" {
			return url
		}
		return ""
	}
	return fmt.Sprintf("http://%s:%d", cfg.Server.Host, cfg.Server.Port)
}

// BucketInfo represents bucket statistics
type BucketInfo struct {
	Name        string `json:"name"`
	ObjectCount int    `json:"objectCount"`
}

func runMonitorStatus() (map[string]interface{}, error) {
	url := getServerURL()
	if url == "" {
		return nil, fmt.Errorf("server URL not configured. Set OPENEP_SERVER_URL environment variable or provide config file")
	}
	resp, err := httpClient.Get(url + "/_mgmt/")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}
	defer resp.Body.Close()

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}
	return status, nil
}

func runMonitorHealth() (bool, error) {
	url := getServerURL()
	if url == "" {
		return false, fmt.Errorf("server URL not configured. Set OPENEP_SERVER_URL environment variable or provide config file")
	}
	resp, err := httpClient.Get(url + "/_mgmt/health")
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}

func runMonitorReady() (bool, error) {
	url := getServerURL()
	if url == "" {
		return false, fmt.Errorf("server URL not configured. Set OPENEP_SERVER_URL environment variable or provide config file")
	}
	resp, err := httpClient.Get(url + "/_mgmt/ready")
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}

func runMonitorCluster() (map[string]interface{}, error) {
	url := getServerURL()
	if url == "" {
		return nil, fmt.Errorf("server URL not configured. Set OPENEP_SERVER_URL environment variable or provide config file")
	}
	resp, err := httpClient.Get(url + "/_mgmt/cluster")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}
	defer resp.Body.Close()

	var cluster map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

func runMonitorMetrics() (string, error) {
	url := getServerURL()
	if url == "" {
		return "", fmt.Errorf("server URL not configured. Set OPENEP_SERVER_URL environment variable or provide config file")
	}
	resp, err := httpClient.Get(url + "/metrics")
	if err != nil {
		return "", fmt.Errorf("failed to connect to server: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func runMonitorBuckets() ([]BucketInfo, error) {
	url := getServerURL()
	if url == "" {
		return nil, fmt.Errorf("server URL not configured. Set OPENEP_SERVER_URL environment variable or provide config file")
	}
	resp, err := httpClient.Get(url + "/_mgmt/buckets")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Buckets []struct {
			Name string `json:"name"`
		} `json:"buckets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	// Get object counts for each bucket
	buckets := make([]BucketInfo, len(result.Buckets))
	for i, b := range result.Buckets {
		objURL := fmt.Sprintf("%s/_mgmt/buckets/%s/objects", url, b.Name)
		objResp, err := httpClient.Get(objURL)
		if err != nil {
			buckets[i] = BucketInfo{Name: b.Name, ObjectCount: 0}
			continue
		}
		var objResult struct {
			Contents []interface{} `json:"Contents"`
		}
		json.NewDecoder(objResp.Body).Decode(&objResult)
		objResp.Body.Close()
		buckets[i] = BucketInfo{Name: b.Name, ObjectCount: len(objResult.Contents)}
	}

	return buckets, nil
}

func runMonitorWatch(interval int) {
	if interval <= 0 {
		interval = 2
	}

	// Check server URL first
	serverURL := getServerURL()
	if serverURL == "" {
		fmt.Println("Error: server URL not configured. Set OPENEP_SERVER_URL environment variable or provide config file")
		return
	}

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	fmt.Println("Watching server metrics... (Press Ctrl+C to stop)")
	fmt.Println()

	for range ticker.C {
		// Clear screen (works on most terminals)
		fmt.Print("\033[H\033[2J")

		// Get health
		healthy, _ := runMonitorHealth()
		status := "✗ UNHEALTHY"
		if healthy {
			status = "✓ HEALTHY"
		}

		// Get bucket count
		url := serverURL + "/_mgmt/buckets"
		resp, _ := httpClient.Get(url)
		bucketCount := 0
		if resp != nil {
			var result struct {
				Buckets []interface{} `json:"buckets"`
			}
			json.NewDecoder(resp.Body).Decode(&result)
			resp.Body.Close()
			bucketCount = len(result.Buckets)
		}

		// Get metrics
		metrics, _ := runMonitorMetrics()

		// Print summary
		fmt.Printf("OpenEndpoint Monitor - %s\n", time.Now().Format("2006-01-02 15:04:05"))
		fmt.Printf("Status: %s | Buckets: %d\n", status, bucketCount)
		fmt.Println()
		fmt.Println("Metrics:")
		fmt.Println(metrics)
	}
}

func init() {
	RootCmd.AddCommand(ServerCmd)
	RootCmd.AddCommand(VersionCmd)
	RootCmd.AddCommand(BucketCmd)
	RootCmd.AddCommand(ObjectCmd)
	RootCmd.AddCommand(ConfigCmd)
	RootCmd.AddCommand(AdminCmd)
	RootCmd.AddCommand(MonitorCmd)

	// Bucket subcommands
	BucketCmd.AddCommand(BucketCreateCmd)
	BucketCmd.AddCommand(BucketListCmd)
	BucketCmd.AddCommand(BucketDeleteCmd)
	BucketCmd.AddCommand(BucketInfoCmd)

	// Object subcommands
	ObjectCmd.AddCommand(ObjectPutCmd)
	ObjectCmd.AddCommand(ObjectGetCmd)
	ObjectCmd.AddCommand(ObjectListCmd)
	ObjectCmd.AddCommand(ObjectDeleteCmd)
	ObjectCmd.AddCommand(ObjectCopyCmd)

	// Config subcommands
	ConfigCmd.AddCommand(ConfigGetCmd)
	ConfigCmd.AddCommand(ConfigSetCmd)

	// Admin subcommands
	AdminCmd.AddCommand(AdminInfoCmd)
	AdminCmd.AddCommand(AdminStatsCmd)

	// Monitor subcommands
	MonitorCmd.AddCommand(MonitorStatusCmd)
	MonitorCmd.AddCommand(MonitorHealthCmd)
	MonitorCmd.AddCommand(MonitorReadyCmd)
	MonitorCmd.AddCommand(MonitorClusterCmd)
	MonitorCmd.AddCommand(MonitorMetricsCmd)
	MonitorCmd.AddCommand(MonitorBucketsCmd)
	MonitorCmd.AddCommand(MonitorWatchCmd)

	// Global flags
	RootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "", "Path to config file")

	// Object list flags
	ObjectListCmd.Flags().BoolP("recursive", "r", false, "List recursively")
	ObjectListCmd.Flags().BoolP("long", "l", false, "Long format")

	// Bucket delete flags
	BucketDeleteCmd.Flags().BoolP("force", "f", false, "Force delete without confirmation")

	// Object delete flags
	ObjectDeleteCmd.Flags().BoolP("force", "f", false, "Force delete without confirmation")

	// Monitor watch flags
	MonitorWatchCmd.Flags().IntP("interval", "i", 2, "Update interval in seconds")
}
