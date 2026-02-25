package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/openendpoint/openendpoint/internal/api"
	"github.com/openendpoint/openendpoint/internal/auth"
	"github.com/openendpoint/openendpoint/internal/cluster"
	"github.com/openendpoint/openendpoint/internal/config"
	"github.com/openendpoint/openendpoint/internal/dashboard"
	"github.com/openendpoint/openendpoint/internal/engine"
	"github.com/openendpoint/openendpoint/internal/lifecycle"
	"github.com/openendpoint/openendpoint/internal/metadata/pebble"
	"github.com/openendpoint/openendpoint/internal/mgmt"
	"github.com/openendpoint/openendpoint/internal/middleware"
	"github.com/openendpoint/openendpoint/internal/storage/flatfile"
	"github.com/openendpoint/openendpoint/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// clusterAdapter adapts *cluster.Cluster to dashboard interface
type clusterAdapter struct {
	cluster *cluster.Cluster
}

func (c *clusterAdapter) GetClusterInfo() interface{} {
	if c.cluster == nil {
		return nil
	}
	return c.cluster.GetClusterInfo()
}

func (c *clusterAdapter) GetNodes() interface{} {
	if c.cluster == nil {
		return nil
	}
	return c.cluster.GetNodes()
}

var (
	version   = "v1.0.0"
	buildTime = "dev"
)

func main() {
	rootCmd := &cobra.Command{
		Use:     "openep",
		Short:   "OpenEndpoint - Developer-first object storage",
		Long:    `OpenEndpoint is a self-hosted, S3-compatible object storage platform.`,
		Version: fmt.Sprintf("OpenEndpoint %s (built at %s)", version, buildTime),
	}

	rootCmd.AddCommand(serverCmd())
	rootCmd.AddCommand(versionCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func serverCmd() *cobra.Command {
	var cfgPath string

	cmd := &cobra.Command{
		Use:   "server",
		Short: "Start OpenEndpoint server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServer(cfgPath)
		},
	}

	cmd.Flags().StringVarP(&cfgPath, "config", "c", "", "Path to config file")

	return cmd
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("OpenEndpoint version %s (built at %s)\n", version, buildTime)
		},
	}
}

func runServer(cfgPath string) error {
	// Load configuration
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Initialize logger
	logger, err := telemetry.NewLogger("info") // Default to info, can be configured via cfg.Logging.Level
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer logger.Sync()

	// Also create a plain zap logger for cluster
	zapLogger, _ := zap.NewProduction()
	defer zapLogger.Sync()

	logger.Info("starting OpenEndpoint server",
		zap.String("version", version),
		zap.String("build_time", buildTime),
	)

	// Initialize storage backend
	storage, err := flatfile.New(cfg.Storage.DataDir)
	if err != nil {
		logger.Error("failed to initialize storage backend", zap.Error(err))
		return fmt.Errorf("failed to initialize storage: %w", err)
	}
	defer storage.Close()

	// Initialize metadata store
	metadata, err := pebble.New(cfg.Storage.DataDir)
	if err != nil {
		logger.Error("failed to initialize metadata store", zap.Error(err))
		return fmt.Errorf("failed to initialize metadata: %w", err)
	}
	defer metadata.Close()

	// Initialize object engine
	objEngine := engine.New(storage, metadata, logger)

	// Initialize storage metrics from existing data
	if bytes, objects, err := objEngine.ComputeStorageMetrics(); err == nil {
		telemetry.SetStorageBytes(bytes)
		telemetry.SetStorageObjectsTotal(objects)
		logger.Info("storage metrics initialized",
			zap.Int64("bytes", bytes),
			zap.Int64("objects", objects))
	}

	// Initialize auth service
	authService := auth.New(cfg.Auth)

	// Initialize cluster (if enabled)
	var clusterService *cluster.Cluster
	if cfg.Cluster.Enabled {
		logger.Info("initializing cluster mode",
			zap.String("node_id", cfg.Cluster.NodeID),
			zap.String("bind_addr", cfg.Cluster.BindAddr),
			zap.Int("replication_factor", cfg.Cluster.ReplicationFactor),
		)

		clusterService = cluster.NewCluster(zapLogger,
			cluster.WithNodeID(cfg.Cluster.NodeID),
			cluster.WithBindAddress(cfg.Cluster.BindAddr),
		)

		if err := clusterService.Start(context.Background()); err != nil {
			logger.Warn("failed to start cluster", zap.Error(err))
		} else {
			logger.Info("cluster started successfully")
		}
	}

	// Initialize lifecycle processor (if enabled)
	var lifecycleProcessor *lifecycle.Processor
	lifecycleProcessor = lifecycle.NewProcessor(objEngine, 1*time.Hour)
	go lifecycleProcessor.Start()
	defer lifecycleProcessor.Stop()

	// Initialize S3 API router with all dependencies
	s3Router := api.NewRouter(objEngine, authService, logger, cfg)

	// Initialize management API router with cluster info
	mgmtRouter := mgmt.NewRouter(objEngine, logger, cfg, clusterService, cfg.Storage.DataDir)

	// Create dashboard wrapper that adapts cluster.Cluster to dashboard interface
	var dashboardCluster interface {
		GetClusterInfo() interface{}
		GetNodes() interface{}
	}
	if clusterService != nil {
		dashboardCluster = &clusterAdapter{cluster: clusterService}
	}

	// Setup HTTP server
	mux := http.NewServeMux()

	// S3 API endpoints
	mux.Handle("/s3/", s3Router)

	// Management API endpoints
	mux.Handle("/_mgmt/", mgmtRouter)

	// Web Dashboard
	mux.Handle("/_dashboard/", dashboard.Handler(dashboardCluster))

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Readiness check
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	})

	// Prometheus metrics - register all telemetry metrics
	reg := prometheus.NewRegistry()
	reg.MustRegister(telemetry.RequestsTotal)
	reg.MustRegister(telemetry.RequestDuration)
	reg.MustRegister(telemetry.BytesUploaded)
	reg.MustRegister(telemetry.BytesDownloaded)
	// Storage metrics
	reg.MustRegister(telemetry.StorageBytesStored)
	reg.MustRegister(telemetry.StorageObjectsTotal)
	reg.MustRegister(telemetry.StorageBucketsTotal)
	reg.MustRegister(telemetry.StorageDiskUsagePercent)
	// Operation metrics
	reg.MustRegister(telemetry.OperationDuration)
	reg.MustRegister(telemetry.OperationsTotal)
	// Request metrics
	reg.MustRegister(telemetry.RequestsFailedTotal)
	reg.MustRegister(telemetry.RequestSizeBytes)
	reg.MustRegister(telemetry.ResponseSizeBytes)
	// Bucket metrics
	reg.MustRegister(telemetry.BucketObjects)
	reg.MustRegister(telemetry.BucketBytes)

	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)

	// Apply CORS middleware for WebUI access
	corsHandler := middleware.CORS([]string{"*"})(mux)

	server := &http.Server{
		Addr:         addr,
		Handler:      telemetry.LoggingMiddleware(logger)(corsHandler),
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(cfg.Server.IdleTimeout) * time.Second,
	}

	// Start server in goroutine
	go func() {
		logger.Info("server listening", zap.String("address", addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", zap.Error(err))
		}
	}()

	logger.Info("OpenEndpoint server started successfully",
		zap.String("address", addr),
		zap.String("version", version),
	)

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down server...")

	// Stop lifecycle processor
	lifecycleProcessor.Stop()

	// Stop cluster if running
	if clusterService != nil {
		clusterService.Stop()
	}

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("server forced to shutdown", zap.Error(err))
		return err
	}

	logger.Info("server exited")
	return nil
}
