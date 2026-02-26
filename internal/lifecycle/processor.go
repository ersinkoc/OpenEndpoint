package lifecycle

import (
	"context"
	"sync"
	"time"

	"github.com/openendpoint/openendpoint/internal/engine"
	"github.com/openendpoint/openendpoint/internal/metadata"
	"go.uber.org/zap"
)

var logger, _ = zap.NewProduction()

// Processor handles lifecycle rule processing
type Processor struct {
	engine   *engine.ObjectService
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewProcessor creates a new lifecycle processor
func NewProcessor(eng *engine.ObjectService, interval time.Duration) *Processor {
	return &Processor{
		engine:   eng,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start starts the lifecycle processor
func (p *Processor) Start() {
	p.wg.Add(1)
	go p.run()
	logger.Info("lifecycle processor started", zap.Duration("interval", p.interval))
}

// Stop stops the lifecycle processor
func (p *Processor) Stop() {
	close(p.stopCh)
	p.wg.Wait()
	logger.Info("lifecycle processor stopped")
}

// Run runs the lifecycle processor loop
func (p *Processor) run() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// Run immediately on start
	p.processBuckets()

	for {
		select {
		case <-ticker.C:
			p.processBuckets()
		case <-p.stopCh:
			return
		}
	}
}

// processBuckets processes all buckets
func (p *Processor) processBuckets() {
	// Create a cancellable context with timeout for background operations
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	buckets, err := p.engine.ListBuckets(ctx)
	if err != nil {
		logger.Error("failed to list buckets", zap.Error(err))
		return
	}

	for _, bucket := range buckets {
		select {
		case <-p.stopCh:
			return
		default:
			p.processBucket(ctx, bucket.Name)
		}
	}
}

// processBucket processes lifecycle rules for a bucket
func (p *Processor) processBucket(ctx context.Context, bucket string) {
	rules, err := p.engine.GetLifecycleRules(ctx, bucket)
	if err != nil || len(rules) == 0 {
		return
	}

	// Process each rule
	for _, rule := range rules {
		if rule.Status != "Enabled" {
			continue
		}

		select {
		case <-p.stopCh:
			return
		default:
			p.processRule(ctx, bucket, &rule)
		}
	}
}

// processRule processes a single lifecycle rule
func (p *Processor) processRule(ctx context.Context, bucket string, rule *metadata.LifecycleRule) {

	// Process expiration
	if rule.Expiration != nil && rule.Expiration.Days > 0 {
		p.processExpiration(ctx, bucket, rule)
	}

	// Process transitions
	if len(rule.Transitions) > 0 {
		p.processTransitions(ctx, bucket, rule)
	}

	// Process noncurrent version expiration
	if rule.NoncurrentVersionExpiration != nil {
		p.processNoncurrentVersionExpiration(ctx, bucket, rule)
	}
}

// processExpiration processes object expiration
func (p *Processor) processExpiration(ctx context.Context, bucket string, rule *metadata.LifecycleRule) {
	cutoffTime := time.Now().AddDate(0, 0, -rule.Expiration.Days).Unix()

	// List objects
	opts := engine.ListObjectsOptions{
		Prefix:   rule.Prefix,
		MaxKeys:  1000,
	}

	result, err := p.engine.ListObjects(ctx, bucket, opts)
	if err != nil {
		logger.Error("failed to list objects for expiration", zap.Error(err))
		return
	}

	for _, obj := range result.Objects {
		if obj.LastModified < cutoffTime {
			err := p.engine.DeleteObject(ctx, bucket, obj.Key, engine.DeleteObjectOptions{})
			if err != nil {
				logger.Error("failed to delete expired object",
					zap.String("key", obj.Key),
					zap.Error(err))
			} else {
				logger.Info("deleted expired object",
					zap.String("bucket", bucket),
					zap.String("key", obj.Key))
			}
		}
	}
}

// processTransitions processes storage class transitions
func (p *Processor) processTransitions(ctx context.Context, bucket string, rule *metadata.LifecycleRule) {
	// Get transition actions from rule
	transitions := rule.Transitions
	if len(transitions) == 0 {
		return
	}

	// Get all objects in bucket
	result, err := p.engine.ListObjects(ctx, bucket, engine.ListObjectsOptions{
		MaxKeys: 1000,
	})
	if err != nil {
		logger.Error("failed to list objects for transition", zap.Error(err))
		return
	}

	now := time.Now().Unix()

	for _, obj := range result.Objects {
		// Get object metadata using HeadObject
		objMeta, err := p.engine.HeadObject(ctx, bucket, obj.Key)
		if err != nil {
			continue
		}

		// Check each transition
		for _, transition := range transitions {
			days := transition.Days
			if days == 0 {
				continue
			}

			// Calculate age of object
			age := now - objMeta.LastModified

			// If object is old enough, transition to new storage class
			if age >= int64(days) {
				// Skip if already in target storage class
				if objMeta.StorageClass == transition.StorageClass {
					continue
				}

				// Perform the transition by copying to itself with new storage class
				_, err := p.engine.CopyObject(ctx, bucket, obj.Key, bucket, obj.Key)
				if err != nil {
					logger.Error("failed to transition object",
						zap.String("bucket", bucket),
						zap.String("key", obj.Key),
						zap.String("storage_class", transition.StorageClass),
						zap.Error(err))
					continue
				}

				logger.Info("transitioned object to storage class",
					zap.String("bucket", bucket),
					zap.String("key", obj.Key),
					zap.String("storage_class", transition.StorageClass))
			}
		}
	}
}

// processNoncurrentVersionExpiration processes noncurrent version expiration
func (p *Processor) processNoncurrentVersionExpiration(ctx context.Context, bucket string, rule *metadata.LifecycleRule) {
	noncurrentExp := rule.NoncurrentVersionExpiration
	if noncurrentExp == nil || noncurrentExp.NoncurrentDays == 0 {
		return
	}

	// Note: Full version expiration would require tracking all versions
	// This is a simplified implementation that logs the action
	logger.Info("noncurrent version expiration",
		zap.String("bucket", bucket),
		zap.Int("days", noncurrentExp.NoncurrentDays))

	// In a full implementation, we would:
	// 1. List all object versions in the bucket
	// 2. For each non-latest version, check if it's older than NoncurrentDays
	// 3. Delete versions that exceed the threshold
}

// AddRule adds a lifecycle rule to a bucket
func (p *Processor) AddRule(ctx context.Context, bucket string, rule *metadata.LifecycleRule) error {
	return p.engine.PutLifecycleRule(ctx, bucket, rule)
}

// RemoveRule removes a lifecycle rule from a bucket
func (p *Processor) RemoveRule(ctx context.Context, bucket, ruleID string) error {
	rules, err := p.engine.GetLifecycleRules(ctx, bucket)
	if err != nil {
		return err
	}

	// Find and remove rule
	var newRules []metadata.LifecycleRule
	for _, r := range rules {
		if r.ID != ruleID {
			newRules = append(newRules, r)
		}
	}

	// If no rules left, we're done
	if len(newRules) == 0 {
		return nil
	}

	// Update rules
	return p.engine.PutLifecycleRule(ctx, bucket, &newRules[0])
}

// GetRules returns lifecycle rules for a bucket
func (p *Processor) GetRules(ctx context.Context, bucket string) ([]metadata.LifecycleRule, error) {
	return p.engine.GetLifecycleRules(ctx, bucket)
}
