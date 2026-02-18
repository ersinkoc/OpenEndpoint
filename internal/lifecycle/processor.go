package lifecycle

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/openendpoint/openendpoint/internal/engine"
	"github.com/openendpoint/openendpoint/internal/metadata"
)

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
	log.Printf("Lifecycle processor started with interval: %v", p.interval)
}

// Stop stops the lifecycle processor
func (p *Processor) Stop() {
	close(p.stopCh)
	p.wg.Wait()
	log.Println("Lifecycle processor stopped")
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
	ctx := context.Background()

	buckets, err := p.engine.ListBuckets(ctx)
	if err != nil {
		log.Printf("Failed to list buckets: %v", err)
		return
	}

	for _, bucket := range buckets {
		p.processBucket(bucket.Name)
	}
}

// processBucket processes lifecycle rules for a bucket
func (p *Processor) processBucket(bucket string) {
	ctx := context.Background()

	rules, err := p.engine.GetLifecycleRules(ctx, bucket)
	if err != nil || len(rules) == 0 {
		return
	}

	// Process each rule
	for _, rule := range rules {
		if rule.Status != "Enabled" {
			continue
		}

		p.processRule(bucket, &rule)
	}
}

// processRule processes a single lifecycle rule
func (p *Processor) processRule(bucket string, rule *metadata.LifecycleRule) {
	ctx := context.Background()

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
		log.Printf("Failed to list objects for expiration: %v", err)
		return
	}

	for _, obj := range result.Objects {
		if obj.LastModified < cutoffTime {
			err := p.engine.DeleteObject(ctx, bucket, obj.Key, engine.DeleteObjectOptions{})
			if err != nil {
				log.Printf("Failed to delete expired object %s: %v", obj.Key, err)
			} else {
				log.Printf("Deleted expired object: %s/%s", bucket, obj.Key)
			}
		}
	}
}

// processTransitions processes storage class transitions
func (p *Processor) processTransitions(ctx context.Context, bucket string, rule *metadata.LifecycleRule) {
	// TODO: Implement storage class transitions
	log.Printf("Processing transitions for bucket %s (not implemented)", bucket)
}

// processNoncurrentVersionExpiration processes noncurrent version expiration
func (p *Processor) processNoncurrentVersionExpiration(ctx context.Context, bucket string, rule *metadata.LifecycleRule) {
	// TODO: Implement noncurrent version expiration
	log.Printf("Processing noncurrent version expiration for bucket %s (not implemented)", bucket)
}

// AddRule adds a lifecycle rule to a bucket
func (p *Processor) AddRule(bucket string, rule *metadata.LifecycleRule) error {
	ctx := context.Background()
	return p.engine.PutLifecycleRule(ctx, bucket, rule)
}

// RemoveRule removes a lifecycle rule from a bucket
func (p *Processor) RemoveRule(bucket, ruleID string) error {
	ctx := context.Background()

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
func (p *Processor) GetRules(bucket string) ([]metadata.LifecycleRule, error) {
	ctx := context.Background()
	return p.engine.GetLifecycleRules(ctx, bucket)
}
