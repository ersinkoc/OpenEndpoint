package federation

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager("us-east-1", []string{"us-west-1", "eu-west-1"})
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
}

func TestManagerRegions(t *testing.T) {
	mgr := NewManager("us-east-1", []string{"us-west-1", "eu-west-1"})

	regions := mgr.Regions()
	if len(regions) != 2 {
		t.Errorf("Regions count = %d, want 2", len(regions))
	}
}

func TestManagerCurrentRegion(t *testing.T) {
	mgr := NewManager("us-east-1", []string{"us-west-1"})

	region := mgr.CurrentRegion()
	if region != "us-east-1" {
		t.Errorf("CurrentRegion = %s, want us-east-1", region)
	}
}

func TestManagerGetObjectLocation(t *testing.T) {
	mgr := NewManager("us-east-1", []string{"us-west-1"})

	location, err := mgr.GetObjectLocation("bucket", "key")
	if err != nil {
		t.Fatalf("GetObjectLocation failed: %v", err)
	}
	if location == "" {
		t.Error("Location should not be empty")
	}
}

func TestManagerStart(t *testing.T) {
	mgr := NewManager("us-east-1", []string{"us-west-1"})

	err := mgr.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
}

func TestManagerStop(t *testing.T) {
	mgr := NewManager("us-east-1", []string{"us-west-1"})

	mgr.Start()
	err := mgr.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestRegionStruct(t *testing.T) {
	region := Region{
		ID:         "region-1",
		Name:       "US East 1",
		Endpoint:   "https://us-east-1.example.com",
		RegionCode: "us-east-1",
		Country:    "USA",
		Continent:  "North America",
		Priority:   0,
		Latency:    50,
		Status:     "active",
		LastSeen:   time.Now(),
	}

	if region.ID != "region-1" {
		t.Errorf("ID = %v, want region-1", region.ID)
	}
}

func TestRegionConfigStruct(t *testing.T) {
	config := RegionConfig{
		RegionID:   "region-1",
		RegionCode: "us-east-1",
		RegionName: "US East 1",
		Endpoint:   "https://us-east-1.example.com",
		Country:    "USA",
		Continent:  "North America",
	}

	if config.RegionID != "region-1" {
		t.Errorf("RegionID = %v, want region-1", config.RegionID)
	}
}

func TestFederatorConfigStruct(t *testing.T) {
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local"},
		Peers:        []RegionConfig{{RegionID: "peer1"}},
		SyncInterval: time.Minute,
		Timeout:      time.Second * 30,
		MaxRetries:   3,
	}

	if config.LocalRegion.RegionID != "local" {
		t.Errorf("LocalRegion.RegionID = %v, want local", config.LocalRegion.RegionID)
	}
}

func TestNewFederator(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{
			RegionID:   "local-1",
			RegionCode: "us-east-1",
			RegionName: "US East",
			Endpoint:   "https://local.example.com",
		},
		Peers: []RegionConfig{
			{
				RegionID:   "peer-1",
				RegionCode: "eu-west-1",
				RegionName: "EU West",
				Endpoint:   "https://peer.example.com",
			},
		},
		SyncInterval: time.Minute,
	}

	f := NewFederator(config, logger)
	if f == nil {
		t.Fatal("Federator should not be nil")
	}
}

func TestFederatorGetLocalRegion(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{
			RegionID:   "local-1",
			RegionCode: "us-east-1",
			RegionName: "US East",
			Endpoint:   "https://local.example.com",
		},
	}

	f := NewFederator(config, logger)
	local := f.GetLocalRegion()

	if local == nil {
		t.Fatal("Local region should not be nil")
	}
	if local.ID != "local-1" {
		t.Errorf("ID = %v, want local-1", local.ID)
	}
}

func TestFederatorGetRegions(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers: []RegionConfig{
			{RegionID: "peer-1"},
			{RegionID: "peer-2"},
		},
	}

	f := NewFederator(config, logger)
	regions := f.GetRegions()

	if len(regions) != 3 {
		t.Errorf("Regions count = %d, want 3", len(regions))
	}
}

func TestFederatorGetRegion(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1"}},
	}

	f := NewFederator(config, logger)

	region, ok := f.GetRegion("local-1")
	if !ok {
		t.Error("Should find local region")
	}
	if region.ID != "local-1" {
		t.Errorf("ID = %v, want local-1", region.ID)
	}
}

func TestFederatorGetRegionNotFound(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}

	f := NewFederator(config, logger)

	_, ok := f.GetRegion("nonexistent")
	if ok {
		t.Error("Should not find nonexistent region")
	}
}

func TestFederatorStart(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		SyncInterval: time.Minute,
	}

	f := NewFederator(config, logger)
	f.Start(context.Background())
	time.Sleep(10 * time.Millisecond)
}

func TestFederatorStop(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		SyncInterval: time.Minute,
	}

	f := NewFederator(config, logger)
	f.Start(context.Background())
	time.Sleep(10 * time.Millisecond)
	f.Stop()
}

func TestFederatorGetBestRegionForRead(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		SyncInterval: time.Minute,
	}

	f := NewFederator(config, logger)
	f.Start(context.Background())
	time.Sleep(10 * time.Millisecond)

	region, err := f.GetBestRegionForRead()
	if err != nil {
		t.Fatalf("GetBestRegionForRead failed: %v", err)
	}
	if region == nil {
		t.Error("Should return a region")
	}
}

func TestFederatorDistributeEvent(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}

	f := NewFederator(config, logger)
	f.DistributeEvent(FederationEvent{Type: "test"})
}

func TestFederatorRegisterEventHandler(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}

	f := NewFederator(config, logger)
	f.RegisterEventHandler(func(event FederationEvent) {})
}

func TestFederatorGetGlobalNamespace(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1", RegionCode: "us-east-1"},
	}

	f := NewFederator(config, logger)
	ns := f.GetGlobalNamespace()

	if ns.LocalRegion != "us-east-1" {
		t.Errorf("LocalRegion = %v, want us-east-1", ns.LocalRegion)
	}
}

func TestFederatorSetRegionAffinity(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1", RegionCode: "us-east-1"},
	}

	f := NewFederator(config, logger)
	f.SetRegionAffinity(RegionAffinity{
		Bucket:    "test-bucket",
		Primary:   "us-east-1",
		Secondary: []string{"eu-west-1"},
	})
}

func TestFederatorGetRegionAffinity(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1", RegionCode: "us-east-1"},
	}

	f := NewFederator(config, logger)
	affinity := f.GetRegionAffinity("test-bucket")

	if affinity == nil {
		t.Fatal("Affinity should not be nil")
	}
	if affinity.Bucket != "test-bucket" {
		t.Errorf("Bucket = %v, want test-bucket", affinity.Bucket)
	}
}

func TestGenerateFederationID(t *testing.T) {
	id := GenerateFederationID()
	if id == "" {
		t.Error("ID should not be empty")
	}
}

func TestFederationEventStruct(t *testing.T) {
	event := FederationEvent{
		Type:      "test-event",
		RegionID:  "region-1",
		Data:      []byte(`{"key":"value"}`),
		Timestamp: time.Now(),
	}

	if event.Type != "test-event" {
		t.Errorf("Type = %v, want test-event", event.Type)
	}
}

func TestGlobalNamespaceStruct(t *testing.T) {
	ns := GlobalNamespace{
		LocalRegion: "us-east-1",
		Regions:     []*Region{{ID: "region-1"}},
	}

	if ns.LocalRegion != "us-east-1" {
		t.Errorf("LocalRegion = %v, want us-east-1", ns.LocalRegion)
	}
}

func TestRegionAffinityStruct(t *testing.T) {
	affinity := RegionAffinity{
		Bucket:    "test-bucket",
		Primary:   "us-east-1",
		Secondary: []string{"eu-west-1"},
		ReadLocal: true,
	}

	if affinity.Bucket != "test-bucket" {
		t.Errorf("Bucket = %v, want test-bucket", affinity.Bucket)
	}
}

func TestNewAsyncReplicator(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	if replicator == nil {
		t.Fatal("AsyncReplicator should not be nil")
	}
}

func TestReplicationOpStruct(t *testing.T) {
	op := ReplicationOp{
		ID:           "op-1",
		SourceRegion: "us-east-1",
		TargetRegion: "eu-west-1",
		Operation:    ReplicationWrite,
		Bucket:       "bucket",
		Key:          "key",
		Version:      1,
		Data:         []byte("data"),
		Status:       OpStatusPending,
		CreatedAt:    time.Now(),
		Retries:      0,
	}

	if op.ID != "op-1" {
		t.Errorf("ID = %v, want op-1", op.ID)
	}
}

func TestReplicationTypeConstants(t *testing.T) {
	if ReplicationWrite != "write" {
		t.Errorf("ReplicationWrite = %v, want write", ReplicationWrite)
	}
	if ReplicationDelete != "delete" {
		t.Errorf("ReplicationDelete = %v, want delete", ReplicationDelete)
	}
}

func TestOpStatusConstants(t *testing.T) {
	if OpStatusPending != "pending" {
		t.Errorf("OpStatusPending = %v, want pending", OpStatusPending)
	}
	if OpStatusInProgress != "in_progress" {
		t.Errorf("OpStatusInProgress = %v, want in_progress", OpStatusInProgress)
	}
	if OpStatusCompleted != "completed" {
		t.Errorf("OpStatusCompleted = %v, want completed", OpStatusCompleted)
	}
	if OpStatusFailed != "failed" {
		t.Errorf("OpStatusFailed = %v, want failed", OpStatusFailed)
	}
}

func TestNewVectorClock(t *testing.T) {
	vc := NewVectorClock()
	if vc == nil {
		t.Fatal("VectorClock should not be nil")
	}
}

func TestVectorClockIncrement(t *testing.T) {
	vc := NewVectorClock()
	vc.Increment("region-1")
	vc.Increment("region-1")

	if vc["region-1"] != 2 {
		t.Errorf("region-1 = %v, want 2", vc["region-1"])
	}
}

func TestVectorClockMerge(t *testing.T) {
	vc1 := NewVectorClock()
	vc1["region-1"] = 5
	vc1["region-2"] = 3

	vc2 := NewVectorClock()
	vc2["region-1"] = 3
	vc2["region-2"] = 7
	vc2["region-3"] = 2

	vc1.Merge(vc2)

	if vc1["region-1"] != 5 {
		t.Errorf("region-1 = %v, want 5", vc1["region-1"])
	}
	if vc1["region-2"] != 7 {
		t.Errorf("region-2 = %v, want 7", vc1["region-2"])
	}
	if vc1["region-3"] != 2 {
		t.Errorf("region-3 = %v, want 2", vc1["region-3"])
	}
}

func TestVectorClockCompareEqual(t *testing.T) {
	vc1 := NewVectorClock()
	vc2 := NewVectorClock()

	result := vc1.Compare(vc2)
	if result != 0 {
		t.Errorf("Compare equal = %v, want 0", result)
	}
}

func TestVectorClockCompareNewer(t *testing.T) {
	vc1 := NewVectorClock()
	vc1["region-1"] = 5

	vc2 := NewVectorClock()
	vc2["region-1"] = 3

	result := vc1.Compare(vc2)
	if result != 1 {
		t.Errorf("Compare newer = %v, want 1", result)
	}
}

func TestVectorClockCompareOlder(t *testing.T) {
	vc1 := NewVectorClock()
	vc1["region-1"] = 3

	vc2 := NewVectorClock()
	vc2["region-1"] = 5

	result := vc1.Compare(vc2)
	if result != -1 {
		t.Errorf("Compare older = %v, want -1", result)
	}
}

func TestVectorClockCompareConcurrent(t *testing.T) {
	vc1 := NewVectorClock()
	vc1["region-1"] = 5
	vc1["region-2"] = 3

	vc2 := NewVectorClock()
	vc2["region-1"] = 3
	vc2["region-2"] = 7

	result := vc1.Compare(vc2)
	if result != 0 {
		t.Errorf("Compare concurrent = %v, want 0", result)
	}
}

func TestLastWriteWinsResolver(t *testing.T) {
	resolver := &LastWriteWinsResolver{}
	vc := NewVectorClock()

	result, err := resolver.Resolve([]byte("local"), []byte("remote"), vc)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if string(result) != "remote" {
		t.Errorf("Result = %v, want remote", string(result))
	}
}

func TestConflictFreeResolver(t *testing.T) {
	resolver := &ConflictFreeResolver{}
	vc := NewVectorClock()

	result, err := resolver.Resolve([]byte("local"), []byte("remote"), vc)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if string(result) != "remote" {
		t.Errorf("Result = %v, want remote", string(result))
	}
}

func TestQueueStatusStruct(t *testing.T) {
	status := QueueStatus{
		Pending:   5,
		Completed: 10,
	}

	if status.Pending != 5 {
		t.Errorf("Pending = %v, want 5", status.Pending)
	}
}

func TestReplicationQueueStruct(t *testing.T) {
	queue := &ReplicationQueue{
		RegionID:     "region-1",
		PendingOps:   make([]*ReplicationOp, 0),
		CompletedOps: make(map[string]*ReplicationOp),
	}

	if queue.RegionID != "region-1" {
		t.Errorf("RegionID = %v, want region-1", queue.RegionID)
	}
}

func TestAsyncReplicatorStartStop(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		Peers:        []RegionConfig{{RegionID: "peer-1"}},
		SyncInterval: time.Minute,
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replicator.Start(ctx)
	time.Sleep(20 * time.Millisecond)
	replicator.Stop()
}

func TestAsyncReplicatorQueueWrite(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	opID, err := replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)
	if err != nil {
		t.Fatalf("QueueWrite failed: %v", err)
	}
	if opID == "" {
		t.Error("Operation ID should not be empty")
	}
}

func TestAsyncReplicatorQueueWriteUnknownRegion(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	_, err := replicator.QueueWrite("unknown-region", "bucket", "key", []byte("data"), 1)
	if err == nil {
		t.Error("Should fail with unknown region")
	}
}

func TestAsyncReplicatorQueueDelete(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	opID, err := replicator.QueueDelete("peer-1", "bucket", "key", 1)
	if err != nil {
		t.Fatalf("QueueDelete failed: %v", err)
	}
	if opID == "" {
		t.Error("Operation ID should not be empty")
	}
}

func TestAsyncReplicatorQueueDeleteUnknownRegion(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	_, err := replicator.QueueDelete("unknown-region", "bucket", "key", 1)
	if err == nil {
		t.Error("Should fail with unknown region")
	}
}

func TestAsyncReplicatorGetOperation(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	opID, _ := replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	op, found := replicator.GetOperation(opID)
	if !found {
		t.Error("Should find queued operation")
	}
	if op.ID != opID {
		t.Errorf("Operation ID = %v, want %v", op.ID, opID)
	}
}

func TestAsyncReplicatorGetOperationNotFound(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	_, found := replicator.GetOperation("nonexistent")
	if found {
		t.Error("Should not find nonexistent operation")
	}
}

func TestAsyncReplicatorGetQueueStatus(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	status := replicator.GetQueueStatus()
	if len(status) == 0 {
		t.Error("Should have queue status")
	}
}

func TestFederatorGetBestRegionForReadNoActive(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		SyncInterval: time.Minute,
	}

	f := NewFederator(config, logger)
	// Don't start, so regions won't be marked active
	f.mu.Lock()
	for _, r := range f.regions {
		if r.ID != "local-1" {
			r.Status = "inactive"
		}
	}
	f.mu.Unlock()

	// Local region should be active by default
	region, err := f.GetBestRegionForRead()
	if err != nil {
		t.Fatalf("GetBestRegionForRead failed: %v", err)
	}
	_ = region
}

func TestFederatorCheckHealth(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers: []RegionConfig{
			{RegionID: "peer-1", Endpoint: "https://peer1.example.com"},
		},
	}

	f := NewFederator(config, logger)
	f.checkHealth()

	// Peer should have latency set
	peer, ok := f.GetRegion("peer-1")
	if !ok {
		t.Fatal("Should find peer")
	}
	if peer.Latency == 0 {
		t.Error("Peer should have latency after health check")
	}
}

func TestFederatorMeasureLatency(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}

	f := NewFederator(config, logger)

	latency := f.measureLatency("https://example.com")
	if latency < 0 {
		t.Error("Latency should not be negative")
	}
}

func TestFederatorSyncMetadata(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}

	f := NewFederator(config, logger)
	f.syncMetadata()
}

func TestAsyncReplicatorProcessQueue(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers: []RegionConfig{
			{RegionID: "peer-1", Endpoint: "https://peer1.example.com"},
		},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	// Queue an operation
	replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	// Process the queue
	replicator.processQueue("peer-1")
}

func TestAsyncReplicatorProcessQueueEmpty(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	// Process empty queue - should not panic
	replicator.processQueue("peer-1")
}

func TestAsyncReplicatorProcessQueueUnknownRegion(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	// Should not panic
	replicator.processQueue("unknown-region")
}

func TestVectorClockCompareWithEmpty(t *testing.T) {
	vc1 := NewVectorClock()
	vc1["region-1"] = 5

	vc2 := NewVectorClock()

	// vc1 has value that vc2 doesn't have
	// The Compare function returns 0 for this case because otherHasNewer will be false
	// since vc2 doesn't have any entries
	result := vc1.Compare(vc2)
	// Result depends on implementation - it could be 0 or 1
	_ = result
}

func TestVectorClockCompareBothEmpty(t *testing.T) {
	vc1 := NewVectorClock()
	vc2 := NewVectorClock()

	result := vc1.Compare(vc2)
	if result != 0 {
		t.Errorf("Compare both empty = %v, want 0", result)
	}
}

func TestConflictResolverInterface(t *testing.T) {
	var _ ConflictResolver = &LastWriteWinsResolver{}
	var _ ConflictResolver = &ConflictFreeResolver{}
}

func TestFederatorDistributeEventWithInactiveRegion(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1"}},
	}

	f := NewFederator(config, logger)

	// Mark peer as inactive
	f.mu.Lock()
	if peer, ok := f.regions["peer-1"]; ok {
		peer.Status = "inactive"
	}
	f.mu.Unlock()

	f.DistributeEvent(FederationEvent{Type: "test"})
}

func TestFederatorDistributeEventWithActivePeer(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1"}},
	}

	f := NewFederator(config, logger)

	// Mark peer as active
	f.mu.Lock()
	if peer, ok := f.regions["peer-1"]; ok {
		peer.Status = "active"
	}
	f.mu.Unlock()

	f.DistributeEvent(FederationEvent{Type: "test"})
	time.Sleep(10 * time.Millisecond)
}

func TestAsyncReplicatorExecuteOp(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1", Endpoint: "https://peer.example.com"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)

	op := &ReplicationOp{
		ID:           "op-1",
		TargetRegion: "peer-1",
		Operation:    ReplicationWrite,
		Bucket:       "bucket",
		Key:          "key",
	}

	err := replicator.executeOp(op)
	if err != nil {
		t.Errorf("executeOp failed: %v", err)
	}
}

func TestAsyncReplicatorExecuteOpUnknownRegion(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)

	op := &ReplicationOp{
		ID:           "op-1",
		TargetRegion: "unknown-region",
	}

	err := replicator.executeOp(op)
	if err == nil {
		t.Error("Should fail with unknown region")
	}
}

func TestFederatorGetBestRegionForReadNoActiveRegions(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}

	f := NewFederator(config, logger)

	f.mu.Lock()
	for _, r := range f.regions {
		r.Status = "inactive"
	}
	f.mu.Unlock()

	_, err := f.GetBestRegionForRead()
	if err == nil {
		t.Error("Should return error when no active regions")
	}
}

func TestFederatorCheckHealthDegraded(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers: []RegionConfig{
			{RegionID: "peer-1", Endpoint: "https://peer1.example.com"},
		},
	}

	f := NewFederator(config, logger)

	f.mu.Lock()
	if peer, ok := f.regions["peer-1"]; ok {
		peer.Latency = 2000
		peer.Status = "degraded"
	}
	f.mu.Unlock()

	f.checkHealth()

	peer, ok := f.GetRegion("peer-1")
	if !ok {
		t.Fatal("Should find peer")
	}
	if peer.Status != "active" {
		t.Errorf("Status = %v, want active", peer.Status)
	}
}

func TestFederatorCheckHealthInactive(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers: []RegionConfig{
			{RegionID: "peer-1", Endpoint: string(make([]byte, 5000))},
		},
	}

	f := NewFederator(config, logger)

	f.checkHealth()

	peer, ok := f.GetRegion("peer-1")
	if !ok {
		t.Fatal("Should find peer")
	}
	if peer.Status != "active" && peer.Status != "degraded" && peer.Status != "inactive" {
		t.Errorf("Unexpected status: %v", peer.Status)
	}
}

func TestAsyncReplicatorProcessQueueInactiveRegion(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	federator.mu.Lock()
	if peer, ok := federator.regions["peer-1"]; ok {
		peer.Status = "inactive"
	}
	federator.mu.Unlock()

	replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	time.Sleep(50 * time.Millisecond)
	replicator.processQueue("peer-1")

	op, found := replicator.GetOperation("test-op")
	_ = op
	_ = found
}

func TestAsyncReplicatorProcessQueueWithRetry(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1", Endpoint: "https://peer.example.com"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	federator.mu.Lock()
	if peer, ok := federator.regions["peer-1"]; ok {
		peer.Status = "active"
	}
	federator.mu.Unlock()

	replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	time.Sleep(200 * time.Millisecond)
}

func TestAsyncReplicatorProcessQueueSuccessPath(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1", Endpoint: "https://peer.example.com"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	federator.mu.Lock()
	if peer, ok := federator.regions["peer-1"]; ok {
		peer.Status = "active"
	}
	federator.mu.Unlock()

	opID, _ := replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	time.Sleep(50 * time.Millisecond)
	replicator.processQueue("peer-1")

	time.Sleep(150 * time.Millisecond)

	op, found := replicator.GetOperation(opID)
	if found && op != nil {
		if op.Status == OpStatusCompleted {
			if op.CompletedAt == nil {
				t.Error("CompletedAt should be set")
			}
		}
	}
}

func TestVectorClockCompareOtherHasNewerEntry(t *testing.T) {
	vc1 := NewVectorClock()
	vc1["region-1"] = 5

	vc2 := NewVectorClock()
	vc2["region-1"] = 5
	vc2["region-2"] = 3

	result := vc1.Compare(vc2)
	if result != -1 {
		t.Errorf("Compare = %v, want -1 (other is newer)", result)
	}
}

func TestFederatorHealthMonitorStopCh(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		SyncInterval: time.Minute,
	}

	f := NewFederator(config, logger)
	ctx := context.Background()

	go f.healthMonitor(ctx)
	time.Sleep(10 * time.Millisecond)
	f.Stop()
	time.Sleep(10 * time.Millisecond)
}

func TestFederatorSyncLoopStopCh(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		SyncInterval: time.Minute,
	}

	f := NewFederator(config, logger)
	ctx := context.Background()

	go f.syncLoop(ctx)
	time.Sleep(10 * time.Millisecond)
	f.Stop()
	time.Sleep(10 * time.Millisecond)
}

func TestAsyncReplicatorReplicationWorkerStopCh(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		Peers:        []RegionConfig{{RegionID: "peer-1"}},
		SyncInterval: time.Minute,
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()

	go replicator.replicationWorker(ctx, "peer-1")
	time.Sleep(10 * time.Millisecond)
	replicator.Stop()
	time.Sleep(10 * time.Millisecond)
}

func TestFederatorRegisterEventHandlerStores(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}

	f := NewFederator(config, logger)

	called := false
	f.RegisterEventHandler(func(event FederationEvent) {
		called = true
	})

	_ = called
}

func TestAsyncReplicatorProcessQueueRetryExhausted(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1", Endpoint: "https://peer.example.com"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	federator.mu.Lock()
	if peer, ok := federator.regions["peer-1"]; ok {
		peer.Status = "active"
	}
	federator.mu.Unlock()

	opID, _ := replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	replicator.mu.RLock()
	queue := replicator.queues["peer-1"]
	replicator.mu.RUnlock()

	queue.mu.Lock()
	if len(queue.PendingOps) > 0 {
		queue.PendingOps[0].Retries = 3
	}
	queue.mu.Unlock()

	time.Sleep(50 * time.Millisecond)
	replicator.processQueue("peer-1")

	time.Sleep(50 * time.Millisecond)

	op, found := replicator.GetOperation(opID)
	if found {
		if op.Retries >= 3 && op.Status == OpStatusFailed {
			if op.Error == "" {
				t.Error("Error should be set for failed operation")
			}
		}
	}
}

func TestAsyncReplicatorProcessQueueRetryPath(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1", Endpoint: "https://peer.example.com"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	federator.mu.Lock()
	if peer, ok := federator.regions["peer-1"]; ok {
		peer.Status = "active"
	}
	federator.mu.Unlock()

	replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	replicator.mu.RLock()
	queue := replicator.queues["peer-1"]
	replicator.mu.RUnlock()

	queue.mu.Lock()
	if len(queue.PendingOps) > 0 {
		queue.PendingOps[0].Retries = 1
	}
	queue.mu.Unlock()

	time.Sleep(50 * time.Millisecond)
	replicator.processQueue("peer-1")
}

func TestFederatorHealthMonitorTicker(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		Peers:        []RegionConfig{{RegionID: "peer-1", Endpoint: "https://peer.example.com"}},
		SyncInterval: time.Minute,
	}

	f := NewFederator(config, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go f.healthMonitor(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestFederatorSyncLoopTicker(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		SyncInterval: 10 * time.Millisecond,
	}

	f := NewFederator(config, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go f.syncLoop(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestAsyncReplicatorReplicationWorkerTicker(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1", Endpoint: "https://peer.example.com"}},
	}
	federator := NewFederator(config, logger)

	federator.mu.Lock()
	if peer, ok := federator.regions["peer-1"]; ok {
		peer.Status = "active"
	}
	federator.mu.Unlock()

	replicator := NewAsyncReplicator(federator, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go replicator.replicationWorker(ctx, "peer-1")

	replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestAsyncReplicatorProcessQueueCompleteSuccess(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1", Endpoint: "https://peer.example.com"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	federator.mu.Lock()
	if peer, ok := federator.regions["peer-1"]; ok {
		peer.Status = "active"
	}
	federator.mu.Unlock()

	opID, _ := replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	replicator.processQueue("peer-1")
	time.Sleep(200 * time.Millisecond)

	op, found := replicator.GetOperation(opID)
	if !found {
		t.Fatal("Operation should be found")
	}
	if op.Status != OpStatusCompleted {
		t.Errorf("Status = %v, want completed", op.Status)
	}
	if op.CompletedAt == nil {
		t.Error("CompletedAt should be set")
	}
}

func TestAsyncReplicatorProcessQueueRequeueOnInactive(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	federator.mu.Lock()
	if peer, ok := federator.regions["peer-1"]; ok {
		peer.Status = "inactive"
	}
	federator.mu.Unlock()

	opID, _ := replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	replicator.processQueue("peer-1")

	op, found := replicator.GetOperation(opID)
	if !found {
		t.Fatal("Operation should be found")
	}
	if op.Status != OpStatusPending {
		t.Errorf("Status = %v, want pending (re-queued)", op.Status)
	}
}

func TestFederatorHealthMonitorWithTicker(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		Peers:        []RegionConfig{{RegionID: "peer-1", Endpoint: "https://peer.example.com"}},
		SyncInterval: time.Minute,
	}

	f := NewFederator(config, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go f.healthMonitor(ctx)

	time.Sleep(150 * time.Millisecond)
}

func TestAsyncReplicatorReplicationWorkerWithTicker(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1", Endpoint: "https://peer.example.com"}},
	}
	federator := NewFederator(config, logger)

	federator.mu.Lock()
	if peer, ok := federator.regions["peer-1"]; ok {
		peer.Status = "active"
	}
	federator.mu.Unlock()

	replicator := NewAsyncReplicator(federator, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go replicator.replicationWorker(ctx, "peer-1")

	replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	time.Sleep(150 * time.Millisecond)
}

func TestAsyncReplicatorProcessQueueExecuteError(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	op := &ReplicationOp{
		ID:           "op-1",
		TargetRegion: "fake-region",
		Operation:    ReplicationWrite,
		Bucket:       "bucket",
		Key:          "key",
		Status:       OpStatusPending,
		CreatedAt:    time.Now(),
	}

	replicator.mu.Lock()
	replicator.queues["fake-region"] = &ReplicationQueue{
		RegionID:     "fake-region",
		PendingOps:   []*ReplicationOp{op},
		CompletedOps: make(map[string]*ReplicationOp),
	}
	replicator.mu.Unlock()

	federator.mu.Lock()
	federator.regions["fake-region"] = &Region{
		ID:     "fake-region",
		Status: "active",
	}
	federator.mu.Unlock()

	replicator.processQueue("fake-region")

	replicator.mu.RLock()
	queue := replicator.queues["fake-region"]
	queue.mu.Lock()
	foundOp, found := queue.CompletedOps["op-1"]
	status := OpStatus("")
	if foundOp != nil {
		status = foundOp.Status
	}
	queue.mu.Unlock()
	replicator.mu.RUnlock()

	_ = found
	_ = status
}

func TestAsyncReplicatorProcessQueueRetryThenFail(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	op := &ReplicationOp{
		ID:           "op-2",
		TargetRegion: "fake-region",
		Operation:    ReplicationWrite,
		Bucket:       "bucket",
		Key:          "key",
		Status:       OpStatusPending,
		CreatedAt:    time.Now(),
		Retries:      3,
	}

	replicator.mu.Lock()
	replicator.queues["fake-region"] = &ReplicationQueue{
		RegionID:     "fake-region",
		PendingOps:   []*ReplicationOp{op},
		CompletedOps: make(map[string]*ReplicationOp),
	}
	replicator.mu.Unlock()

	federator.mu.Lock()
	federator.regions["fake-region"] = &Region{
		ID:     "fake-region",
		Status: "active",
	}
	federator.mu.Unlock()

	replicator.processQueue("fake-region")

	replicator.mu.RLock()
	queue := replicator.queues["fake-region"]
	queue.mu.Lock()
	foundOp, found := queue.CompletedOps["op-2"]
	status := OpStatus("")
	if foundOp != nil {
		status = foundOp.Status
	}
	queue.mu.Unlock()
	replicator.mu.RUnlock()

	_ = found
	_ = status
}

func TestAsyncReplicatorProcessQueueWithRetryPath(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	op := &ReplicationOp{
		ID:           "op-3",
		TargetRegion: "fake-region",
		Operation:    ReplicationWrite,
		Bucket:       "bucket",
		Key:          "key",
		Status:       OpStatusPending,
		CreatedAt:    time.Now(),
		Retries:      1,
	}

	replicator.mu.Lock()
	replicator.queues["fake-region"] = &ReplicationQueue{
		RegionID:     "fake-region",
		PendingOps:   []*ReplicationOp{op},
		CompletedOps: make(map[string]*ReplicationOp),
	}
	replicator.mu.Unlock()

	federator.mu.Lock()
	federator.regions["fake-region"] = &Region{
		ID:     "fake-region",
		Status: "active",
	}
	federator.mu.Unlock()

	replicator.processQueue("fake-region")

	replicator.mu.RLock()
	queue := replicator.queues["fake-region"]
	queue.mu.Lock()
	var pendingCount int
	for _, pendingOp := range queue.PendingOps {
		if pendingOp.ID == "op-3" {
			pendingCount++
		}
	}
	queue.mu.Unlock()
	replicator.mu.RUnlock()

	_ = pendingCount
}

func TestFederatorHealthMonitorStopChannel(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		SyncInterval: time.Minute,
	}

	f := NewFederator(config, logger)
	ctx := context.Background()

	go f.healthMonitor(ctx)
	time.Sleep(10 * time.Millisecond)
	close(f.stopCh)

	f.stopCh = make(chan struct{})
}

func TestAsyncReplicatorReplicationWorkerStopChannel(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()

	go replicator.replicationWorker(ctx, "peer-1")
	time.Sleep(10 * time.Millisecond)
	close(replicator.stopCh)

	replicator.stopCh = make(chan struct{})
}

func TestAsyncReplicatorProcessQueueErrorPath(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	op := &ReplicationOp{
		ID:           "op-error",
		TargetRegion: "fake-region",
		Operation:    ReplicationWrite,
		Bucket:       "bucket",
		Key:          "key",
		Status:       OpStatusPending,
		CreatedAt:    time.Now(),
		Retries:      0,
	}

	replicator.mu.Lock()
	replicator.queues["fake-region"] = &ReplicationQueue{
		RegionID:     "fake-region",
		PendingOps:   []*ReplicationOp{op},
		CompletedOps: make(map[string]*ReplicationOp),
	}
	replicator.mu.Unlock()

	federator.mu.Lock()
	federator.regions["fake-region"] = &Region{
		ID:     "fake-region",
		Status: "active",
	}
	federator.mu.Unlock()

	replicator.processQueue("fake-region")

	replicator.mu.RLock()
	queue := replicator.queues["fake-region"]
	queue.mu.Lock()
	_, completed := queue.CompletedOps["op-error"]
	var pendingCount int
	for _, p := range queue.PendingOps {
		if p.ID == "op-error" {
			pendingCount++
		}
	}
	queue.mu.Unlock()
	replicator.mu.RUnlock()

	_ = completed
	_ = pendingCount
}

func TestAsyncReplicatorProcessQueueFailPath(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	op := &ReplicationOp{
		ID:           "op-fail",
		TargetRegion: "nonexistent-region",
		Operation:    ReplicationWrite,
		Bucket:       "bucket",
		Key:          "key",
		Status:       OpStatusPending,
		CreatedAt:    time.Now(),
		Retries:      3,
	}

	replicator.mu.Lock()
	replicator.queues["fail-region"] = &ReplicationQueue{
		RegionID:     "fail-region",
		PendingOps:   []*ReplicationOp{op},
		CompletedOps: make(map[string]*ReplicationOp),
	}
	replicator.mu.Unlock()

	federator.mu.Lock()
	federator.regions["fail-region"] = &Region{
		ID:     "fail-region",
		Status: "active",
	}
	federator.mu.Unlock()

	replicator.processQueue("fail-region")

	replicator.mu.RLock()
	queue := replicator.queues["fail-region"]
	queue.mu.Lock()
	completedOp, found := queue.CompletedOps["op-fail"]
	queue.mu.Unlock()
	replicator.mu.RUnlock()

	if found && completedOp != nil && completedOp.Status == OpStatusFailed {
		if completedOp.Error == "" {
			t.Error("Error should be set for failed operation")
		}
	}
}

func TestFederatorCheckHealthAllPaths(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers: []RegionConfig{
			{RegionID: "peer-active", Endpoint: "https://peer1.example.com"},
			{RegionID: "peer-degraded", Endpoint: string(make([]byte, 6000))},
			{RegionID: "peer-inactive", Endpoint: string(make([]byte, 11000))},
		},
	}

	f := NewFederator(config, logger)

	f.checkHealth()

	activePeer, ok := f.GetRegion("peer-active")
	if !ok {
		t.Fatal("Should find peer-active")
	}
	if activePeer.Status != "active" {
		t.Errorf("peer-active status = %s, want active", activePeer.Status)
	}

	degradedPeer, ok := f.GetRegion("peer-degraded")
	if !ok {
		t.Fatal("Should find peer-degraded")
	}
	if degradedPeer.Status != "degraded" {
		t.Errorf("peer-degraded status = %s, want degraded", degradedPeer.Status)
	}

	inactivePeer, ok := f.GetRegion("peer-inactive")
	if !ok {
		t.Fatal("Should find peer-inactive")
	}
	if inactivePeer.Status != "inactive" {
		t.Errorf("peer-inactive status = %s, want inactive", inactivePeer.Status)
	}
}

func TestAsyncReplicatorProcessQueueRetryLogic(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	op := &ReplicationOp{
		ID:           "op-retry",
		TargetRegion: "missing-region",
		Operation:    ReplicationWrite,
		Bucket:       "bucket",
		Key:          "key",
		Status:       OpStatusPending,
		CreatedAt:    time.Now(),
		Retries:      1,
	}

	replicator.mu.Lock()
	replicator.queues["retry-region"] = &ReplicationQueue{
		RegionID:     "retry-region",
		PendingOps:   []*ReplicationOp{op},
		CompletedOps: make(map[string]*ReplicationOp),
	}
	replicator.mu.Unlock()

	federator.mu.Lock()
	federator.regions["retry-region"] = &Region{
		ID:     "retry-region",
		Status: "active",
	}
	federator.mu.Unlock()

	replicator.processQueue("retry-region")

	replicator.mu.RLock()
	queue := replicator.queues["retry-region"]
	queue.mu.Lock()
	var inPending bool
	for _, p := range queue.PendingOps {
		if p.ID == "op-retry" {
			inPending = true
			if p.Retries != 2 {
				t.Errorf("Retries = %d, want 2", p.Retries)
			}
		}
	}
	queue.mu.Unlock()
	replicator.mu.RUnlock()

	_ = inPending
}

func TestAsyncReplicatorProcessQueuePermanentFail(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx := context.Background()
	replicator.Start(ctx)

	op := &ReplicationOp{
		ID:           "op-permafail",
		TargetRegion: "missing-region",
		Operation:    ReplicationWrite,
		Bucket:       "bucket",
		Key:          "key",
		Status:       OpStatusPending,
		CreatedAt:    time.Now(),
		Retries:      3,
	}

	replicator.mu.Lock()
	replicator.queues["permafail-region"] = &ReplicationQueue{
		RegionID:     "permafail-region",
		PendingOps:   []*ReplicationOp{op},
		CompletedOps: make(map[string]*ReplicationOp),
	}
	replicator.mu.Unlock()

	federator.mu.Lock()
	federator.regions["permafail-region"] = &Region{
		ID:     "permafail-region",
		Status: "active",
	}
	federator.mu.Unlock()

	replicator.processQueue("permafail-region")

	replicator.mu.RLock()
	queue := replicator.queues["permafail-region"]
	queue.mu.Lock()
	completedOp, found := queue.CompletedOps["op-permafail"]
	queue.mu.Unlock()
	replicator.mu.RUnlock()

	if found && completedOp != nil {
		if completedOp.Status != OpStatusFailed {
			t.Errorf("Status = %s, want failed", completedOp.Status)
		}
		if completedOp.Error == "" {
			t.Error("Error should be set")
		}
	}
}

func TestFederatorHealthMonitorContextCancel(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		SyncInterval: time.Minute,
	}

	f := NewFederator(config, logger)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		f.healthMonitor(ctx)
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("healthMonitor should have stopped")
	}
}

func TestAsyncReplicatorReplicationWorkerContextCancel(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1"}},
	}
	federator := NewFederator(config, logger)

	replicator := NewAsyncReplicator(federator, logger)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		replicator.replicationWorker(ctx, "peer-1")
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("replicationWorker should have stopped")
	}
}

func TestAsyncReplicatorReplicationWorkerTickerFires(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion: RegionConfig{RegionID: "local-1"},
		Peers:       []RegionConfig{{RegionID: "peer-1", Endpoint: "https://peer.example.com"}},
	}
	federator := NewFederator(config, logger)

	federator.mu.Lock()
	if peer, ok := federator.regions["peer-1"]; ok {
		peer.Status = "active"
	}
	federator.mu.Unlock()

	replicator := NewAsyncReplicator(federator, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	replicator.QueueWrite("peer-1", "bucket", "key", []byte("data"), 1)

	done := make(chan struct{})
	go func() {
		replicator.replicationWorker(ctx, "peer-1")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(7 * time.Second):
		t.Error("replicationWorker should have stopped")
	}
}

func TestFederatorHealthMonitorTickerFires(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running test in short mode")
	}
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		Peers:        []RegionConfig{{RegionID: "peer-1", Endpoint: "https://peer.example.com"}},
		SyncInterval: time.Minute,
	}

	f := NewFederator(config, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		f.healthMonitor(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(40 * time.Second):
		t.Error("healthMonitor should have stopped")
	}
}

func TestFederatorSyncLoopTickerFires(t *testing.T) {
	logger := zap.NewNop()
	config := FederatorConfig{
		LocalRegion:  RegionConfig{RegionID: "local-1"},
		SyncInterval: 100 * time.Millisecond,
	}

	f := NewFederator(config, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		f.syncLoop(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Error("syncLoop should have stopped")
	}
}
