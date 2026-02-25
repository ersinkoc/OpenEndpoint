package cluster

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNewBackupManager(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)
	if mgr == nil {
		t.Fatal("NewBackupManager returned nil")
	}
	if mgr.targets == nil {
		t.Error("targets map should be initialized")
	}
	if mgr.jobs == nil {
		t.Error("jobs map should be initialized")
	}
}

func TestBackupManager_AddTarget(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetLocal,
		Endpoint: "/backup",
		Enabled:  true,
	}

	err := mgr.AddTarget(target)
	if err != nil {
		t.Fatalf("AddTarget failed: %v", err)
	}
	if target.ID == "" {
		t.Error("Target ID should be auto-generated")
	}
	if target.CreatedAt.IsZero() {
		t.Error("CreatedAt should be auto-set")
	}
}

func TestBackupManager_AddTargetWithID(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		ID:        "custom-id",
		Name:      "test-target",
		Type:      BackupTargetLocal,
		Endpoint:  "/backup",
		Enabled:   true,
		CreatedAt: time.Now(),
	}

	err := mgr.AddTarget(target)
	if err != nil {
		t.Fatalf("AddTarget failed: %v", err)
	}
	if target.ID != "custom-id" {
		t.Errorf("Target ID = %s, want custom-id", target.ID)
	}
}

func TestBackupManager_AddTargetInvalid(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name: "", // Empty name should fail validation
		Type: BackupTargetLocal,
	}

	err := mgr.AddTarget(target)
	if err == nil {
		t.Error("AddTarget should fail for invalid target")
	}
}

func TestBackupManager_RemoveTarget(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetLocal,
		Endpoint: "/backup",
		Enabled:  true,
	}
	mgr.AddTarget(target)

	err := mgr.RemoveTarget(target.ID)
	if err != nil {
		t.Fatalf("RemoveTarget failed: %v", err)
	}

	_, ok := mgr.GetTarget(target.ID)
	if ok {
		t.Error("Target should be removed")
	}
}

func TestBackupManager_RemoveTargetNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	err := mgr.RemoveTarget("nonexistent")
	if err == nil {
		t.Error("RemoveTarget should fail for nonexistent target")
	}
}

func TestBackupManager_GetTarget(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetLocal,
		Endpoint: "/backup",
		Enabled:  true,
	}
	mgr.AddTarget(target)

	found, ok := mgr.GetTarget(target.ID)
	if !ok {
		t.Fatal("GetTarget should find the target")
	}
	if found.Name != "test-target" {
		t.Errorf("Target Name = %s, want test-target", found.Name)
	}
}

func TestBackupManager_GetTargetNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	_, ok := mgr.GetTarget("nonexistent")
	if ok {
		t.Error("GetTarget should return false for nonexistent target")
	}
}

func TestBackupManager_ListTargets(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target1 := &BackupTarget{Name: "target1", Type: BackupTargetLocal, Endpoint: "/backup1", Enabled: true}
	target2 := &BackupTarget{Name: "target2", Type: BackupTargetS3, Endpoint: "https://s3.amazonaws.com", Enabled: true}

	mgr.AddTarget(target1)
	mgr.AddTarget(target2)

	targets := mgr.ListTargets()
	if len(targets) != 2 {
		t.Errorf("ListTargets returned %d targets, want 2", len(targets))
	}
}

func TestBackupManager_ListTargetsEmpty(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	targets := mgr.ListTargets()
	if len(targets) != 0 {
		t.Errorf("ListTargets returned %d targets, want 0", len(targets))
	}
}

func TestBackupManager_CreateBackupJob(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetLocal,
		Endpoint: "/backup",
		Enabled:  true,
	}
	mgr.AddTarget(target)

	job, err := mgr.CreateBackupJob("test-job", target.ID, "test-bucket", BackupJobFull)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}
	if job == nil {
		t.Fatal("CreateBackupJob returned nil job")
	}
	if job.ID == "" {
		t.Error("Job ID should be auto-generated")
	}
	if job.Status != BackupStatusPending {
		t.Errorf("Job Status = %s, want pending", job.Status)
	}
}

func TestBackupManager_CreateBackupJobTargetNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	_, err := mgr.CreateBackupJob("test-job", "nonexistent", "test-bucket", BackupJobFull)
	if err == nil {
		t.Error("CreateBackupJob should fail for nonexistent target")
	}
}

func TestBackupManager_CreateBackupJobTargetDisabled(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetLocal,
		Endpoint: "/backup",
		Enabled:  false,
	}
	mgr.AddTarget(target)

	_, err := mgr.CreateBackupJob("test-job", target.ID, "test-bucket", BackupJobFull)
	if err == nil {
		t.Error("CreateBackupJob should fail for disabled target")
	}
}

func TestBackupManager_GetJob(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{Name: "test-target", Type: BackupTargetLocal, Endpoint: "/backup", Enabled: true}
	mgr.AddTarget(target)

	job, err := mgr.CreateBackupJob("test-job", target.ID, "test-bucket", BackupJobFull)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	found, ok := mgr.GetJob(job.ID)
	if !ok {
		t.Fatal("GetJob should find the job")
	}
	if found.Name != "test-job" {
		t.Errorf("Job Name = %s, want test-job", found.Name)
	}
}

func TestBackupManager_GetJobNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	_, ok := mgr.GetJob("nonexistent")
	if ok {
		t.Error("GetJob should return false for nonexistent job")
	}
}

func TestBackupManager_ListJobs(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{Name: "test-target", Type: BackupTargetLocal, Endpoint: "/backup", Enabled: true}
	mgr.AddTarget(target)

	mgr.CreateBackupJob("job1", target.ID, "bucket1", BackupJobFull)
	mgr.CreateBackupJob("job2", target.ID, "bucket2", BackupJobIncremental)

	jobs := mgr.ListJobs()
	if len(jobs) != 2 {
		t.Errorf("ListJobs returned %d jobs, want 2", len(jobs))
	}
}

func TestBackupManager_ListJobsEmpty(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	jobs := mgr.ListJobs()
	if len(jobs) != 0 {
		t.Errorf("ListJobs returned %d jobs, want 0", len(jobs))
	}
}

func TestBackupManager_CancelJob(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{Name: "test-target", Type: BackupTargetLocal, Endpoint: "/backup", Enabled: true}
	mgr.AddTarget(target)

	job, err := mgr.CreateBackupJob("test-job", target.ID, "test-bucket", BackupJobFull)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	// Job must be running to cancel
	job.Status = BackupStatusRunning

	err = mgr.CancelJob(job.ID)
	if err != nil {
		t.Fatalf("CancelJob failed: %v", err)
	}

	found, _ := mgr.GetJob(job.ID)
	if found.Status != BackupStatusCancelled {
		t.Errorf("Job Status = %s, want cancelled", found.Status)
	}
}

func TestBackupManager_CancelJobNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	err := mgr.CancelJob("nonexistent")
	if err == nil {
		t.Error("CancelJob should fail for nonexistent job")
	}
}

func TestBackupManager_RunBackupJobNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	err := mgr.RunBackupJob(context.Background(), "nonexistent")
	if err == nil {
		t.Error("RunBackupJob should fail for nonexistent job")
	}
}

func TestBackupManager_ValidateTarget(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	tests := []struct {
		name    string
		target  *BackupTarget
		wantErr bool
	}{
		{"valid local target", &BackupTarget{Name: "test", Type: BackupTargetLocal, Endpoint: "/backup", Enabled: true}, false},
		{"empty name", &BackupTarget{Name: "", Type: BackupTargetLocal, Endpoint: "/backup", Enabled: true}, true},
		{"empty type", &BackupTarget{Name: "test", Type: "", Endpoint: "/backup", Enabled: true}, true},
		{"empty endpoint", &BackupTarget{Name: "test", Type: BackupTargetS3, Endpoint: "", Enabled: true}, true},
		{"s3 with endpoint", &BackupTarget{Name: "test", Type: BackupTargetS3, Endpoint: "https://s3.amazonaws.com", Enabled: true}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mgr.validateTarget(tt.target)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTarget() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBackupTargetStruct(t *testing.T) {
	now := time.Now()
	target := BackupTarget{
		ID:        "target-1",
		Name:      "Test Target",
		Type:      BackupTargetS3,
		Endpoint:  "https://s3.amazonaws.com",
		Bucket:    "my-bucket",
		Prefix:    "backups/",
		Enabled:   true,
		CreatedAt: now,
	}

	if target.ID != "target-1" {
		t.Error("ID mismatch")
	}
	if target.Type != BackupTargetS3 {
		t.Error("Type mismatch")
	}
}

func TestBackupJobStruct(t *testing.T) {
	now := time.Now()
	job := BackupJob{
		ID:        "job-1",
		Name:      "Test Job",
		TargetID:  "target-1",
		Bucket:    "bucket",
		Type:      BackupJobFull,
		Status:    BackupStatusPending,
		StartedAt: now,
		Objects:   100,
		SizeBytes: 1024 * 1024,
	}

	if job.ID != "job-1" {
		t.Error("ID mismatch")
	}
	if job.Type != BackupJobFull {
		t.Error("Type mismatch")
	}
}

func TestBackupProgressStruct(t *testing.T) {
	progress := BackupProgress{
		TotalObjects:     100,
		CompletedObjects: 50,
		TotalBytes:       10000,
		CompletedBytes:   5000,
		PercentComplete:  50.0,
	}

	if progress.TotalObjects != 100 {
		t.Error("TotalObjects mismatch")
	}
	if progress.PercentComplete != 50.0 {
		t.Error("PercentComplete mismatch")
	}
}

func TestBackupAuthStruct(t *testing.T) {
	auth := BackupAuth{
		AccessKey: "access",
		SecretKey: "secret",
		Token:     "token",
	}

	if auth.AccessKey != "access" {
		t.Error("AccessKey mismatch")
	}
}

func TestBackupTypes(t *testing.T) {
	if BackupTargetS3 != "s3" {
		t.Error("BackupTargetS3 should be 's3'")
	}
	if BackupTargetGCS != "gcs" {
		t.Error("BackupTargetGCS should be 'gcs'")
	}
	if BackupTargetAzure != "azure" {
		t.Error("BackupTargetAzure should be 'azure'")
	}
	if BackupTargetNFS != "nfs" {
		t.Error("BackupTargetNFS should be 'nfs'")
	}
	if BackupTargetLocal != "local" {
		t.Error("BackupTargetLocal should be 'local'")
	}
}

func TestBackupJobTypes(t *testing.T) {
	if BackupJobFull != "full" {
		t.Error("BackupJobFull should be 'full'")
	}
	if BackupJobIncremental != "incremental" {
		t.Error("BackupJobIncremental should be 'incremental'")
	}
}

func TestBackupStatuses(t *testing.T) {
	if BackupStatusPending != "pending" {
		t.Error("BackupStatusPending should be 'pending'")
	}
	if BackupStatusRunning != "running" {
		t.Error("BackupStatusRunning should be 'running'")
	}
	if BackupStatusComplete != "complete" {
		t.Error("BackupStatusComplete should be 'complete'")
	}
	if BackupStatusFailed != "failed" {
		t.Error("BackupStatusFailed should be 'failed'")
	}
	if BackupStatusCancelled != "cancelled" {
		t.Error("BackupStatusCancelled should be 'cancelled'")
	}
}

func TestBackupManager_RunBackupJob(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetS3,
		Endpoint: "https://s3.amazonaws.com",
		Bucket:   "backup-bucket",
		Enabled:  true,
	}
	mgr.AddTarget(target)

	job, err := mgr.CreateBackupJob("test-job", target.ID, "source-bucket", BackupJobFull)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	ctx := context.Background()
	err = mgr.RunBackupJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("RunBackupJob failed: %v", err)
	}

	found, _ := mgr.GetJob(job.ID)
	if found.Status != BackupStatusComplete {
		t.Errorf("Job Status = %s, want complete", found.Status)
	}
}

func TestBackupManager_RunBackupJobLocal(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetLocal,
		Endpoint: "/backup",
		Enabled:  true,
	}
	mgr.AddTarget(target)

	job, err := mgr.CreateBackupJob("test-job", target.ID, "source-bucket", BackupJobFull)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	ctx := context.Background()
	err = mgr.RunBackupJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("RunBackupJob failed: %v", err)
	}

	found, _ := mgr.GetJob(job.ID)
	if found.Status != BackupStatusComplete {
		t.Errorf("Job Status = %s, want complete", found.Status)
	}
}

func TestBackupManager_RunBackupJobGCS(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetGCS,
		Endpoint: "https://storage.googleapis.com",
		Bucket:   "backup-bucket",
		Enabled:  true,
	}
	mgr.AddTarget(target)

	job, err := mgr.CreateBackupJob("test-job", target.ID, "source-bucket", BackupJobFull)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	ctx := context.Background()
	err = mgr.RunBackupJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("RunBackupJob failed: %v", err)
	}
}

func TestBackupManager_RunBackupJobAzure(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetAzure,
		Endpoint: "https://blob.core.windows.net",
		Bucket:   "backup-container",
		Enabled:  true,
	}
	mgr.AddTarget(target)

	job, err := mgr.CreateBackupJob("test-job", target.ID, "source-bucket", BackupJobFull)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	ctx := context.Background()
	err = mgr.RunBackupJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("RunBackupJob failed: %v", err)
	}
}

func TestBackupManager_RunBackupJobNFS(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetNFS,
		Endpoint: "/mnt/backup",
		Enabled:  true,
	}
	mgr.AddTarget(target)

	job, err := mgr.CreateBackupJob("test-job", target.ID, "source-bucket", BackupJobFull)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	ctx := context.Background()
	err = mgr.RunBackupJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("RunBackupJob failed: %v", err)
	}
}

func TestBackupManager_RunBackupJobUnsupportedType(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetType("unsupported"),
		Endpoint: "/backup",
		Enabled:  true,
	}
	mgr.AddTarget(target)

	job, err := mgr.CreateBackupJob("test-job", target.ID, "source-bucket", BackupJobFull)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	ctx := context.Background()
	err = mgr.RunBackupJob(ctx, job.ID)
	if err == nil {
		t.Error("RunBackupJob should fail for unsupported target type")
	}
}

func TestBackupManager_RunBackupJobTargetRemoved(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetS3,
		Endpoint: "https://s3.amazonaws.com",
		Bucket:   "backup-bucket",
		Enabled:  true,
	}
	mgr.AddTarget(target)

	job, err := mgr.CreateBackupJob("test-job", target.ID, "source-bucket", BackupJobFull)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	mgr.RemoveTarget(target.ID)

	ctx := context.Background()
	err = mgr.RunBackupJob(ctx, job.ID)
	if err == nil {
		t.Error("RunBackupJob should fail when target is removed")
	}
}

func TestBackupManager_RunBackupJobIncremental(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetS3,
		Endpoint: "https://s3.amazonaws.com",
		Bucket:   "backup-bucket",
		Enabled:  true,
	}
	mgr.AddTarget(target)

	job, err := mgr.CreateBackupJob("test-job", target.ID, "source-bucket", BackupJobIncremental)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	ctx := context.Background()
	err = mgr.RunBackupJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("RunBackupJob failed: %v", err)
	}

	found, _ := mgr.GetJob(job.ID)
	if found.Type != BackupJobIncremental {
		t.Errorf("Job Type = %s, want incremental", found.Type)
	}
}

func TestMirrorConfigStruct(t *testing.T) {
	config := MirrorConfig{
		SourceCluster: "http://source:9000",
		TargetCluster: "http://target:9000",
		Bucket:        "mirror-bucket",
		Prefix:        "data/",
		Mode:          MirrorModeAsync,
		Interval:      time.Minute * 5,
		Enabled:       true,
	}

	if config.SourceCluster != "http://source:9000" {
		t.Error("SourceCluster mismatch")
	}
	if config.Mode != MirrorModeAsync {
		t.Error("Mode mismatch")
	}
}

func TestMirrorModes(t *testing.T) {
	if MirrorModeSync != "sync" {
		t.Error("MirrorModeSync should be 'sync'")
	}
	if MirrorModeAsync != "async" {
		t.Error("MirrorModeAsync should be 'async'")
	}
}
