package backup

import (
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager()
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
}

func TestManager_CreateBackup(t *testing.T) {
	mgr := NewManager()

	config := &BackupConfig{
		Name:      "test-backup",
		Type:      "full",
		CreatedAt: time.Now(),
	}

	id, err := mgr.CreateBackup(config)
	if err != nil {
		t.Fatalf("CreateBackup failed: %v", err)
	}

	if id == "" {
		t.Error("Backup ID should not be empty")
	}
}

func TestManager_GetBackup(t *testing.T) {
	mgr := NewManager()

	config := &BackupConfig{Name: "test-backup"}
	id, _ := mgr.CreateBackup(config)

	backup, err := mgr.GetBackup(id)
	if err != nil {
		t.Fatalf("GetBackup failed: %v", err)
	}

	if backup.ID != id {
		t.Errorf("Backup ID = %s, want %s", backup.ID, id)
	}
}

func TestManager_GetBackup_NotFound(t *testing.T) {
	mgr := NewManager()

	_, err := mgr.GetBackup("non-existent")
	if err == nil {
		t.Error("Should fail for non-existent backup")
	}
}

func TestManager_DeleteBackup(t *testing.T) {
	mgr := NewManager()

	config := &BackupConfig{Name: "test-backup"}
	id, _ := mgr.CreateBackup(config)

	err := mgr.DeleteBackup(id)
	if err != nil {
		t.Fatalf("DeleteBackup failed: %v", err)
	}

	_, err = mgr.GetBackup(id)
	if err == nil {
		t.Error("Backup should be deleted")
	}
}

func TestManager_ListBackups(t *testing.T) {
	mgr := NewManager()

	// Empty list
	backups := mgr.ListBackups()
	if len(backups) != 0 {
		t.Errorf("Empty list should have 0 backups")
	}

	mgr.CreateBackup(&BackupConfig{Name: "backup1"})
	mgr.CreateBackup(&BackupConfig{Name: "backup2"})

	backups = mgr.ListBackups()
	if len(backups) != 2 {
		t.Errorf("Backup count = %d, want 2", len(backups))
	}
}

func TestManager_RestoreBackup(t *testing.T) {
	mgr := NewManager()

	config := &BackupConfig{Name: "test-backup"}
	id, _ := mgr.CreateBackup(config)

	err := mgr.RestoreBackup(id)
	if err != nil {
		t.Fatalf("RestoreBackup failed: %v", err)
	}
}

func TestBackupConfig(t *testing.T) {
	config := &BackupConfig{
		Name:        "daily-backup",
		Type:        "incremental",
		Destination:  "/backup/daily",
		Compression: true,
		Encryption:  true,
	}

	if config.Name != "daily-backup" {
		t.Errorf("Name = %s, want daily-backup", config.Name)
	}

	if !config.Compression {
		t.Error("Compression should be true")
	}
}

func TestBackupStatus(t *testing.T) {
	status := &BackupStatus{
		BackupID:    "backup-123",
		Status:      "completed",
		StartedAt:   time.Now(),
		CompletedAt: time.Now(),
		BytesTotal:  1024,
		BytesDone:   1024,
	}

	if status.Status != "completed" {
		t.Errorf("Status = %s, want completed", status.Status)
	}

	if status.BytesDone != status.BytesTotal {
		t.Error("Backup should be 100% complete")
	}
}

func TestManager_ScheduleBackup(t *testing.T) {
	mgr := NewManager()

	schedule := &BackupSchedule{
		Name:     "daily",
		CronExpr: "0 2 * * *",
		Config:   BackupConfig{Name: "scheduled-backup"},
	}

	err := mgr.ScheduleBackup(schedule)
	if err != nil {
		t.Fatalf("ScheduleBackup failed: %v", err)
	}
}

func TestManager_CancelSchedule(t *testing.T) {
	mgr := NewManager()

	schedule := &BackupSchedule{
		Name:     "daily",
		CronExpr: "0 2 * * *",
	}
	mgr.ScheduleBackup(schedule)

	err := mgr.CancelSchedule("daily")
	if err != nil {
		t.Fatalf("CancelSchedule failed: %v", err)
	}
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			name := string(rune('A' + id))
			mgr.CreateBackup(&BackupConfig{Name: name})
			mgr.ListBackups()
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
