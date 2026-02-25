package iam

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNewManager(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
	if mgr.users == nil {
		t.Error("users map should be initialized")
	}
	if mgr.groups == nil {
		t.Error("groups map should be initialized")
	}
	if mgr.policies == nil {
		t.Error("policies map should be initialized")
	}
}

func TestCreateUser(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, err := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}
	if user == nil {
		t.Fatal("User should not be nil")
	}
	if user.Username != "testuser" {
		t.Errorf("Username = %s, want testuser", user.Username)
	}
	if user.Email != "test@example.com" {
		t.Errorf("Email = %s, want test@example.com", user.Email)
	}
	if user.TenantID != "tenant1" {
		t.Errorf("TenantID = %s, want tenant1", user.TenantID)
	}
	if user.Status != "active" {
		t.Errorf("Status = %s, want active", user.Status)
	}
	if user.ID == "" {
		t.Error("ID should not be empty")
	}
}

func TestGetUser(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")

	retrieved, ok := mgr.GetUser(user.ID)
	if !ok {
		t.Fatal("Should find user")
	}
	if retrieved.Username != "testuser" {
		t.Errorf("Username = %s, want testuser", retrieved.Username)
	}
}

func TestGetUserNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, ok := mgr.GetUser("non-existent-id")
	if ok {
		t.Error("Should not find non-existent user")
	}
}

func TestGetUserByName(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	mgr.CreateUser("tenant1", "testuser", "test@example.com")

	user, ok := mgr.GetUserByName("tenant1", "testuser")
	if !ok {
		t.Fatal("Should find user by name")
	}
	if user.Username != "testuser" {
		t.Errorf("Username = %s, want testuser", user.Username)
	}
}

func TestGetUserByNameNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, ok := mgr.GetUserByName("tenant1", "nonexistent")
	if ok {
		t.Error("Should not find non-existent user")
	}
}

func TestListUsers(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	users := mgr.ListUsers("tenant1")
	if len(users) != 0 {
		t.Errorf("Empty list should have 0 users, got %d", len(users))
	}

	mgr.CreateUser("tenant1", "user1", "user1@example.com")
	mgr.CreateUser("tenant1", "user2", "user2@example.com")
	mgr.CreateUser("tenant2", "user3", "user3@example.com")

	users = mgr.ListUsers("tenant1")
	if len(users) != 2 {
		t.Errorf("User count = %d, want 2", len(users))
	}

	users = mgr.ListUsers("tenant2")
	if len(users) != 1 {
		t.Errorf("User count = %d, want 1", len(users))
	}
}

func TestDeleteUser(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")

	err := mgr.DeleteUser(user.ID)
	if err != nil {
		t.Fatalf("DeleteUser failed: %v", err)
	}

	_, ok := mgr.GetUser(user.ID)
	if ok {
		t.Error("User should be deleted")
	}
}

func TestDeleteUserNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	err := mgr.DeleteUser("nonexistent")
	if err == nil {
		t.Error("DeleteUser should fail for non-existent user")
	}
}

func TestCreateAccessKey(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")

	key, err := mgr.CreateAccessKey(user.ID)
	if err != nil {
		t.Fatalf("CreateAccessKey failed: %v", err)
	}
	if key.ID == "" {
		t.Error("AccessKey ID should not be empty")
	}
	if key.Secret == "" {
		t.Error("AccessKey Secret should not be empty")
	}
	if key.Status != "active" {
		t.Errorf("Status = %s, want active", key.Status)
	}
}

func TestCreateAccessKeyUserNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, err := mgr.CreateAccessKey("nonexistent")
	if err == nil {
		t.Error("CreateAccessKey should fail for non-existent user")
	}
}

func TestCreateGroup(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	group, err := mgr.CreateGroup("tenant1", "developers")
	if err != nil {
		t.Fatalf("CreateGroup failed: %v", err)
	}
	if group == nil {
		t.Fatal("Group should not be nil")
	}
	if group.Name != "developers" {
		t.Errorf("Name = %s, want developers", group.Name)
	}
	if group.TenantID != "tenant1" {
		t.Errorf("TenantID = %s, want tenant1", group.TenantID)
	}
	if group.ID == "" {
		t.Error("ID should not be empty")
	}
}

func TestAddUserToGroup(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	group, _ := mgr.CreateGroup("tenant1", "developers")

	err := mgr.AddUserToGroup(user.ID, group.ID)
	if err != nil {
		t.Fatalf("AddUserToGroup failed: %v", err)
	}

	updatedUser, _ := mgr.GetUser(user.ID)
	if len(updatedUser.Groups) != 1 {
		t.Errorf("User should have 1 group, got %d", len(updatedUser.Groups))
	}

	updatedGroup := mgr.groups[group.ID]
	if len(updatedGroup.Members) != 1 {
		t.Errorf("Group should have 1 member, got %d", len(updatedGroup.Members))
	}
}

func TestAddUserToGroupAlreadyMember(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	group, _ := mgr.CreateGroup("tenant1", "developers")

	mgr.AddUserToGroup(user.ID, group.ID)
	err := mgr.AddUserToGroup(user.ID, group.ID)

	if err == nil {
		t.Error("AddUserToGroup should fail if user already in group")
	}
}

func TestAddUserToGroupGroupNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")

	err := mgr.AddUserToGroup(user.ID, "nonexistent")
	if err == nil {
		t.Error("AddUserToGroup should fail for non-existent group")
	}
}

func TestAddUserToGroupUserNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	group, _ := mgr.CreateGroup("tenant1", "developers")

	err := mgr.AddUserToGroup("nonexistent", group.ID)
	if err == nil {
		t.Error("AddUserToGroup should fail for non-existent user")
	}
}

func TestRemoveUserFromGroup(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	group, _ := mgr.CreateGroup("tenant1", "developers")
	mgr.AddUserToGroup(user.ID, group.ID)

	err := mgr.RemoveUserFromGroup(user.ID, group.ID)
	if err != nil {
		t.Fatalf("RemoveUserFromGroup failed: %v", err)
	}

	updatedUser, _ := mgr.GetUser(user.ID)
	if len(updatedUser.Groups) != 0 {
		t.Errorf("User should have 0 groups, got %d", len(updatedUser.Groups))
	}
}

func TestRemoveUserFromGroupMultipleGroups(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	group1, _ := mgr.CreateGroup("tenant1", "developers")
	group2, _ := mgr.CreateGroup("tenant1", "admins")
	mgr.AddUserToGroup(user.ID, group1.ID)
	mgr.AddUserToGroup(user.ID, group2.ID)

	err := mgr.RemoveUserFromGroup(user.ID, group1.ID)
	if err != nil {
		t.Fatalf("RemoveUserFromGroup failed: %v", err)
	}

	updatedUser, _ := mgr.GetUser(user.ID)
	if len(updatedUser.Groups) != 1 {
		t.Errorf("User should have 1 group, got %d", len(updatedUser.Groups))
	}
	if updatedUser.Groups[0] != group2.ID {
		t.Errorf("User should be in group2, got %s", updatedUser.Groups[0])
	}
}

func TestRemoveUserFromGroupMultipleMembers(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user1, _ := mgr.CreateUser("tenant1", "user1", "user1@example.com")
	user2, _ := mgr.CreateUser("tenant1", "user2", "user2@example.com")
	group, _ := mgr.CreateGroup("tenant1", "developers")
	mgr.AddUserToGroup(user1.ID, group.ID)
	mgr.AddUserToGroup(user2.ID, group.ID)

	err := mgr.RemoveUserFromGroup(user1.ID, group.ID)
	if err != nil {
		t.Fatalf("RemoveUserFromGroup failed: %v", err)
	}

	updatedGroup := mgr.groups[group.ID]
	if len(updatedGroup.Members) != 1 {
		t.Errorf("Group should have 1 member, got %d", len(updatedGroup.Members))
	}
	if updatedGroup.Members[0] != user2.ID {
		t.Errorf("Group should still have user2, got %s", updatedGroup.Members[0])
	}
}

func TestRemoveUserFromGroupGroupNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")

	err := mgr.RemoveUserFromGroup(user.ID, "nonexistent")
	if err == nil {
		t.Error("RemoveUserFromGroup should fail for non-existent group")
	}
}

func TestCreatePolicy(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	doc := PolicyDoc{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Effect:    "Allow",
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"*"},
			},
		},
	}

	policy, err := mgr.CreatePolicy("tenant1", "TestPolicy", doc)
	if err != nil {
		t.Fatalf("CreatePolicy failed: %v", err)
	}
	if policy == nil {
		t.Fatal("Policy should not be nil")
	}
	if policy.Name != "TestPolicy" {
		t.Errorf("Name = %s, want TestPolicy", policy.Name)
	}
	if policy.ID == "" {
		t.Error("ID should not be empty")
	}
	if policy.Arn == "" {
		t.Error("Arn should not be empty")
	}
}

func TestGetPolicy(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	doc := PolicyDoc{Version: "2012-10-17"}
	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", doc)

	retrieved, ok := mgr.GetPolicy(policy.ID)
	if !ok {
		t.Fatal("Should find policy")
	}
	if retrieved.Name != "TestPolicy" {
		t.Errorf("Name = %s, want TestPolicy", retrieved.Name)
	}
}

func TestGetPolicyNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, ok := mgr.GetPolicy("nonexistent")
	if ok {
		t.Error("Should not find non-existent policy")
	}
}

func TestGetPolicyByArn(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	doc := PolicyDoc{Version: "2012-10-17"}
	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", doc)

	retrieved, ok := mgr.GetPolicyByArn(policy.Arn)
	if !ok {
		t.Fatal("Should find policy by ARN")
	}
	if retrieved.ID != policy.ID {
		t.Errorf("ID = %s, want %s", retrieved.ID, policy.ID)
	}
}

func TestAttachPolicyToUser(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	doc := PolicyDoc{Version: "2012-10-17"}
	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", doc)

	err := mgr.AttachPolicy(policy.ID, user.ID, "user")
	if err != nil {
		t.Fatalf("AttachPolicy failed: %v", err)
	}

	updatedUser, _ := mgr.GetUser(user.ID)
	if len(updatedUser.PolicyArns) != 1 {
		t.Errorf("User should have 1 policy, got %d", len(updatedUser.PolicyArns))
	}
}

func TestAttachPolicyToGroup(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	group, _ := mgr.CreateGroup("tenant1", "developers")
	doc := PolicyDoc{Version: "2012-10-17"}
	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", doc)

	err := mgr.AttachPolicy(policy.ID, group.ID, "group")
	if err != nil {
		t.Fatalf("AttachPolicy failed: %v", err)
	}

	updatedGroup := mgr.groups[group.ID]
	if len(updatedGroup.PolicyArns) != 1 {
		t.Errorf("Group should have 1 policy, got %d", len(updatedGroup.PolicyArns))
	}
}

func TestAttachPolicyInvalidEntityType(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	doc := PolicyDoc{Version: "2012-10-17"}
	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", doc)

	err := mgr.AttachPolicy(policy.ID, "entity1", "invalid")
	if err == nil {
		t.Error("AttachPolicy should fail for invalid entity type")
	}
}

func TestDetachPolicyFromUser(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	doc := PolicyDoc{Version: "2012-10-17"}
	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", doc)
	mgr.AttachPolicy(policy.ID, user.ID, "user")

	err := mgr.DetachPolicy(policy.Arn, user.ID, "user")
	if err != nil {
		t.Fatalf("DetachPolicy failed: %v", err)
	}

	updatedUser, _ := mgr.GetUser(user.ID)
	if len(updatedUser.PolicyArns) != 0 {
		t.Errorf("User should have 0 policies, got %d", len(updatedUser.PolicyArns))
	}
}

func TestEvaluatePolicy(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	doc := PolicyDoc{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Effect:    "Allow",
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"*"},
			},
		},
	}
	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", doc)
	mgr.AttachPolicy(policy.ID, user.ID, "user")

	allowed, err := mgr.EvaluatePolicy("tenant1", user.ID, "s3:GetObject", "*")
	if err != nil {
		t.Fatalf("EvaluatePolicy failed: %v", err)
	}
	if !allowed {
		t.Error("Action should be allowed")
	}

	allowed, _ = mgr.EvaluatePolicy("tenant1", user.ID, "s3:DeleteObject", "*")
	if allowed {
		t.Error("Action should not be allowed")
	}
}

func TestEvaluatePolicyUserNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, err := mgr.EvaluatePolicy("tenant1", "nonexistent", "s3:GetObject", "*")
	if err == nil {
		t.Error("EvaluatePolicy should fail for non-existent user")
	}
}

func TestPolicyFromJSON(t *testing.T) {
	jsonData := `{"id":"test-id","name":"TestPolicy","version":"2012-10-17"}`

	policy, err := PolicyFromJSON([]byte(jsonData))
	if err != nil {
		t.Fatalf("PolicyFromJSON failed: %v", err)
	}
	if policy.Name != "TestPolicy" {
		t.Errorf("Name = %s, want TestPolicy", policy.Name)
	}
}

func TestPolicyFromJSONInvalid(t *testing.T) {
	_, err := PolicyFromJSON([]byte("invalid json"))
	if err == nil {
		t.Error("PolicyFromJSON should fail for invalid JSON")
	}
}

func TestUserStruct(t *testing.T) {
	user := &User{
		ID:           "test-id",
		TenantID:     "tenant1",
		Username:     "testuser",
		Email:        "test@example.com",
		Groups:       []string{"group1"},
		PolicyArns:   []string{"arn:policy:1"},
		Status:       "active",
		AccessKeys:   []AccessKey{{ID: "key1", Secret: "secret1"}},
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
	}

	if user.ID != "test-id" {
		t.Errorf("ID = %v, want test-id", user.ID)
	}
}

func TestAccessKeyStruct(t *testing.T) {
	expires := time.Now().Add(24 * time.Hour)
	key := AccessKey{
		ID:        "AKIA123",
		Secret:    "secret123",
		Status:    "active",
		CreatedAt: time.Now(),
		ExpiresAt: &expires,
	}

	if key.ID != "AKIA123" {
		t.Errorf("ID = %v, want AKIA123", key.ID)
	}
}

func TestGroupStruct(t *testing.T) {
	group := &Group{
		ID:         "group-id",
		TenantID:   "tenant1",
		Name:       "developers",
		PolicyArns: []string{"arn:policy:1"},
		Members:    []string{"user1", "user2"},
		CreatedAt:  time.Now(),
	}

	if group.Name != "developers" {
		t.Errorf("Name = %v, want developers", group.Name)
	}
}

func TestPolicyStruct(t *testing.T) {
	policy := &Policy{
		ID:         "policy-id",
		TenantID:   "tenant1",
		Name:       "TestPolicy",
		Arn:        "arn:test:policy",
		Version:    "2012-10-17",
		IsAttached: true,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	if policy.Name != "TestPolicy" {
		t.Errorf("Name = %v, want TestPolicy", policy.Name)
	}
}

func TestStatementStruct(t *testing.T) {
	stmt := Statement{
		Sid:        "Statement1",
		Effect:     "Allow",
		Actions:    []string{"s3:GetObject"},
		Resources:  []string{"*"},
		Conditions: map[string]map[string]interface{}{},
	}

	if stmt.Effect != "Allow" {
		t.Errorf("Effect = %v, want Allow", stmt.Effect)
	}
}

func TestPrincipalStruct(t *testing.T) {
	p := Principal{
		Type:   "AWS",
		Values: []string{"arn:aws:iam::123:user/test"},
	}

	if p.Type != "AWS" {
		t.Errorf("Type = %v, want AWS", p.Type)
	}
}

func TestRoleStruct(t *testing.T) {
	role := &Role{
		ID:         "role-id",
		TenantID:   "tenant1",
		Name:       "TestRole",
		Arn:        "arn:test:role",
		Path:       "/",
		PolicyArns: []string{"arn:policy:1"},
		CreatedAt:  time.Now(),
	}

	if role.Name != "TestRole" {
		t.Errorf("Name = %v, want TestRole", role.Name)
	}
}

func TestDetachPolicyFromNonExistentUser(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", PolicyDoc{Version: "2012-10-17"})

	err := mgr.DetachPolicy(policy.Arn, "nonexistent-user", "user")
	if err == nil {
		t.Error("DetachPolicy should fail for non-existent user")
	}
}

func TestDetachPolicyFromNonExistentGroup(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", PolicyDoc{Version: "2012-10-17"})

	err := mgr.DetachPolicy(policy.Arn, "nonexistent-group", "group")
	if err == nil {
		t.Error("DetachPolicy should fail for non-existent group")
	}
}

func TestDetachPolicyInvalidEntityType(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", PolicyDoc{Version: "2012-10-17"})

	err := mgr.DetachPolicy(policy.Arn, "some-id", "invalid-type")
	if err == nil {
		t.Error("DetachPolicy should fail for invalid entity type")
	}
}

func TestEvaluatePolicyNonExistentUser(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, err := mgr.EvaluatePolicy("tenant1", "nonexistent-user", "s3:GetObject", "*")
	if err == nil {
		t.Error("EvaluatePolicy should fail for non-existent user")
	}
}

func TestEvaluatePolicyWithGroupPolicies(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	group, _ := mgr.CreateGroup("tenant1", "developers")
	mgr.AddUserToGroup(user.ID, group.ID)

	doc := PolicyDoc{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Effect:    "Allow",
				Actions:   []string{"s3:ListBucket"},
				Resources: []string{"*"},
			},
		},
	}
	policy, _ := mgr.CreatePolicy("tenant1", "GroupPolicy", doc)
	mgr.AttachPolicy(policy.ID, group.ID, "group")

	allowed, err := mgr.EvaluatePolicy("tenant1", user.ID, "s3:ListBucket", "*")
	if err != nil {
		t.Fatalf("EvaluatePolicy failed: %v", err)
	}
	if !allowed {
		t.Error("Action should be allowed via group policy")
	}
}

func TestEvaluatePolicyWithInlinePolicy(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	user.InlinePolicy = &Policy{
		Document: PolicyDoc{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Effect:    "Allow",
					Actions:   []string{"s3:PutObject"},
					Resources: []string{"*"},
				},
			},
		},
	}

	allowed, err := mgr.EvaluatePolicy("tenant1", user.ID, "s3:PutObject", "*")
	if err != nil {
		t.Fatalf("EvaluatePolicy failed: %v", err)
	}
	if !allowed {
		t.Error("Action should be allowed via inline policy")
	}
}

func TestEvaluatePolicyDenyEffect(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	doc := PolicyDoc{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Effect:    "Deny",
				Actions:   []string{"s3:DeleteObject"},
				Resources: []string{"*"},
			},
			{
				Effect:    "Allow",
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"*"},
			},
		},
	}
	policy, _ := mgr.CreatePolicy("tenant1", "MixedPolicy", doc)
	mgr.AttachPolicy(policy.ID, user.ID, "user")

	allowed, _ := mgr.EvaluatePolicy("tenant1", user.ID, "s3:DeleteObject", "*")
	if allowed {
		t.Error("Deny effect should skip to next statement")
	}

	allowed, _ = mgr.EvaluatePolicy("tenant1", user.ID, "s3:GetObject", "*")
	if !allowed {
		t.Error("Allow effect should permit action")
	}
}

func TestEvaluatePolicyActionNotMatched(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	doc := PolicyDoc{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Effect:    "Allow",
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"*"},
			},
		},
	}
	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", doc)
	mgr.AttachPolicy(policy.ID, user.ID, "user")

	allowed, _ := mgr.EvaluatePolicy("tenant1", user.ID, "s3:PutObject", "*")
	if allowed {
		t.Error("Action not in policy should be denied")
	}
}

func TestEvaluatePolicyResourceNotMatched(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	doc := PolicyDoc{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Effect:    "Allow",
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"bucket1/*"},
			},
		},
	}
	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", doc)
	mgr.AttachPolicy(policy.ID, user.ID, "user")

	allowed, _ := mgr.EvaluatePolicy("tenant1", user.ID, "s3:GetObject", "bucket2/key")
	if allowed {
		t.Error("Resource not in policy should be denied")
	}
}

func TestDetachPolicyFromUserNoMatch(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	doc := PolicyDoc{Version: "2012-10-17"}
	policy1, _ := mgr.CreatePolicy("tenant1", "Policy1", doc)
	policy2, _ := mgr.CreatePolicy("tenant1", "Policy2", doc)
	mgr.AttachPolicy(policy1.ID, user.ID, "user")

	err := mgr.DetachPolicy(policy2.Arn, user.ID, "user")
	if err != nil {
		t.Fatalf("DetachPolicy should succeed even if policy not attached: %v", err)
	}

	updatedUser, _ := mgr.GetUser(user.ID)
	if len(updatedUser.PolicyArns) != 1 {
		t.Errorf("User should still have 1 policy, got %d", len(updatedUser.PolicyArns))
	}
}

func TestDetachPolicyFromGroupNoMatch(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	group, _ := mgr.CreateGroup("tenant1", "developers")
	doc := PolicyDoc{Version: "2012-10-17"}
	policy1, _ := mgr.CreatePolicy("tenant1", "Policy1", doc)
	policy2, _ := mgr.CreatePolicy("tenant1", "Policy2", doc)
	mgr.AttachPolicy(policy1.ID, group.ID, "group")

	err := mgr.DetachPolicy(policy2.Arn, group.ID, "group")
	if err != nil {
		t.Fatalf("DetachPolicy should succeed even if policy not attached: %v", err)
	}

	updatedGroup := mgr.groups[group.ID]
	if len(updatedGroup.PolicyArns) != 1 {
		t.Errorf("Group should still have 1 policy, got %d", len(updatedGroup.PolicyArns))
	}
}

func TestDetachPolicyFromGroup(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	group, _ := mgr.CreateGroup("tenant1", "developers")
	doc := PolicyDoc{Version: "2012-10-17"}
	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", doc)
	mgr.AttachPolicy(policy.ID, group.ID, "group")

	err := mgr.DetachPolicy(policy.Arn, group.ID, "group")
	if err != nil {
		t.Fatalf("DetachPolicy failed: %v", err)
	}

	updatedGroup := mgr.groups[group.ID]
	if len(updatedGroup.PolicyArns) != 0 {
		t.Errorf("Group should have 0 policies, got %d", len(updatedGroup.PolicyArns))
	}
}

func TestRemoveUserFromGroupUserNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	group, _ := mgr.CreateGroup("tenant1", "developers")

	err := mgr.RemoveUserFromGroup("nonexistent-user", group.ID)
	if err != nil {
		t.Fatalf("RemoveUserFromGroup should succeed even if user not found: %v", err)
	}
}

func TestRemoveUserFromGroupUserNotInGroup(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")
	group, _ := mgr.CreateGroup("tenant1", "developers")

	err := mgr.RemoveUserFromGroup(user.ID, group.ID)
	if err != nil {
		t.Fatalf("RemoveUserFromGroup should succeed even if user not in group: %v", err)
	}

	updatedUser, _ := mgr.GetUser(user.ID)
	if len(updatedUser.Groups) != 0 {
		t.Errorf("User should have 0 groups, got %d", len(updatedUser.Groups))
	}
}

func TestGetPolicyByArnNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, ok := mgr.GetPolicyByArn("arn:nonexistent")
	if ok {
		t.Error("Should not find non-existent policy by ARN")
	}
}

func TestAttachPolicyPolicyNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)
	user, _ := mgr.CreateUser("tenant1", "testuser", "test@example.com")

	err := mgr.AttachPolicy("nonexistent-policy", user.ID, "user")
	if err == nil {
		t.Error("AttachPolicy should fail for non-existent policy")
	}
}

func TestAttachPolicyUserNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)
	doc := PolicyDoc{Version: "2012-10-17"}
	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", doc)

	err := mgr.AttachPolicy(policy.ID, "nonexistent-user", "user")
	if err == nil {
		t.Error("AttachPolicy should fail for non-existent user")
	}
}

func TestAttachPolicyGroupNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)
	doc := PolicyDoc{Version: "2012-10-17"}
	policy, _ := mgr.CreatePolicy("tenant1", "TestPolicy", doc)

	err := mgr.AttachPolicy(policy.ID, "nonexistent-group", "group")
	if err == nil {
		t.Error("AttachPolicy should fail for non-existent group")
	}
}
