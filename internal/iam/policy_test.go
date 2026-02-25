package iam

import (
	"testing"
	"time"
)

func TestNewPolicyEvaluator(t *testing.T) {
	evaluator := NewPolicyEvaluator()
	if evaluator == nil {
		t.Fatal("PolicyEvaluator should not be nil")
	}
	if evaluator.policies == nil {
		t.Error("policies map should be initialized")
	}
}

func TestPolicyEvaluatorAddPolicy(t *testing.T) {
	evaluator := NewPolicyEvaluator()
	policy := &IAMPolicy{
		ID:   "policy1",
		Name: "TestPolicy",
	}

	evaluator.AddPolicy(policy)

	if len(evaluator.policies) != 1 {
		t.Errorf("policies count = %d, want 1", len(evaluator.policies))
	}
}

func TestPolicyEvaluatorRemovePolicy(t *testing.T) {
	evaluator := NewPolicyEvaluator()
	policy := &IAMPolicy{
		ID:   "policy1",
		Name: "TestPolicy",
	}

	evaluator.AddPolicy(policy)
	evaluator.RemovePolicy("policy1")

	if len(evaluator.policies) != 0 {
		t.Errorf("policies count = %d, want 0", len(evaluator.policies))
	}
}

func TestPolicyEvaluatorEvaluateAllow(t *testing.T) {
	evaluator := NewPolicyEvaluator()
	policy := &IAMPolicy{
		ID:   "policy1",
		Name: "TestPolicy",
		Statements: []IAMStatement{
			{
				Effect:    "Allow",
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"*"},
				Principal: &IAMPrincipal{AWS: []string{"*"}},
			},
		},
	}

	evaluator.AddPolicy(policy)

	allowed := evaluator.Evaluate("*", "s3:GetObject", "*")
	if !allowed {
		t.Error("Action should be allowed")
	}
}

func TestPolicyEvaluatorEvaluateDeny(t *testing.T) {
	evaluator := NewPolicyEvaluator()
	policy := &IAMPolicy{
		ID:   "policy1",
		Name: "TestPolicy",
		Statements: []IAMStatement{
			{
				Effect:    "Deny",
				Actions:   []string{"s3:DeleteObject"},
				Resources: []string{"*"},
				Principal: &IAMPrincipal{AWS: []string{"*"}},
			},
		},
	}

	evaluator.AddPolicy(policy)

	allowed := evaluator.Evaluate("*", "s3:DeleteObject", "*")
	if allowed {
		t.Error("Action should be denied")
	}
}

func TestPolicyEvaluatorEvaluateNoMatch(t *testing.T) {
	evaluator := NewPolicyEvaluator()
	policy := &IAMPolicy{
		ID:   "policy1",
		Name: "TestPolicy",
		Statements: []IAMStatement{
			{
				Effect:    "Allow",
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"bucket1/*"},
				Principal: &IAMPrincipal{AWS: []string{"user1"}},
			},
		},
	}

	evaluator.AddPolicy(policy)

	allowed := evaluator.Evaluate("user2", "s3:GetObject", "bucket1/file")
	if allowed {
		t.Error("Action should not be allowed (principal mismatch)")
	}
}

func TestPolicyEvaluatorMatchPrincipals(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if !evaluator.matchPrincipals([]string{"*"}, "anyone") {
		t.Error("* should match any principal")
	}

	if !evaluator.matchPrincipals([]string{"user1"}, "user1") {
		t.Error("Exact match should work")
	}

	if evaluator.matchPrincipals([]string{"user1"}, "user2") {
		t.Error("Non-matching principal should not match")
	}

	if !evaluator.matchPrincipals([]string{"user*"}, "user123") {
		t.Error("Wildcard should match prefix")
	}
}

func TestPolicyEvaluatorMatchActions(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if !evaluator.matchActions([]string{"*"}, "s3:GetObject") {
		t.Error("* should match any action")
	}

	if !evaluator.matchActions([]string{"s3:GetObject"}, "s3:GetObject") {
		t.Error("Exact match should work")
	}

	if evaluator.matchActions([]string{"s3:GetObject"}, "s3:PutObject") {
		t.Error("Non-matching action should not match")
	}

	if !evaluator.matchActions([]string{"s3:*"}, "s3:GetObject") {
		t.Error("Wildcard should match prefix")
	}
}

func TestPolicyEvaluatorMatchResources(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if !evaluator.matchResources([]string{"*"}, "bucket/key") {
		t.Error("* should match any resource")
	}

	if !evaluator.matchResources([]string{"bucket/key"}, "bucket/key") {
		t.Error("Exact match should work")
	}

	if evaluator.matchResources([]string{"bucket1/*"}, "bucket2/key") {
		t.Error("Non-matching resource should not match")
	}

	if !evaluator.matchResources([]string{"bucket/*"}, "bucket/key") {
		t.Error("Wildcard should match prefix")
	}

	if !evaluator.matchResources([]string{"arn:aws:s3:::bucket"}, "arn:aws:s3:::bucket") {
		t.Error("ARN match should work")
	}
}

func TestParsePolicy(t *testing.T) {
	jsonData := `{
  "Id": "policy1",
  "Name": "TestPolicy",
  "Version": "2012-10-17",
  "Statement": [
   {
    "Effect": "Allow",
    "Action": ["s3:GetObject"],
    "Resource": ["*"]
   }
  ]
 }`

	policy, err := ParsePolicy([]byte(jsonData))
	if err != nil {
		t.Fatalf("ParsePolicy failed: %v", err)
	}

	if policy.ID != "policy1" {
		t.Errorf("ID = %v, want policy1", policy.ID)
	}
	if policy.Name != "TestPolicy" {
		t.Errorf("Name = %v, want TestPolicy", policy.Name)
	}
	if len(policy.Statements) != 1 {
		t.Errorf("Statements count = %d, want 1", len(policy.Statements))
	}
}

func TestParsePolicyInvalid(t *testing.T) {
	_, err := ParsePolicy([]byte("invalid json"))
	if err == nil {
		t.Error("ParsePolicy should fail for invalid JSON")
	}
}

func TestParsePolicyDefaults(t *testing.T) {
	jsonData := `{"Id": "policy1", "Name": "TestPolicy"}`

	policy, err := ParsePolicy([]byte(jsonData))
	if err != nil {
		t.Fatalf("ParsePolicy failed: %v", err)
	}

	if policy.Version != "2012-10-17" {
		t.Errorf("Version = %v, want 2012-10-17", policy.Version)
	}
	if policy.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
	if policy.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should be set")
	}
}

func TestIAMPolicyToJSON(t *testing.T) {
	policy := &IAMPolicy{
		ID:        "policy1",
		Name:      "TestPolicy",
		Version:   "2012-10-17",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	data, err := policy.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("JSON should not be empty")
	}
}

func TestNewACL(t *testing.T) {
	acl := NewACL("owner1", "Owner Name")

	if acl == nil {
		t.Fatal("ACL should not be nil")
	}
	if acl.Owner.ID != "owner1" {
		t.Errorf("Owner.ID = %v, want owner1", acl.Owner.ID)
	}
	if acl.Owner.DisplayName != "Owner Name" {
		t.Errorf("Owner.DisplayName = %v, want Owner Name", acl.Owner.DisplayName)
	}
	if acl.Grants == nil {
		t.Error("Grants should be initialized")
	}
}

func TestACLAddGrant(t *testing.T) {
	acl := NewACL("owner1", "Owner Name")

	grantee := Grantee{
		Type:        "CanonicalUser",
		ID:          "user1",
		DisplayName: "User One",
	}

	acl.AddGrant(grantee, PermissionRead)

	if len(acl.Grants) != 1 {
		t.Errorf("Grants count = %d, want 1", len(acl.Grants))
	}
}

func TestACLToXML(t *testing.T) {
	acl := NewACL("owner1", "Owner Name")
	acl.AddGrant(Grantee{
		Type:        "CanonicalUser",
		ID:          "user1",
		DisplayName: "User One",
	}, PermissionRead)

	xml := acl.ToXML()

	if xml == "" {
		t.Error("XML should not be empty")
	}
}

func TestACLToXMLWithURI(t *testing.T) {
	acl := NewACL("owner1", "Owner Name")
	acl.AddGrant(Grantee{
		Type: "Group",
		URI:  AllUsersGroup,
	}, PermissionRead)

	xml := acl.ToXML()

	if xml == "" {
		t.Error("XML should not be empty")
	}
}

func TestPermissionConstants(t *testing.T) {
	if PermissionRead != "READ" {
		t.Errorf("PermissionRead = %v, want READ", PermissionRead)
	}
	if PermissionWrite != "WRITE" {
		t.Errorf("PermissionWrite = %v, want WRITE", PermissionWrite)
	}
	if PermissionReadACP != "READ_ACP" {
		t.Errorf("PermissionReadACP = %v, want READ_ACP", PermissionReadACP)
	}
	if PermissionWriteACP != "WRITE_ACP" {
		t.Errorf("PermissionWriteACP = %v, want WRITE_ACP", PermissionWriteACP)
	}
	if PermissionFullControl != "FULL_CONTROL" {
		t.Errorf("PermissionFullControl = %v, want FULL_CONTROL", PermissionFullControl)
	}
}

func TestS3ActionToIAMActionMap(t *testing.T) {
	if S3ActionToIAMAction["s3:GetObject"] != "s3:GetObject" {
		t.Error("s3:GetObject mapping incorrect")
	}
	if S3ActionToIAMAction["s3:PutObject"] != "s3:PutObject" {
		t.Error("s3:PutObject mapping incorrect")
	}
}

func TestIAMPolicyStruct(t *testing.T) {
	policy := &IAMPolicy{
		ID:          "policy1",
		Name:        "TestPolicy",
		Description: "Test Description",
		Version:     "2012-10-17",
		Statements: []IAMStatement{
			{Effect: "Allow"},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if policy.ID != "policy1" {
		t.Errorf("ID = %v, want policy1", policy.ID)
	}
}

func TestIAMStatementStruct(t *testing.T) {
	stmt := IAMStatement{
		Sid:       "Stmt1",
		Effect:    "Allow",
		Actions:   []string{"s3:GetObject"},
		Resources: []string{"*"},
		Principal: &IAMPrincipal{AWS: []string{"*"}},
		Condition: &IAMCondition{
			StringEquals: map[string]string{"key": "value"},
		},
	}

	if stmt.Effect != "Allow" {
		t.Errorf("Effect = %v, want Allow", stmt.Effect)
	}
}

func TestIAMPrincipalStruct(t *testing.T) {
	p := IAMPrincipal{
		AWS:     []string{"arn:aws:iam::123:user/test"},
		Service: []string{"s3.amazonaws.com"},
	}

	if len(p.AWS) != 1 {
		t.Errorf("AWS count = %d, want 1", len(p.AWS))
	}
}

func TestIAMConditionStruct(t *testing.T) {
	cond := IAMCondition{
		StringEquals: map[string]string{"aws:userid": "user1"},
		StringLike:   map[string]string{"s3:prefix": "home/*"},
		IpAddress:    map[string]string{"aws:SourceIp": "192.168.1.0/24"},
		Numeric:      map[string]int{"s3:max-keys": 1000},
		Bool:         map[string]bool{"aws:SecureTransport": true},
		Null:         map[string]bool{"s3:x-amz-acl": false},
	}

	if cond.StringEquals["aws:userid"] != "user1" {
		t.Error("StringEquals not set correctly")
	}
}

func TestIAMUserStruct(t *testing.T) {
	user := IAMUser{
		ID:          "user1",
		AccessKey:   "AKIA123",
		DisplayName: "Test User",
		PolicyArns:  []string{"arn:policy:1"},
		Permissions: []string{"s3:GetObject"},
		CreatedAt:   time.Now(),
		LastUsed:    time.Now(),
	}

	if user.ID != "user1" {
		t.Errorf("ID = %v, want user1", user.ID)
	}
}

func TestBucketPermissionStruct(t *testing.T) {
	bp := BucketPermission{
		Bucket:     "bucket1",
		Prefix:     "prefix/",
		Grantee:    "user1",
		Permission: "READ",
	}

	if bp.Bucket != "bucket1" {
		t.Errorf("Bucket = %v, want bucket1", bp.Bucket)
	}
}

func TestOwnerStruct(t *testing.T) {
	owner := Owner{
		ID:          "owner1",
		DisplayName: "Owner",
	}

	if owner.ID != "owner1" {
		t.Errorf("ID = %v, want owner1", owner.ID)
	}
}

func TestGrantStruct(t *testing.T) {
	grant := Grant{
		Grantee: Grantee{
			Type: "CanonicalUser",
			ID:   "user1",
		},
		Permission: "READ",
	}

	if grant.Permission != "READ" {
		t.Errorf("Permission = %v, want READ", grant.Permission)
	}
}

func TestGranteeStruct(t *testing.T) {
	grantee := Grantee{
		Type:         "CanonicalUser",
		ID:           "user1",
		DisplayName:  "User One",
		EmailAddress: "user@example.com",
		URI:          "http://example.com",
	}

	if grantee.Type != "CanonicalUser" {
		t.Errorf("Type = %v, want CanonicalUser", grantee.Type)
	}
}

func TestACLConstants(t *testing.T) {
	if AllUsersGroup != "http://acs.amazonaws.com/groups/global/AuthenticatedUsers" {
		t.Errorf("AllUsersGroup = %v", AllUsersGroup)
	}
	if AuthenticatedGroup != "http://acs.amazonaws.com/groups/global/AllUsers" {
		t.Errorf("AuthenticatedGroup = %v", AuthenticatedGroup)
	}
	if LogDeliveryGroup != "http://acs.amazonaws.com/groups/global/LogDelivery" {
		t.Errorf("LogDeliveryGroup = %v", LogDeliveryGroup)
	}
}

func TestPolicyEvaluatorEvaluateStatementNilPrincipal(t *testing.T) {
	evaluator := NewPolicyEvaluator()
	policy := &IAMPolicy{
		ID:   "policy1",
		Name: "TestPolicy",
		Statements: []IAMStatement{
			{
				Effect:    "Allow",
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"*"},
			},
		},
	}

	evaluator.AddPolicy(policy)

	allowed := evaluator.Evaluate("anyone", "s3:GetObject", "*")
	if !allowed {
		t.Error("Action should be allowed with nil principal")
	}
}

func TestPolicyEvaluatorMatchResourcesARN(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if !evaluator.matchResources([]string{"arn:aws:s3:::mybucket/*"}, "arn:aws:s3:::mybucket/key") {
		t.Error("ARN resource should match")
	}

	if !evaluator.matchResources([]string{"arn:aws:s3:::bucket"}, "arn:aws:s3:::bucket") {
		t.Error("Exact ARN match should work")
	}

	if evaluator.matchResources([]string{"arn:aws:s3:::bucket1"}, "arn:aws:s3:::bucket2") {
		t.Error("Different ARNs should not match")
	}
}

func TestPolicyEvaluatorMatchActionsWildcardPrefix(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if !evaluator.matchActions([]string{"s3:Get*"}, "s3:GetObject") {
		t.Error("Wildcard prefix should match")
	}

	if !evaluator.matchActions([]string{"s3:*"}, "s3:DeleteObject") {
		t.Error("s3:* should match any s3 action")
	}
}

func TestPolicyEvaluatorMatchPrincipalsWildcardPrefix(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if !evaluator.matchPrincipals([]string{"arn:aws:iam::*"}, "arn:aws:iam::123456789012:user/test") {
		t.Error("Wildcard principal prefix should match")
	}
}

func TestPolicyEvaluatorMatchResourcesShortResource(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if evaluator.matchResources([]string{"verylongprefix*"}, "short") {
		t.Error("Resource shorter than prefix should not match")
	}
}

func TestPolicyEvaluatorMatchActionsShortAction(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if evaluator.matchActions([]string{"verylongaction*"}, "short") {
		t.Error("Action shorter than prefix should not match")
	}
}

func TestPolicyEvaluatorMatchPrincipalsShortPrincipal(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if evaluator.matchPrincipals([]string{"verylongprincipal*"}, "short") {
		t.Error("Principal shorter than prefix should not match")
	}
}

func TestPolicyEvaluatorEvaluateStatementActionMismatch(t *testing.T) {
	evaluator := NewPolicyEvaluator()
	policy := &IAMPolicy{
		ID:   "policy1",
		Name: "TestPolicy",
		Statements: []IAMStatement{
			{
				Effect:    "Allow",
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"*"},
				Principal: &IAMPrincipal{AWS: []string{"*"}},
			},
		},
	}

	evaluator.AddPolicy(policy)

	allowed := evaluator.Evaluate("*", "s3:DeleteObject", "*")
	if allowed {
		t.Error("Action mismatch should deny access")
	}
}

func TestPolicyEvaluatorEvaluateStatementResourceMismatch(t *testing.T) {
	evaluator := NewPolicyEvaluator()
	policy := &IAMPolicy{
		ID:   "policy1",
		Name: "TestPolicy",
		Statements: []IAMStatement{
			{
				Effect:    "Allow",
				Actions:   []string{"*"},
				Resources: []string{"bucket1/*"},
				Principal: &IAMPrincipal{AWS: []string{"*"}},
			},
		},
	}

	evaluator.AddPolicy(policy)

	allowed := evaluator.Evaluate("*", "s3:GetObject", "bucket2/key")
	if allowed {
		t.Error("Resource mismatch should deny access")
	}
}

func TestPolicyEvaluatorMatchResourcesARNNonMatching(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if evaluator.matchResources([]string{"arn:aws:s3:::bucket1/*"}, "arn:aws:s3:::bucket2/key") {
		t.Error("Non-matching ARN should not match")
	}
}

func TestPolicyEvaluatorMatchResourcesResourceShorterThanPrefix(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if evaluator.matchResources([]string{"verylongprefix*"}, "x") {
		t.Error("Resource shorter than prefix should not match wildcard")
	}
}

func TestPolicyEvaluatorMatchResourcesARNNotMatching(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if evaluator.matchResources([]string{"arn:aws:s3:::bucket1/key"}, "arn:aws:s3:::bucket2/key") {
		t.Error("Different ARN resources should not match")
	}
}

func TestPolicyEvaluatorMatchResourcesARNExactMatch(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if !evaluator.matchResources([]string{"arn:aws:s3:::mybucket/mykey"}, "arn:aws:s3:::mybucket/mykey") {
		t.Error("Exact ARN match should work")
	}
}

func TestPolicyEvaluatorMatchResourcesWildcardPrefixMismatch(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if evaluator.matchResources([]string{"bucket1/*"}, "bucket2/key") {
		t.Error("Wildcard with mismatched prefix should not match")
	}
}

func TestPolicyEvaluatorMatchResourcesARNWithNonARNResource(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if evaluator.matchResources([]string{"bucket/key"}, "arn:aws:s3:::bucket/key") {
		t.Error("Non-ARN pattern should not match ARN resource via ARN check")
	}
}

func TestPolicyEvaluatorMatchResourcesARNNotEqual(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if evaluator.matchResources([]string{"arn:aws:s3:::bucket1/key"}, "arn:aws:s3:::bucket2/key") {
		t.Error("Different ARN resources should not match via ARN equality check")
	}
}

func TestPolicyEvaluatorMatchResourcesARNWithNonWildcardPattern(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if !evaluator.matchResources([]string{"arn:aws:s3:::bucket/key"}, "arn:aws:s3:::bucket/key") {
		t.Error("Exact ARN pattern should match ARN resource")
	}
}

func TestPolicyEvaluatorMatchResourcesARNBlockNoMatch(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if evaluator.matchResources([]string{"arn:aws:s3:::bucket1/key"}, "arn:aws:s3:::bucket2/key") {
		t.Error("Different ARN resources should not match")
	}
}

func TestPolicyEvaluatorMatchResourcesARNBlockEntered(t *testing.T) {
	evaluator := NewPolicyEvaluator()

	if evaluator.matchResources([]string{"bucket1"}, "arn:aws:s3:::bucket2") {
		t.Error("Non-matching pattern should not match ARN resource")
	}
}
