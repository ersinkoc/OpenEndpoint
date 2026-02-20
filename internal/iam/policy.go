package iam

import (
	"encoding/json"
	"fmt"
	"time"
)

// IAMPolicy represents an IAM policy
type IAMPolicy struct {
	ID          string         `json:"Id"`
	Name        string         `json:"Name"`
	Description string         `json:"Description"`
	Version     string         `json:"Version"`
	Statements  []IAMStatement `json:"Statement"`
	CreatedAt   time.Time     `json:"CreatedAt"`
	UpdatedAt   time.Time     `json:"UpdatedAt"`
}

// IAMStatement represents a policy statement
type IAMStatement struct {
	Sid          string          `json:"Sid,omitempty"`
	Effect       string          `json:"Effect"` // Allow or Deny
	Principal    *IAMPrincipal   `json:"Principal,omitempty"`
	NotPrincipal *IAMPrincipal   `json:"NotPrincipal,omitempty"`
	Actions      []string        `json:"Action"`
	NotActions   []string        `json:"NotAction,omitempty"`
	Resources    []string        `json:"Resource"`
	NotResources []string        `json:"NotResource,omitempty"`
	Condition    *IAMCondition  `json:"Condition,omitempty"`
}

// IAMPrincipal represents who the policy applies to
type IAMPrincipal struct {
	AWS     []string `json:"AWS,omitempty"`
	Service []string `json:"Service,omitempty"`
}

// IAMCondition represents a policy condition
type IAMCondition struct {
	StringEquals map[string]string `json:"StringEquals,omitempty"`
	StringLike   map[string]string `json:"StringLike,omitempty"`
	IpAddress    map[string]string `json:"IpAddress,omitempty"`
	Numeric      map[string]int    `json:"Numeric,omitempty"`
	Bool         map[string]bool  `json:"Bool,omitempty"`
	Null         map[string]bool  `json:"Null,omitempty"`
}

// IAMUser represents an IAM user
type IAMUser struct {
	ID           string    `json:"Id"`
	AccessKey    string    `json:"AccessKey"`
	DisplayName  string    `json:"DisplayName"`
	PolicyArns   []string  `json:"PolicyArns"`
	Permissions  []string  `json:"Permissions"`
	CreatedAt    time.Time `json:"CreatedAt"`
	LastUsed     time.Time `json:"LastUsed"`
}

// PolicyEvaluator evaluates policies
type PolicyEvaluator struct {
	policies map[string]*IAMPolicy
}

// NewPolicyEvaluator creates a new policy evaluator
func NewPolicyEvaluator() *PolicyEvaluator {
	return &PolicyEvaluator{
		policies: make(map[string]*IAMPolicy),
	}
}

// AddPolicy adds a policy
func (e *PolicyEvaluator) AddPolicy(policy *IAMPolicy) {
	e.policies[policy.ID] = policy
}

// RemovePolicy removes a policy
func (e *PolicyEvaluator) RemovePolicy(policyID string) {
	delete(e.policies, policyID)
}

// Evaluate evaluates if an action is allowed
func (e *PolicyEvaluator) Evaluate(principal string, action, resource string) bool {
	for _, policy := range e.policies {
		if e.evaluatePolicy(policy, principal, action, resource) {
			return true
		}
	}
	return false
}

// evaluatePolicy evaluates a single policy
func (e *PolicyEvaluator) evaluatePolicy(policy *IAMPolicy, principal, action, resource string) bool {
	for _, stmt := range policy.Statements {
		if e.evaluateStatement(&stmt, principal, action, resource) {
			return stmt.Effect == "Allow"
		}
	}
	return false
}

// evaluateStatement evaluates a single statement
func (e *PolicyEvaluator) evaluateStatement(stmt *IAMStatement, principal, action, resource string) bool {
	// Check principal
	if stmt.Principal != nil {
		if !e.matchPrincipals(stmt.Principal.AWS, principal) {
			return false
		}
	}

	// Check action
	if !e.matchActions(stmt.Actions, action) {
		return false
	}

	// Check resource
	if !e.matchResources(stmt.Resources, resource) {
		return false
	}

	return true
}

// matchPrincipals checks if principal matches
func (e *PolicyEvaluator) matchPrincipals(principals []string, principal string) bool {
	for _, p := range principals {
		if p == "*" || p == principal {
			return true
		}
		// Support wildcards
		if len(p) > 1 && p[len(p)-1] == '*' {
			prefix := p[:len(p)-1]
			if len(principal) >= len(prefix) && principal[:len(prefix)] == prefix {
				return true
			}
		}
	}
	return false
}

// matchActions checks if action matches
func (e *PolicyEvaluator) matchActions(actions []string, action string) bool {
	for _, a := range actions {
		if a == "*" || a == action {
			return true
		}
		// Support wildcards
		if len(a) > 1 && a[len(a)-1] == '*' {
			prefix := a[:len(a)-1]
			if len(action) >= len(prefix) && action[:len(prefix)] == prefix {
				return true
			}
		}
	}
	return false
}

// matchResources checks if resource matches
func (e *PolicyEvaluator) matchResources(resources []string, resource string) bool {
	for _, r := range resources {
		if r == "*" || r == resource {
			return true
		}
		// Support wildcards
		if len(r) > 1 && r[len(r)-1] == '*' {
			prefix := r[:len(r)-1]
			if len(resource) >= len(prefix) && resource[:len(prefix)] == prefix {
				return true
			}
		}
		// Support ARNs
		if len(resource) > 6 && resource[:6] == "arn:aws" {
			if r == resource {
				return true
			}
		}
	}
	return false
}

// S3ActionToIAMAction converts S3 action to IAM action
var S3ActionToIAMAction = map[string]string{
	"s3:GetObject":           "s3:GetObject",
	"s3:PutObject":           "s3:PutObject",
	"s3:DeleteObject":        "s3:DeleteObject",
	"s3:ListBucket":          "s3:ListBucket",
	"s3:CreateBucket":       "s3:CreateBucket",
	"s3:DeleteBucket":        "s3:DeleteBucket",
	"s3:GetBucketLocation":   "s3:GetBucketLocation",
	"s3:ListMultipartUploads": "s3:ListMultipartUploads",
}

// ParsePolicy parses a JSON policy
func ParsePolicy(data []byte) (*IAMPolicy, error) {
	var policy IAMPolicy
	if err := json.Unmarshal(data, &policy); err != nil {
		return nil, fmt.Errorf("failed to parse policy: %w", err)
	}

	// Set defaults
	if policy.Version == "" {
		policy.Version = "2012-10-17"
	}
	if policy.CreatedAt.IsZero() {
		policy.CreatedAt = time.Now()
	}
	policy.UpdatedAt = time.Now()

	return &policy, nil
}

// ToJSON converts policy to JSON
func (p *IAMPolicy) ToJSON() ([]byte, error) {
	return json.MarshalIndent(p, "", "  ")
}

// BucketPermission represents bucket permissions
type BucketPermission struct {
	Bucket    string
	Prefix    string
	Grantee   string
	Permission string
}

// ACL represents an access control list
type ACL struct {
	Owner       Owner
	Grants     []Grant
}

// Owner represents resource owner
type Owner struct {
	ID          string
	DisplayName string
}

// Grant represents a grant
type Grant struct {
	Grantee    Grantee
	Permission string
}

// Grantee represents who receives a grant
type Grantee struct {
	Type         string
	URI          string
	ID           string
	DisplayName  string
	EmailAddress string
}

// Common ACL permissions
const (
	PermissionRead        = "READ"
	PermissionWrite       = "WRITE"
	PermissionReadACP     = "READ_ACP"
	PermissionWriteACP    = "WRITE_ACP"
	PermissionFullControl = "FULL_CONTROL"
)

// Common ACL grantees
var (
	AllUsersGroup      = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
	AuthenticatedGroup = "http://acs.amazonaws.com/groups/global/AllUsers"
	LogDeliveryGroup   = "http://acs.amazonaws.com/groups/global/LogDelivery"
)

// NewACL creates a new ACL
func NewACL(ownerID, ownerName string) *ACL {
	return &ACL{
		Owner: Owner{
			ID:          ownerID,
			DisplayName: ownerName,
		},
		Grants: make([]Grant, 0),
	}
}

// AddGrant adds a grant to the ACL
func (a *ACL) AddGrant(grantee Grantee, permission string) {
	a.Grants = append(a.Grants, Grant{
		Grantee:    grantee,
		Permission: permission,
	})
}

// ToXML converts ACL to XML
func (a *ACL) ToXML() string {
	xml := `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`
	xml += fmt.Sprintf(`<Owner><ID>%s</ID><DisplayName>%s</DisplayName></Owner>`, a.Owner.ID, a.Owner.DisplayName)
	xml += `<AccessControlList>`

	for _, grant := range a.Grants {
		xml += `<Grant>`
		xml += fmt.Sprintf(`<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="%s">`, grant.Grantee.Type)
		if grant.Grantee.ID != "" {
			xml += fmt.Sprintf(`<ID>%s</ID>`, grant.Grantee.ID)
		}
		if grant.Grantee.DisplayName != "" {
			xml += fmt.Sprintf(`<DisplayName>%s</DisplayName>`, grant.Grantee.DisplayName)
		}
		if grant.Grantee.URI != "" {
			xml += fmt.Sprintf(`<URI>%s</URI>`, grant.Grantee.URI)
		}
		xml += `</Grantee>`
		xml += fmt.Sprintf(`<Permission>%s</Permission>`, grant.Permission)
		xml += `</Grant>`
	}

	xml += `</AccessControlList>`
	xml += `</AccessControlPolicy>`

	return xml
}
