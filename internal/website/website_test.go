package website

import (
	"errors"
	"strings"
	"testing"
)

func TestNewConfig(t *testing.T) {
	cfg := NewConfig("index.html")
	if cfg == nil {
		t.Fatal("Config should not be nil")
	}
	if cfg.IndexDocument == nil {
		t.Fatal("IndexDocument should not be nil")
	}
	if cfg.IndexDocument.Suffix != "index.html" {
		t.Errorf("Suffix = %v, want index.html", cfg.IndexDocument.Suffix)
	}
}

func TestConfigWithErrorDocument(t *testing.T) {
	cfg := NewConfig("index.html").WithErrorDocument("error.html")

	if cfg.ErrorDocument == nil {
		t.Fatal("ErrorDocument should not be nil")
	}
	if cfg.ErrorDocument.Key != "error.html" {
		t.Errorf("Key = %v, want error.html", cfg.ErrorDocument.Key)
	}
}

func TestConfigWithRoutingRules(t *testing.T) {
	rules := []RoutingRule{
		{Condition: &Condition{KeyPrefixEquals: "docs/"}},
	}
	cfg := NewConfig("index.html").WithRoutingRules(rules)

	if len(cfg.RoutingRules) != 1 {
		t.Errorf("len(RoutingRules) = %d, want 1", len(cfg.RoutingRules))
	}
}

func TestConfigToXML(t *testing.T) {
	cfg := NewConfig("index.html")

	xml, err := cfg.ToXML()
	if err != nil {
		t.Fatalf("ToXML failed: %v", err)
	}

	if !strings.Contains(xml, "<?xml") {
		t.Error("XML should contain XML header")
	}
	if !strings.Contains(xml, "WebsiteConfiguration") {
		t.Error("XML should contain WebsiteConfiguration")
	}
	if !strings.Contains(xml, "index.html") {
		t.Error("XML should contain index.html")
	}
}

func TestConfigFromXML(t *testing.T) {
	xmlData := `<?xml version="1.0" encoding="UTF-8"?>
<WebsiteConfiguration>
  <IndexDocument>
    <Suffix>index.html</Suffix>
  </IndexDocument>
</WebsiteConfiguration>`

	cfg, err := FromXML([]byte(xmlData))
	if err != nil {
		t.Fatalf("FromXML failed: %v", err)
	}

	if cfg.IndexDocument.Suffix != "index.html" {
		t.Errorf("Suffix = %v, want index.html", cfg.IndexDocument.Suffix)
	}
}

func TestConfigFromXMLInvalid(t *testing.T) {
	_, err := FromXML([]byte("invalid xml"))
	if err == nil {
		t.Error("FromXML should fail for invalid XML")
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := NewConfig("index.html")
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate should pass for valid config: %v", err)
	}
}

func TestConfigValidateNoIndexDocument(t *testing.T) {
	cfg := &Config{}
	if err := cfg.Validate(); err == nil {
		t.Error("Validate should fail without IndexDocument")
	}
}

func TestConfigValidateEmptySuffix(t *testing.T) {
	cfg := &Config{IndexDocument: &IndexDocument{Suffix: ""}}
	if err := cfg.Validate(); err == nil {
		t.Error("Validate should fail with empty Suffix")
	}
}

func TestNewRoutingRuleBuilder(t *testing.T) {
	builder := NewRoutingRuleBuilder()
	if builder == nil {
		t.Fatal("Builder should not be nil")
	}
}

func TestRoutingRuleBuilderWithKeyPrefix(t *testing.T) {
	rule := NewRoutingRuleBuilder().
		WithKeyPrefix("docs/").
		Build()

	if rule.Condition.KeyPrefixEquals != "docs/" {
		t.Errorf("KeyPrefixEquals = %v, want docs/", rule.Condition.KeyPrefixEquals)
	}
}

func TestRoutingRuleBuilderWithErrorCode(t *testing.T) {
	rule := NewRoutingRuleBuilder().
		WithErrorCode("404").
		Build()

	if rule.Condition.HttpErrorCodeReturnedEquals != "404" {
		t.Errorf("HttpErrorCodeReturnedEquals = %v, want 404", rule.Condition.HttpErrorCodeReturnedEquals)
	}
}

func TestRoutingRuleBuilderToProtocol(t *testing.T) {
	rule := NewRoutingRuleBuilder().
		ToProtocol("https").
		Build()

	if rule.Redirect.Protocol != "https" {
		t.Errorf("Protocol = %v, want https", rule.Redirect.Protocol)
	}
}

func TestRoutingRuleBuilderToHost(t *testing.T) {
	rule := NewRoutingRuleBuilder().
		ToHost("example.com").
		Build()

	if rule.Redirect.HostName != "example.com" {
		t.Errorf("HostName = %v, want example.com", rule.Redirect.HostName)
	}
}

func TestRoutingRuleBuilderWithPrefix(t *testing.T) {
	rule := NewRoutingRuleBuilder().
		WithPrefix("new/").
		Build()

	if rule.Redirect.ReplaceKeyPrefixWith != "new/" {
		t.Errorf("ReplaceKeyPrefixWith = %v, want new/", rule.Redirect.ReplaceKeyPrefixWith)
	}
}

func TestRoutingRuleBuilderWithKey(t *testing.T) {
	rule := NewRoutingRuleBuilder().
		WithKey("error.html").
		Build()

	if rule.Redirect.ReplaceKeyWith != "error.html" {
		t.Errorf("ReplaceKeyWith = %v, want error.html", rule.Redirect.ReplaceKeyWith)
	}
}

func TestRoutingRuleBuilderWithCode(t *testing.T) {
	rule := NewRoutingRuleBuilder().
		WithCode("301").
		Build()

	if rule.Redirect.HttpRedirectCode != "301" {
		t.Errorf("HttpRedirectCode = %v, want 301", rule.Redirect.HttpRedirectCode)
	}
}

func TestRoutingRuleBuilderChained(t *testing.T) {
	rule := NewRoutingRuleBuilder().
		WithKeyPrefix("old/").
		WithPrefix("new/").
		ToHost("example.com").
		ToProtocol("https").
		WithCode("301").
		Build()

	if rule.Condition.KeyPrefixEquals != "old/" {
		t.Errorf("KeyPrefixEquals = %v, want old/", rule.Condition.KeyPrefixEquals)
	}
	if rule.Redirect.ReplaceKeyPrefixWith != "new/" {
		t.Errorf("ReplaceKeyPrefixWith = %v, want new/", rule.Redirect.ReplaceKeyPrefixWith)
	}
	if rule.Redirect.HostName != "example.com" {
		t.Errorf("HostName = %v, want example.com", rule.Redirect.HostName)
	}
	if rule.Redirect.Protocol != "https" {
		t.Errorf("Protocol = %v, want https", rule.Redirect.Protocol)
	}
	if rule.Redirect.HttpRedirectCode != "301" {
		t.Errorf("HttpRedirectCode = %v, want 301", rule.Redirect.HttpRedirectCode)
	}
}

func TestNewStaticWebsiteBuilder(t *testing.T) {
	builder := NewStaticWebsiteBuilder()
	if builder == nil {
		t.Fatal("Builder should not be nil")
	}
}

func TestStaticWebsiteBuilderWithIndexSuffix(t *testing.T) {
	cfg := NewStaticWebsiteBuilder().
		WithIndexSuffix("default.html").
		Build()

	if cfg.IndexDocument.Suffix != "default.html" {
		t.Errorf("Suffix = %v, want default.html", cfg.IndexDocument.Suffix)
	}
}

func TestStaticWebsiteBuilderWith404Page(t *testing.T) {
	cfg := NewStaticWebsiteBuilder().
		With404Page("404.html").
		Build()

	if cfg.ErrorDocument.Key != "404.html" {
		t.Errorf("Key = %v, want 404.html", cfg.ErrorDocument.Key)
	}
}

func TestStaticWebsiteBuilderWithRedirectToHTTPS(t *testing.T) {
	cfg := NewStaticWebsiteBuilder().
		WithRedirectToHTTPS().
		Build()

	if len(cfg.RoutingRules) != 1 {
		t.Errorf("len(RoutingRules) = %d, want 1", len(cfg.RoutingRules))
	}
	if cfg.RoutingRules[0].Redirect.Protocol != "https" {
		t.Errorf("Protocol = %v, want https", cfg.RoutingRules[0].Redirect.Protocol)
	}
}

func TestStaticWebsiteBuilderWithWWWRedirect(t *testing.T) {
	cfg := NewStaticWebsiteBuilder().
		WithWWWRedirect().
		Build()

	if len(cfg.RoutingRules) != 1 {
		t.Errorf("len(RoutingRules) = %d, want 1", len(cfg.RoutingRules))
	}
	if cfg.RoutingRules[0].Redirect.HostName != "www.example.com" {
		t.Errorf("HostName = %v, want www.example.com", cfg.RoutingRules[0].Redirect.HostName)
	}
	if cfg.RoutingRules[0].Redirect.HttpRedirectCode != "301" {
		t.Errorf("HttpRedirectCode = %v, want 301", cfg.RoutingRules[0].Redirect.HttpRedirectCode)
	}
}

func TestStaticWebsiteBuilderChained(t *testing.T) {
	cfg := NewStaticWebsiteBuilder().
		WithIndexSuffix("index.htm").
		With404Page("error.html").
		WithRedirectToHTTPS().
		WithWWWRedirect().
		Build()

	if cfg.IndexDocument.Suffix != "index.htm" {
		t.Errorf("Suffix = %v, want index.htm", cfg.IndexDocument.Suffix)
	}
	if cfg.ErrorDocument.Key != "error.html" {
		t.Errorf("Key = %v, want error.html", cfg.ErrorDocument.Key)
	}
	if len(cfg.RoutingRules) != 2 {
		t.Errorf("len(RoutingRules) = %d, want 2", len(cfg.RoutingRules))
	}
}

func TestConfigToXMLWithAllFields(t *testing.T) {
	cfg := NewConfig("index.html").
		WithErrorDocument("error.html").
		WithRoutingRules([]RoutingRule{
			{
				Condition: &Condition{KeyPrefixEquals: "docs/"},
				Redirect:  &Redirect{HostName: "docs.example.com"},
			},
		})

	xml, err := cfg.ToXML()
	if err != nil {
		t.Fatalf("ToXML failed: %v", err)
	}

	if !strings.Contains(xml, "error.html") {
		t.Error("XML should contain error.html")
	}
	if !strings.Contains(xml, "docs.example.com") {
		t.Error("XML should contain docs.example.com")
	}
}

func TestConfigToXMLError(t *testing.T) {
	original := xmlMarshalIndent
	defer func() { xmlMarshalIndent = original }()

	xmlMarshalIndent = func(any, string, string) ([]byte, error) {
		return nil, errors.New("marshal error")
	}

	cfg := NewConfig("index.html")
	_, err := cfg.ToXML()
	if err == nil {
		t.Error("ToXML should return error when marshal fails")
	}
	if err.Error() != "marshal error" {
		t.Errorf("error = %v, want 'marshal error'", err)
	}
}

func TestIndexDocument(t *testing.T) {
	idx := IndexDocument{Suffix: "index.php"}
	if idx.Suffix != "index.php" {
		t.Errorf("Suffix = %v, want index.php", idx.Suffix)
	}
}

func TestErrorDocument(t *testing.T) {
	err := ErrorDocument{Key: "404.html"}
	if err.Key != "404.html" {
		t.Errorf("Key = %v, want 404.html", err.Key)
	}
}

func TestCondition(t *testing.T) {
	cond := Condition{
		KeyPrefixEquals:             "api/",
		HttpErrorCodeReturnedEquals: "404",
	}

	if cond.KeyPrefixEquals != "api/" {
		t.Errorf("KeyPrefixEquals = %v, want api/", cond.KeyPrefixEquals)
	}
	if cond.HttpErrorCodeReturnedEquals != "404" {
		t.Errorf("HttpErrorCodeReturnedEquals = %v, want 404", cond.HttpErrorCodeReturnedEquals)
	}
}

func TestRedirect(t *testing.T) {
	redirect := Redirect{
		Protocol:             "https",
		HostName:             "example.com",
		ReplaceKeyPrefixWith: "new/",
		ReplaceKeyWith:       "index.html",
		HttpRedirectCode:     "301",
	}

	if redirect.Protocol != "https" {
		t.Errorf("Protocol = %v, want https", redirect.Protocol)
	}
	if redirect.HostName != "example.com" {
		t.Errorf("HostName = %v, want example.com", redirect.HostName)
	}
}

func TestRoutingRule(t *testing.T) {
	rule := RoutingRule{
		Condition: &Condition{KeyPrefixEquals: "old/"},
		Redirect:  &Redirect{ReplaceKeyPrefixWith: "new/"},
	}

	if rule.Condition.KeyPrefixEquals != "old/" {
		t.Errorf("KeyPrefixEquals = %v, want old/", rule.Condition.KeyPrefixEquals)
	}
}
