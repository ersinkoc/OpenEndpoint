package website

import (
	"encoding/xml"
	"fmt"
)

// Config represents bucket website configuration
type Config struct {
	XMLName         xml.Name       `xml:"WebsiteConfiguration"`
	IndexDocument   *IndexDocument `xml:"IndexDocument"`
	ErrorDocument   *ErrorDocument `xml:"ErrorDocument"`
	RoutingRules    []RoutingRule `xml:"RoutingRules>RoutingRule"`
}

// IndexDocument specifies the default index page
type IndexDocument struct {
	Suffix string `xml:"Suffix"`
}

// ErrorDocument specifies the error page
type ErrorDocument struct {
	Key string `xml:"Key"`
}

// RoutingRule represents a single routing rule
type RoutingRule struct {
	Condition *Condition `xml:"Condition"`
	Redirect  *Redirect  `xml:"Redirect"`
}

// Condition specifies when a routing rule is applied
type Condition struct {
	KeyPrefixEquals      string `xml:"KeyPrefixEquals"`
	HttpErrorCodeReturnedEquals string `xml:"HttpErrorCodeReturnedEquals"`
}

// Redirect specifies how to redirect
type Redirect struct {
	Protocol           string `xml:"Protocol,omitempty"`
	HostName           string `xml:"HostName,omitempty"`
	ReplaceKeyPrefixWith string `xml:"ReplaceKeyPrefixWith,omitempty"`
	ReplaceKeyWith     string `xml:"ReplaceKeyWith,omitempty"`
	HttpRedirectCode  string `xml:"HttpRedirectCode,omitempty"`
}

// ToXML converts config to XML
func (c *Config) ToXML() (string, error) {
	data, err := xml.MarshalIndent(c, "", "  ")
	if err != nil {
		return "", err
	}

	header := `<?xml version="1.0" encoding="UTF-8"?>`
	return header + "\n" + string(data), nil
}

// FromXML parses config from XML
func FromXML(data []byte) (*Config, error) {
	var cfg Config
	if err := xml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse website config: %w", err)
	}
	return &cfg, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.IndexDocument == nil {
		return fmt.Errorf("IndexDocument is required")
	}

	if c.IndexDocument.Suffix == "" {
		return fmt.Errorf("IndexDocument Suffix is required")
	}

	return nil
}

// NewConfig creates a new website configuration
func NewConfig(indexSuffix string) *Config {
	return &Config{
		IndexDocument: &IndexDocument{
			Suffix: indexSuffix,
		},
	}
}

// WithErrorDocument sets the error document
func (c *Config) WithErrorDocument(key string) *Config {
	c.ErrorDocument = &ErrorDocument{
		Key: key,
	}
	return c
}

// WithRoutingRules adds routing rules
func (c *Config) WithRoutingRules(rules []RoutingRule) *Config {
	c.RoutingRules = rules
	return c
}

// RoutingRuleBuilder helps build routing rules
type RoutingRuleBuilder struct {
	rule RoutingRule
}

// NewRoutingRuleBuilder creates a new builder
func NewRoutingRuleBuilder() *RoutingRuleBuilder {
	return &RoutingRuleBuilder{
		rule: RoutingRule{
			Condition: &Condition{},
			Redirect:  &Redirect{},
		},
	}
}

// WithKeyPrefix sets the key prefix condition
func (b *RoutingRuleBuilder) WithKeyPrefix(prefix string) *RoutingRuleBuilder {
	b.rule.Condition.KeyPrefixEquals = prefix
	return b
}

// WithErrorCode sets the HTTP error code condition
func (b *RoutingRuleBuilder) WithErrorCode(code string) *RoutingRuleBuilder {
	b.rule.Condition.HttpErrorCodeReturnedEquals = code
	return b
}

// ToProtocol redirects to protocol
func (b *RoutingRuleBuilder) ToProtocol(protocol string) *RoutingRuleBuilder {
	b.rule.Redirect.Protocol = protocol
	return b
}

// ToHost redirects to host
func (b *RoutingRuleBuilder) ToHost(host string) *RoutingRuleBuilder {
	b.rule.Redirect.HostName = host
	return b
}

// WithPrefix replaces key prefix
func (b *RoutingRuleBuilder) WithPrefix(prefix string) *RoutingRuleBuilder {
	b.rule.Redirect.ReplaceKeyPrefixWith = prefix
	return b
}

// WithKey replaces key
func (b *RoutingRuleBuilder) WithKey(key string) *RoutingRuleBuilder {
	b.rule.Redirect.ReplaceKeyWith = key
	return b
}

// WithCode sets HTTP redirect code
func (b *RoutingRuleBuilder) WithCode(code string) *RoutingRuleBuilder {
	b.rule.Redirect.HttpRedirectCode = code
	return b
}

// Build builds the routing rule
func (b *RoutingRuleBuilder) Build() RoutingRule {
	return b.rule
}

// StaticWebsiteBuilder helps build static website configurations
type StaticWebsiteBuilder struct {
	config *Config
}

// NewStaticWebsiteBuilder creates a static website builder
func NewStaticWebsiteBuilder() *StaticWebsiteBuilder {
	return &StaticWebsiteBuilder{
		config: NewConfig("index.html"),
	}
}

// WithIndexSuffix sets the index document suffix
func (b *StaticWebsiteBuilder) WithIndexSuffix(suffix string) *StaticWebsiteBuilder {
	b.config.IndexDocument.Suffix = suffix
	return b
}

// With404Page sets the 404 error page
func (b *StaticWebsiteBuilder) With404Page(key string) *StaticWebsiteBuilder {
	if b.config.ErrorDocument == nil {
		b.config.ErrorDocument = &ErrorDocument{}
	}
	b.config.ErrorDocument.Key = key
	return b
}

// WithRedirectToHTTPS redirects all requests to HTTPS
func (b *StaticWebsiteBuilder) WithRedirectToHTTPS() *StaticWebsiteBuilder {
	rule := RoutingRule{
		Condition: &Condition{},
		Redirect: &Redirect{
			Protocol: "https",
		},
	}
	b.config.RoutingRules = append(b.config.RoutingRules, rule)
	return b
}

// WithWWWRedirect redirects naked domain to www
func (b *StaticWebsiteBuilder) WithWWWRedirect() *StaticWebsiteBuilder {
	rule := RoutingRule{
		Condition: &Condition{
			KeyPrefixEquals: "",
		},
		Redirect: &Redirect{
			HostName:           "www.example.com",
			HttpRedirectCode:   "301",
			Protocol:           "https",
		},
	}
	b.config.RoutingRules = append(b.config.RoutingRules, rule)
	return b
}

// Build builds the configuration
func (b *StaticWebsiteBuilder) Build() *Config {
	return b.config
}
