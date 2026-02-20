package tags

import (
	"encoding/xml"
	"fmt"
)

// TagSet represents a set of tags
type TagSet []Tag

// Tag represents a tag
type Tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// ToMap converts tag set to map
func (ts TagSet) ToMap() map[string]string {
	m := make(map[string]string)
	for _, t := range ts {
		m[t.Key] = t.Value
	}
	return m
}

// FromMap creates tag set from map
func FromMap(m map[string]string) TagSet {
	ts := make(TagSet, 0, len(m))
	for k, v := range m {
		ts = append(ts, Tag{Key: k, Value: v})
	}
	return ts
}

// Get gets value by key
func (ts TagSet) Get(key string) (string, bool) {
	for _, t := range ts {
		if t.Key == key {
			return t.Value, true
		}
	}
	return "", false
}

// Set sets a tag
func (ts *TagSet) Set(key, value string) {
	for i, t := range *ts {
		if t.Key == key {
			(*ts)[i].Value = value
			return
		}
	}
	*ts = append(*ts, Tag{Key: key, Value: value})
}

// Delete deletes a tag
func (ts *TagSet) Delete(key string) {
	for i, t := range *ts {
		if t.Key == key {
			*ts = append((*ts)[:i], (*ts)[i+1:]...)
			return
		}
	}
}

// Tagging represents object tagging
type Tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	TagSet  TagSet  `xml:"TagSet>Tag"`
}

// ToXML converts tagging to XML
func (t *Tagging) ToXML() string {
	xml := `<?xml version="1.0" encoding="UTF-8"?>`
	xml += `<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`
	xml += `<TagSet>`

	for _, tag := range t.TagSet {
		xml += `<Tag>`
		xml += fmt.Sprintf(`<Key>%s</Key>`, EscapeXML(tag.Key))
		xml += fmt.Sprintf(`<Value>%s</Value>`, EscapeXML(tag.Value))
		xml += `</Tag>`
	}

	xml += `</TagSet>`
	xml += `</Tagging>`

	return xml
}

// FromXML parses tagging from XML
func FromXML(data []byte) (*Tagging, error) {
	var tagging Tagging
	if err := xml.Unmarshal(data, &tagging); err != nil {
		return nil, fmt.Errorf("failed to parse tagging: %w", err)
	}
	return &tagging, nil
}

// TagValidator validates tags
type TagValidator struct {
	maxTagsPerResource int
	maxKeyLength      int
	maxValueLength    int
}

// NewTagValidator creates a new tag validator
func NewTagValidator() *TagValidator {
	return &TagValidator{
		maxTagsPerResource: 10,
		maxKeyLength:      128,
		maxValueLength:   256,
	}
}

// Validate validates a tag set
func (v *TagValidator) Validate(tags TagSet) error {
	if len(tags) > v.maxTagsPerResource {
		return fmt.Errorf("too many tags: maximum %d allowed", v.maxTagsPerResource)
	}

	for _, tag := range tags {
		if len(tag.Key) == 0 {
			return fmt.Errorf("tag key cannot be empty")
		}
		if len(tag.Key) > v.maxKeyLength {
			return fmt.Errorf("tag key too long: maximum %d characters", v.maxKeyLength)
		}
		if len(tag.Value) > v.maxValueLength {
			return fmt.Errorf("tag value too long: maximum %d characters", v.maxValueLength)
		}
		// Check for invalid characters
		if containsInvalidTagChars(tag.Key) || containsInvalidTagChars(tag.Value) {
			return fmt.Errorf("tag contains invalid characters")
		}
	}

	// Check for duplicate keys
	keys := make(map[string]bool)
	for _, tag := range tags {
		if keys[tag.Key] {
			return fmt.Errorf("duplicate tag key: %s", tag.Key)
		}
		keys[tag.Key] = true
	}

	return nil
}

// containsInvalidTagChars checks for invalid characters
func containsInvalidTagChars(s string) bool {
	invalidChars := []string{"<", ">", "\"", "&"}
	for _, c := range invalidChars {
		if len(s) > 0 && contains(s, c) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// EscapeXML escapes special XML characters
func EscapeXML(s string) string {
	s = ReplaceAll(s, "&", "&amp;")
	s = ReplaceAll(s, "<", "&lt;")
	s = ReplaceAll(s, ">", "&gt;")
	s = ReplaceAll(s, "\"", "&quot;")
	s = ReplaceAll(s, "'", "&apos;")
	return s
}

// ReplaceAll replaces all occurrences
func ReplaceAll(s, old, new string) string {
	result := s
	for {
		idx := findIndex(result, old)
		if idx == -1 {
			break
		}
		result = result[:idx] + new + result[idx+len(old):]
	}
	return result
}

func findIndex(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// TagProcessor processes tags for objects
type TagProcessor struct {
	validator *TagValidator
}

// NewTagProcessor creates a new tag processor
func NewTagProcessor() *TagProcessor {
	return &TagProcessor{
		validator: NewTagValidator(),
	}
}

// ProcessTags processes and validates tags
func (p *TagProcessor) ProcessTags(tags TagSet) (TagSet, error) {
	if err := p.validator.Validate(tags); err != nil {
		return nil, err
	}
	return tags, nil
}

// FilterTags filters tags by prefix
func (p *TagProcessor) FilterTags(tags TagSet, prefix string) TagSet {
	var filtered TagSet
	for _, tag := range tags {
		if len(prefix) == 0 || len(tag.Key) >= len(prefix) && tag.Key[:len(prefix)] == prefix {
			filtered = append(filtered, tag)
		}
	}
	return filtered
}

// MergeTags merges two tag sets
func (p *TagProcessor) MergeTags(existing, new TagSet) TagSet {
	result := make(TagSet, len(existing))
	copy(result, existing)

	for _, newTag := range new {
		found := false
		for i, existingTag := range result {
			if existingTag.Key == newTag.Key {
				result[i] = newTag
				found = true
				break
			}
		}
		if !found {
			result = append(result, newTag)
		}
	}

	return result
}
