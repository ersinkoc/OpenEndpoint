package tags

import (
	"testing"
)

func TestTagSetToMap(t *testing.T) {
	ts := TagSet{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
	}

	m := ts.ToMap()
	if len(m) != 2 {
		t.Errorf("len(m) = %v, want 2", len(m))
	}
	if m["key1"] != "value1" {
		t.Errorf("m[key1] = %v, want value1", m["key1"])
	}
	if m["key2"] != "value2" {
		t.Errorf("m[key2] = %v, want value2", m["key2"])
	}
}

func TestTagSetToMapEmpty(t *testing.T) {
	ts := TagSet{}
	m := ts.ToMap()
	if len(m) != 0 {
		t.Errorf("len(m) = %v, want 0", len(m))
	}
}

func TestFromMap(t *testing.T) {
	m := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	ts := FromMap(m)
	if len(ts) != 2 {
		t.Errorf("len(ts) = %v, want 2", len(ts))
	}

	v1, ok := ts.Get("key1")
	if !ok || v1 != "value1" {
		t.Errorf("Get(key1) = %v, %v, want value1, true", v1, ok)
	}
}

func TestTagSetGet(t *testing.T) {
	ts := TagSet{
		{Key: "key1", Value: "value1"},
	}

	v, ok := ts.Get("key1")
	if !ok || v != "value1" {
		t.Errorf("Get(key1) = %v, %v, want value1, true", v, ok)
	}

	v, ok = ts.Get("nonexistent")
	if ok || v != "" {
		t.Errorf("Get(nonexistent) = %v, %v, want '', false", v, ok)
	}
}

func TestTagSetSet(t *testing.T) {
	ts := TagSet{}

	ts.Set("key1", "value1")
	if len(ts) != 1 {
		t.Errorf("len(ts) = %v, want 1", len(ts))
	}

	v, ok := ts.Get("key1")
	if !ok || v != "value1" {
		t.Errorf("Get(key1) = %v, %v, want value1, true", v, ok)
	}

	ts.Set("key1", "updated")
	v, ok = ts.Get("key1")
	if !ok || v != "updated" {
		t.Errorf("Get(key1) after update = %v, %v, want updated, true", v, ok)
	}
}

func TestTagSetDelete(t *testing.T) {
	ts := TagSet{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
	}

	ts.Delete("key1")
	if len(ts) != 1 {
		t.Errorf("len(ts) = %v, want 1", len(ts))
	}

	_, ok := ts.Get("key1")
	if ok {
		t.Error("key1 should be deleted")
	}

	ts.Delete("nonexistent")
	if len(ts) != 1 {
		t.Errorf("len(ts) should remain 1 after deleting nonexistent key")
	}
}

func TestTaggingToXML(t *testing.T) {
	tagging := &Tagging{
		TagSet: TagSet{
			{Key: "key1", Value: "value1"},
			{Key: "key2", Value: "value2"},
		},
	}

	xml := tagging.ToXML()
	if xml == "" {
		t.Error("ToXML should not return empty string")
	}
	if !contains(xml, "<Tagging") {
		t.Error("XML should contain <Tagging>")
	}
	if !contains(xml, "<TagSet>") {
		t.Error("XML should contain <TagSet>")
	}
	if !contains(xml, "<Key>key1</Key>") {
		t.Error("XML should contain key1")
	}
}

func TestTaggingToXMLEmpty(t *testing.T) {
	tagging := &Tagging{TagSet: TagSet{}}
	xml := tagging.ToXML()
	if xml == "" {
		t.Error("ToXML should not return empty string for empty tagset")
	}
}

func TestFromXML(t *testing.T) {
	xmlData := `<?xml version="1.0" encoding="UTF-8"?>
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <TagSet>
    <Tag>
      <Key>key1</Key>
      <Value>value1</Value>
    </Tag>
  </TagSet>
</Tagging>`

	tagging, err := FromXML([]byte(xmlData))
	if err != nil {
		t.Fatalf("FromXML failed: %v", err)
	}

	if len(tagging.TagSet) != 1 {
		t.Errorf("len(TagSet) = %v, want 1", len(tagging.TagSet))
	}

	if tagging.TagSet[0].Key != "key1" {
		t.Errorf("Key = %v, want key1", tagging.TagSet[0].Key)
	}
}

func TestFromXMLInvalid(t *testing.T) {
	_, err := FromXML([]byte("invalid xml"))
	if err == nil {
		t.Error("FromXML should return error for invalid XML")
	}
}

func TestTagValidatorValidate(t *testing.T) {
	validator := NewTagValidator()

	ts := TagSet{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
	}

	err := validator.Validate(ts)
	if err != nil {
		t.Errorf("Validate should pass for valid tags: %v", err)
	}
}

func TestTagValidatorTooManyTags(t *testing.T) {
	validator := NewTagValidator()

	ts := make(TagSet, 11)
	for i := 0; i < 11; i++ {
		ts[i] = Tag{Key: "key", Value: "value"}
	}

	err := validator.Validate(ts)
	if err == nil {
		t.Error("Validate should fail for too many tags")
	}
}

func TestTagValidatorEmptyKey(t *testing.T) {
	validator := NewTagValidator()

	ts := TagSet{{Key: "", Value: "value"}}

	err := validator.Validate(ts)
	if err == nil {
		t.Error("Validate should fail for empty key")
	}
}

func TestTagValidatorKeyTooLong(t *testing.T) {
	validator := NewTagValidator()

	longKey := make([]byte, 129)
	for i := range longKey {
		longKey[i] = 'a'
	}

	ts := TagSet{{Key: string(longKey), Value: "value"}}

	err := validator.Validate(ts)
	if err == nil {
		t.Error("Validate should fail for key too long")
	}
}

func TestTagValidatorValueTooLong(t *testing.T) {
	validator := NewTagValidator()

	longValue := make([]byte, 257)
	for i := range longValue {
		longValue[i] = 'a'
	}

	ts := TagSet{{Key: "key", Value: string(longValue)}}

	err := validator.Validate(ts)
	if err == nil {
		t.Error("Validate should fail for value too long")
	}
}

func TestTagValidatorInvalidChars(t *testing.T) {
	validator := NewTagValidator()

	tests := []struct {
		key   string
		value string
	}{
		{"key<", "value"},
		{"key>", "value"},
		{"key\"", "value"},
		{"key&", "value"},
		{"key", "value<"},
		{"key", "value>"},
		{"key", "value\""},
		{"key", "value&"},
	}

	for _, tt := range tests {
		ts := TagSet{{Key: tt.key, Value: tt.value}}
		err := validator.Validate(ts)
		if err == nil {
			t.Errorf("Validate should fail for invalid chars in key=%s value=%s", tt.key, tt.value)
		}
	}
}

func TestTagValidatorDuplicateKeys(t *testing.T) {
	validator := NewTagValidator()

	ts := TagSet{
		{Key: "key1", Value: "value1"},
		{Key: "key1", Value: "value2"},
	}

	err := validator.Validate(ts)
	if err == nil {
		t.Error("Validate should fail for duplicate keys")
	}
}

func TestContainsInvalidTagChars(t *testing.T) {
	if !containsInvalidTagChars("<") {
		t.Error("should detect <")
	}
	if !containsInvalidTagChars(">") {
		t.Error("should detect >")
	}
	if !containsInvalidTagChars("\"") {
		t.Error("should detect \"")
	}
	if !containsInvalidTagChars("&") {
		t.Error("should detect &")
	}
	if containsInvalidTagChars("normal") {
		t.Error("should not detect invalid chars in normal string")
	}
	if containsInvalidTagChars("") {
		t.Error("should return false for empty string")
	}
}

func TestContains(t *testing.T) {
	if !contains("hello world", "world") {
		t.Error("should find 'world' in 'hello world'")
	}
	if contains("hello", "xyz") {
		t.Error("should not find 'xyz' in 'hello'")
	}
	if !contains("hello", "h") {
		t.Error("should find 'h' in 'hello'")
	}
	if !contains("hello", "o") {
		t.Error("should find 'o' in 'hello'")
	}
}

func TestEscapeXML(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"normal", "normal"},
		{"a&b", "a&amp;b"},
		{"<tag>", "&lt;tag&gt;"},
		{`"quote"`, "&quot;quote&quot;"},
		{"'apos'", "&apos;apos&apos;"},
	}

	for _, tt := range tests {
		result := EscapeXML(tt.input)
		if result != tt.expected {
			t.Errorf("EscapeXML(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestReplaceAll(t *testing.T) {
	tests := []struct {
		s        string
		old      string
		new      string
		expected string
	}{
		{"hello", "l", "L", "heLLo"},
		{"aaa", "a", "b", "bbb"},
		{"hello", "x", "y", "hello"},
		{"", "a", "b", ""},
	}

	for _, tt := range tests {
		result := ReplaceAll(tt.s, tt.old, tt.new)
		if result != tt.expected {
			t.Errorf("ReplaceAll(%q, %q, %q) = %q, want %q", tt.s, tt.old, tt.new, result, tt.expected)
		}
	}
}

func TestFindIndex(t *testing.T) {
	tests := []struct {
		s        string
		substr   string
		expected int
	}{
		{"hello", "ll", 2},
		{"hello", "world", -1},
		{"hello", "h", 0},
		{"hello", "o", 4},
		{"", "a", -1},
	}

	for _, tt := range tests {
		result := findIndex(tt.s, tt.substr)
		if result != tt.expected {
			t.Errorf("findIndex(%q, %q) = %d, want %d", tt.s, tt.substr, result, tt.expected)
		}
	}
}

func TestTagProcessorProcessTags(t *testing.T) {
	processor := NewTagProcessor()

	ts := TagSet{{Key: "key1", Value: "value1"}}

	result, err := processor.ProcessTags(ts)
	if err != nil {
		t.Errorf("ProcessTags failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("len(result) = %v, want 1", len(result))
	}
}

func TestTagProcessorProcessTagsInvalid(t *testing.T) {
	processor := NewTagProcessor()

	ts := TagSet{{Key: "", Value: "value"}}

	_, err := processor.ProcessTags(ts)
	if err == nil {
		t.Error("ProcessTags should fail for invalid tags")
	}
}

func TestTagProcessorFilterTags(t *testing.T) {
	processor := NewTagProcessor()

	ts := TagSet{
		{Key: "env:prod", Value: "value1"},
		{Key: "env:dev", Value: "value2"},
		{Key: "owner", Value: "value3"},
	}

	filtered := processor.FilterTags(ts, "env:")
	if len(filtered) != 2 {
		t.Errorf("len(filtered) = %v, want 2", len(filtered))
	}

	filteredAll := processor.FilterTags(ts, "")
	if len(filteredAll) != 3 {
		t.Errorf("len(filteredAll) = %v, want 3 (empty prefix)", len(filteredAll))
	}
}

func TestTagProcessorMergeTags(t *testing.T) {
	processor := NewTagProcessor()

	existing := TagSet{
		{Key: "key1", Value: "old1"},
		{Key: "key2", Value: "old2"},
	}

	newTags := TagSet{
		{Key: "key1", Value: "new1"},
		{Key: "key3", Value: "new3"},
	}

	merged := processor.MergeTags(existing, newTags)

	if len(merged) != 3 {
		t.Errorf("len(merged) = %v, want 3", len(merged))
	}

	v, ok := merged.Get("key1")
	if !ok || v != "new1" {
		t.Errorf("key1 should be updated to new1, got %v", v)
	}

	v, ok = merged.Get("key2")
	if !ok || v != "old2" {
		t.Errorf("key2 should remain old2, got %v", v)
	}

	v, ok = merged.Get("key3")
	if !ok || v != "new3" {
		t.Errorf("key3 should be new3, got %v", v)
	}
}

func TestTagProcessorMergeTagsEmpty(t *testing.T) {
	processor := NewTagProcessor()

	existing := TagSet{{Key: "key1", Value: "value1"}}
	newTags := TagSet{}

	merged := processor.MergeTags(existing, newTags)
	if len(merged) != 1 {
		t.Errorf("len(merged) = %v, want 1", len(merged))
	}

	merged = processor.MergeTags(TagSet{}, existing)
	if len(merged) != 1 {
		t.Errorf("len(merged) = %v, want 1", len(merged))
	}
}
