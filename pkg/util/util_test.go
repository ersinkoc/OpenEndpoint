package util

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestGenerateID(t *testing.T) {
	id1 := GenerateID()
	id2 := GenerateID()

	if id1 == "" {
		t.Error("GenerateID returned empty string")
	}
	if len(id1) != 32 {
		t.Errorf("GenerateID length = %d, expected 32", len(id1))
	}
	if id1 == id2 {
		t.Error("GenerateID returned same value twice")
	}
}

func TestGenerateETag(t *testing.T) {
	etag1 := GenerateETag()
	etag2 := GenerateETag()

	if etag1 == "" {
		t.Error("GenerateETag returned empty string")
	}
	if len(etag1) < 3 {
		t.Errorf("GenerateETag length = %d, expected at least 3", len(etag1))
	}
	if etag1[0] != '"' || etag1[len(etag1)-1] != '"' {
		t.Errorf("GenerateETag should be quoted, got %s", etag1)
	}
	if etag1 == etag2 {
		t.Error("GenerateETag returned same value twice")
	}
}

func TestGenerateUploadID(t *testing.T) {
	id1 := GenerateUploadID()
	id2 := GenerateUploadID()

	if id1 == "" {
		t.Error("GenerateUploadID returned empty string")
	}
	if id1 == id2 {
		t.Error("GenerateUploadID returned same value twice")
	}
}

func TestCopyReader(t *testing.T) {
	src := bytes.NewReader([]byte("hello world"))
	dst := &bytes.Buffer{}

	var progressCalls int64
	written, err := CopyReader(dst, src, func(n int64) {
		progressCalls = n
	})

	if err != nil {
		t.Errorf("CopyReader error: %v", err)
	}
	if written != 11 {
		t.Errorf("CopyReader written = %d, expected 11", written)
	}
	if dst.String() != "hello world" {
		t.Errorf("CopyReader dst = %s, expected 'hello world'", dst.String())
	}
	if progressCalls != 11 {
		t.Errorf("CopyReader progressCalls = %d, expected 11", progressCalls)
	}
}

func TestCopyReaderNoProgress(t *testing.T) {
	src := bytes.NewReader([]byte("test"))
	dst := &bytes.Buffer{}

	written, err := CopyReader(dst, src, nil)

	if err != nil {
		t.Errorf("CopyReader error: %v", err)
	}
	if written != 4 {
		t.Errorf("CopyReader written = %d, expected 4", written)
	}
}

func TestCopyReaderEmpty(t *testing.T) {
	src := bytes.NewReader([]byte{})
	dst := &bytes.Buffer{}

	written, err := CopyReader(dst, src, nil)

	if err != nil {
		t.Errorf("CopyReader error: %v", err)
	}
	if written != 0 {
		t.Errorf("CopyReader written = %d, expected 0", written)
	}
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, io.ErrUnexpectedEOF
}

func TestCopyReaderError(t *testing.T) {
	src := &errorReader{}
	dst := &bytes.Buffer{}

	_, err := CopyReader(dst, src, nil)

	if err == nil {
		t.Error("CopyReader expected error, got nil")
	}
}

type errorWriter struct{}

func (e *errorWriter) Write(p []byte) (n int, err error) {
	return 0, io.ErrClosedPipe
}

func TestCopyReaderWriteError(t *testing.T) {
	src := bytes.NewReader([]byte("hello world"))
	dst := &errorWriter{}

	_, err := CopyReader(dst, src, nil)

	if err == nil {
		t.Error("CopyReader expected write error, got nil")
	}
}

type shortWriter struct{}

func (s *shortWriter) Write(p []byte) (n int, err error) {
	return len(p) - 1, nil
}

func TestCopyReaderShortWrite(t *testing.T) {
	src := bytes.NewReader([]byte("hello world"))
	dst := &shortWriter{}

	_, err := CopyReader(dst, src, nil)

	if err != io.ErrShortWrite {
		t.Errorf("CopyReader expected io.ErrShortWrite, got %v", err)
	}
}

func TestRoundToBlock(t *testing.T) {
	tests := []struct {
		size      int64
		blockSize int64
		expected  int64
	}{
		{0, 4096, 0},
		{100, 4096, 4096},
		{4096, 4096, 4096},
		{4097, 4096, 8192},
		{8192, 4096, 8192},
		{1, 10, 10},
		{10, 10, 10},
		{11, 10, 20},
	}

	for _, tt := range tests {
		result := RoundToBlock(tt.size, tt.blockSize)
		if result != tt.expected {
			t.Errorf("RoundToBlock(%d, %d) = %d, expected %d", tt.size, tt.blockSize, result, tt.expected)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{1099511627776, "1.0 TB"},
		{1125899906842624, "1.0 PB"},
		{1152921504606846976, "1.0 EB"},
	}

	for _, tt := range tests {
		result := FormatBytes(tt.bytes)
		if result != tt.expected {
			t.Errorf("FormatBytes(%d) = %s, expected %s", tt.bytes, result, tt.expected)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		duration time.Duration
		contains string
	}{
		{0, "0ms"},
		{100 * time.Millisecond, "100ms"},
		{500 * time.Millisecond, "500ms"},
		{time.Second, "1.0s"},
		{2500 * time.Millisecond, "2.5s"},
		{time.Minute, "1.0m"},
		{90 * time.Second, "1.5m"},
		{time.Hour, "1.0h"},
		{90 * time.Minute, "1.5h"},
	}

	for _, tt := range tests {
		result := FormatDuration(tt.duration)
		if result != tt.contains {
			t.Errorf("FormatDuration(%v) = %s, expected %s", tt.duration, result, tt.contains)
		}
	}
}

func TestContainsInt(t *testing.T) {
	tests := []struct {
		slice    []int
		value    int
		expected bool
	}{
		{[]int{1, 2, 3}, 2, true},
		{[]int{1, 2, 3}, 4, false},
		{[]int{}, 1, false},
		{[]int{1}, 1, true},
	}

	for _, tt := range tests {
		result := Contains(tt.slice, tt.value)
		if result != tt.expected {
			t.Errorf("Contains(%v, %v) = %v, expected %v", tt.slice, tt.value, result, tt.expected)
		}
	}
}

func TestContainsString(t *testing.T) {
	slice := []string{"a", "b", "c"}

	if !Contains(slice, "a") {
		t.Error("Contains should return true for 'a'")
	}
	if Contains(slice, "d") {
		t.Error("Contains should return false for 'd'")
	}
}

func TestUnique(t *testing.T) {
	tests := []struct {
		input    []int
		expected []int
	}{
		{[]int{1, 2, 3}, []int{1, 2, 3}},
		{[]int{1, 2, 2, 3}, []int{1, 2, 3}},
		{[]int{1, 1, 1}, []int{1}},
		{[]int{}, []int{}},
		{[]int{1, 2, 1, 2, 3}, []int{1, 2, 3}},
	}

	for _, tt := range tests {
		result := Unique(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("Unique(%v) length = %d, expected %d", tt.input, len(result), len(tt.expected))
		}
		for i, v := range result {
			if v != tt.expected[i] {
				t.Errorf("Unique(%v)[%d] = %d, expected %d", tt.input, i, v, tt.expected[i])
			}
		}
	}
}

func TestUniqueString(t *testing.T) {
	input := []string{"a", "b", "a", "c", "b"}
	expected := []string{"a", "b", "c"}

	result := Unique(input)
	if len(result) != len(expected) {
		t.Errorf("Unique length = %d, expected %d", len(result), len(expected))
	}
}

func TestChunkSlice(t *testing.T) {
	tests := []struct {
		slice     []int
		chunkSize int
		expected  [][]int
	}{
		{[]int{1, 2, 3, 4, 5}, 2, [][]int{{1, 2}, {3, 4}, {5}}},
		{[]int{1, 2, 3}, 3, [][]int{{1, 2, 3}}},
		{[]int{1, 2, 3}, 5, [][]int{{1, 2, 3}}},
		{[]int{}, 2, [][]int{}},
		{[]int{1}, 1, [][]int{{1}}},
	}

	for _, tt := range tests {
		result := ChunkSlice(tt.slice, tt.chunkSize)
		if len(result) != len(tt.expected) {
			t.Errorf("ChunkSlice(%v, %d) length = %d, expected %d", tt.slice, tt.chunkSize, len(result), len(tt.expected))
		}
		for i, chunk := range result {
			if len(chunk) != len(tt.expected[i]) {
				t.Errorf("ChunkSlice chunk %d length = %d, expected %d", i, len(chunk), len(tt.expected[i]))
			}
		}
	}
}

func TestMinInt(t *testing.T) {
	tests := []struct {
		a, b     int
		expected int
	}{
		{5, 10, 5},
		{10, 5, 5},
		{-5, 5, -5},
		{0, 0, 0},
	}

	for _, tt := range tests {
		result := Min(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("Min(%d, %d) = %d, expected %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestMaxInt(t *testing.T) {
	tests := []struct {
		a, b     int
		expected int
	}{
		{5, 10, 10},
		{10, 5, 10},
		{-5, 5, 5},
		{0, 0, 0},
	}

	for _, tt := range tests {
		result := Max(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("Max(%d, %d) = %d, expected %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestClampInt(t *testing.T) {
	tests := []struct {
		value, min, max int
		expected        int
	}{
		{5, 0, 10, 5},
		{-5, 0, 10, 0},
		{15, 0, 10, 10},
		{5, 5, 5, 5},
	}

	for _, tt := range tests {
		result := Clamp(tt.value, tt.min, tt.max)
		if result != tt.expected {
			t.Errorf("Clamp(%d, %d, %d) = %d, expected %d", tt.value, tt.min, tt.max, result, tt.expected)
		}
	}
}
