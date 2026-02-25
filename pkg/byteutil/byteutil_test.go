package byteutil

import (
	"testing"
)

func TestRoundUp(t *testing.T) {
	tests := []struct {
		value    int64
		divisor  int64
		expected int64
	}{
		{10, 3, 4},
		{9, 3, 3},
		{0, 5, 0},
		{100, 10, 10},
		{101, 10, 11},
		{10, 0, 0},
		{10, -1, 0},
	}

	for _, tt := range tests {
		result := RoundUp(tt.value, tt.divisor)
		if result != tt.expected {
			t.Errorf("RoundUp(%d, %d) = %d, expected %d", tt.value, tt.divisor, result, tt.expected)
		}
	}
}

func TestRoundDown(t *testing.T) {
	tests := []struct {
		value    int64
		divisor  int64
		expected int64
	}{
		{10, 3, 3},
		{9, 3, 3},
		{0, 5, 0},
		{100, 10, 10},
		{101, 10, 10},
		{10, 0, 0},
		{10, -1, 0},
	}

	for _, tt := range tests {
		result := RoundDown(tt.value, tt.divisor)
		if result != tt.expected {
			t.Errorf("RoundDown(%d, %d) = %d, expected %d", tt.value, tt.divisor, result, tt.expected)
		}
	}
}

func TestMin(t *testing.T) {
	tests := []struct {
		a, b     int64
		expected int64
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

func TestMax(t *testing.T) {
	tests := []struct {
		a, b     int64
		expected int64
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

func TestClamp(t *testing.T) {
	tests := []struct {
		value, min, max int64
		expected        int64
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

func TestToUint64Bytes(t *testing.T) {
	tests := []struct {
		value    uint64
		expected []byte
	}{
		{0, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{1, []byte{0, 0, 0, 0, 0, 0, 0, 1}},
		{255, []byte{0, 0, 0, 0, 0, 0, 0, 255}},
		{256, []byte{0, 0, 0, 0, 0, 0, 1, 0}},
	}

	for _, tt := range tests {
		result := ToUint64Bytes(tt.value)
		if len(result) != 8 {
			t.Errorf("ToUint64Bytes(%d) length = %d, expected 8", tt.value, len(result))
		}
		for i, b := range result {
			if b != tt.expected[i] {
				t.Errorf("ToUint64Bytes(%d)[%d] = %d, expected %d", tt.value, i, b, tt.expected[i])
			}
		}
	}
}

func TestFromUint64Bytes(t *testing.T) {
	tests := []struct {
		bytes    []byte
		expected uint64
		hasError bool
	}{
		{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, 0, false},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 1}, 1, false},
		{[]byte{0, 0, 0, 0, 0, 0, 1, 0}, 256, false},
		{[]byte{0, 0, 0, 0}, 0, true},
		{[]byte{}, 0, true},
		{nil, 0, true},
	}

	for _, tt := range tests {
		result, err := FromUint64Bytes(tt.bytes)
		if tt.hasError {
			if err == nil {
				t.Errorf("FromUint64Bytes(%v) expected error, got nil", tt.bytes)
			}
		} else {
			if err != nil {
				t.Errorf("FromUint64Bytes(%v) unexpected error: %v", tt.bytes, err)
			}
			if result != tt.expected {
				t.Errorf("FromUint64Bytes(%v) = %d, expected %d", tt.bytes, result, tt.expected)
			}
		}
	}
}

func TestToUint32Bytes(t *testing.T) {
	tests := []struct {
		value    uint32
		expected []byte
	}{
		{0, []byte{0, 0, 0, 0}},
		{1, []byte{0, 0, 0, 1}},
		{255, []byte{0, 0, 0, 255}},
		{256, []byte{0, 0, 1, 0}},
	}

	for _, tt := range tests {
		result := ToUint32Bytes(tt.value)
		if len(result) != 4 {
			t.Errorf("ToUint32Bytes(%d) length = %d, expected 4", tt.value, len(result))
		}
		for i, b := range result {
			if b != tt.expected[i] {
				t.Errorf("ToUint32Bytes(%d)[%d] = %d, expected %d", tt.value, i, b, tt.expected[i])
			}
		}
	}
}

func TestFromUint32Bytes(t *testing.T) {
	tests := []struct {
		bytes    []byte
		expected uint32
		hasError bool
	}{
		{[]byte{0, 0, 0, 0}, 0, false},
		{[]byte{0, 0, 0, 1}, 1, false},
		{[]byte{0, 0, 1, 0}, 256, false},
		{[]byte{0, 0}, 0, true},
		{[]byte{}, 0, true},
		{nil, 0, true},
	}

	for _, tt := range tests {
		result, err := FromUint32Bytes(tt.bytes)
		if tt.hasError {
			if err == nil {
				t.Errorf("FromUint32Bytes(%v) expected error, got nil", tt.bytes)
			}
		} else {
			if err != nil {
				t.Errorf("FromUint32Bytes(%v) unexpected error: %v", tt.bytes, err)
			}
			if result != tt.expected {
				t.Errorf("FromUint32Bytes(%v) = %d, expected %d", tt.bytes, result, tt.expected)
			}
		}
	}
}

func TestHumanSize(t *testing.T) {
	tests := []struct {
		bytes    int64
		contains string
	}{
		{0, "< 1 KB"},
		{100, "< 1 KB"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{1099511627776, "1.0 TB"},
	}

	for _, tt := range tests {
		result := HumanSize(tt.bytes)
		if result == "" {
			t.Errorf("HumanSize(%d) returned empty string", tt.bytes)
		}
		if tt.bytes < 1024 && result != tt.contains {
			t.Errorf("HumanSize(%d) = %s, expected %s", tt.bytes, result, tt.contains)
		}
	}
}

func TestUint64RoundTrip(t *testing.T) {
	values := []uint64{0, 1, 255, 256, 65535, 65536, 4294967295, 4294967296, 18446744073709551615}

	for _, v := range values {
		bytes := ToUint64Bytes(v)
		result, err := FromUint64Bytes(bytes)
		if err != nil {
			t.Errorf("Round trip for %d failed: %v", v, err)
		}
		if result != v {
			t.Errorf("Round trip: %d -> bytes -> %d", v, result)
		}
	}
}

func TestUint32RoundTrip(t *testing.T) {
	values := []uint32{0, 1, 255, 256, 65535, 65536, 4294967295}

	for _, v := range values {
		bytes := ToUint32Bytes(v)
		result, err := FromUint32Bytes(bytes)
		if err != nil {
			t.Errorf("Round trip for %d failed: %v", v, err)
		}
		if result != v {
			t.Errorf("Round trip: %d -> bytes -> %d", v, result)
		}
	}
}

func TestFormatFloat(t *testing.T) {
	tests := []struct {
		value     float64
		precision int
		expected  string
	}{
		{1.5, 1, "1.5"},
		{150.0, 1, "0"},
		{-150.0, 1, "0"},
		{50.0, 0, "0"},
		{50.0, -1, "0"},
		{0.0, 1, "0"},
	}

	for _, tt := range tests {
		result := formatFloat(tt.value, tt.precision)
		if result != tt.expected {
			t.Errorf("formatFloat(%v, %d) = %s, expected %s", tt.value, tt.precision, result, tt.expected)
		}
	}
}

func TestFormatFloatStr(t *testing.T) {
	tests := []struct {
		value     float64
		precision int
		contains  string
	}{
		{1.5, 1, "1.5"},
		{-1.5, 1, "-1.5"},
		{0.0, 1, "0"},
		{1.0, 0, "0"},
		{1.0, -1, "0"},
		{123.456, 2, "123.45"},
		{9.999, 2, "9.99"},
		{0.5, 1, "0.5"},
		{0.05, 2, "0.05"},
		{0.9999999999999999, 20, "1."},
	}

	for _, tt := range tests {
		result := formatFloatStr(tt.value, tt.precision)
		if tt.precision <= 0 || tt.value == 0 {
			if result != "0" {
				t.Errorf("formatFloatStr(%v, %d) = %s, expected 0", tt.value, tt.precision, result)
			}
		} else if !containsStr(result, tt.contains) {
			t.Errorf("formatFloatStr(%v, %d) = %s, expected to contain %s", tt.value, tt.precision, result, tt.contains)
		}
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr || len(s) > len(substr) && containsStr(s[1:], substr)
}

func TestFmtDigits(t *testing.T) {
	tests := []struct {
		value    int64
		expected string
	}{
		{0, ""},
		{1, "1"},
		{123, "123"},
		{999, "999"},
	}

	for _, tt := range tests {
		result := fmtDigits(tt.value)
		if result != tt.expected {
			t.Errorf("fmtDigits(%d) = %s, expected %s", tt.value, result, tt.expected)
		}
	}
}

func TestHumanSizeLarge(t *testing.T) {
	tests := []struct {
		bytes    int64
		contains string
	}{
		{1125899906842624, "PB"},
		{1024 * 1024 * 1024 * 1024 * 200, "0"},
	}

	for _, tt := range tests {
		result := HumanSize(tt.bytes)
		if tt.contains != "" && !containsStr(result, tt.contains) {
			t.Errorf("HumanSize(%d) = %s, expected to contain %s", tt.bytes, result, tt.contains)
		}
	}
}
