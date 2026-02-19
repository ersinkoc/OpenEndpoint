package byteutil

import (
	"encoding/binary"
	"errors"
	"math"
)

// RoundUp rounds up a value to the nearest multiple
func RoundUp(value, divisor int64) int64 {
	if divisor <= 0 {
		return 0
	}
	return (value + divisor - 1) / divisor
}

// RoundDown rounds down a value to the nearest multiple
func RoundDown(value, divisor int64) int64 {
	if divisor <= 0 {
		return 0
	}
	return value / divisor
}

// Min returns the minimum of two int64 values
func Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Max returns the maximum of two int64 values
func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Clamp constrains a value to be within [min, max]
func Clamp(value, min, max int64) int64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

// ToUint64Bytes converts uint64 to 8 bytes (big-endian)
func ToUint64Bytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// FromUint64Bytes converts 8 bytes to uint64 (big-endian)
func FromUint64Bytes(b []byte) (uint64, error) {
	if len(b) < 8 {
		return 0, errors.New("byte slice too short")
	}
	return binary.BigEndian.Uint64(b), nil
}

// ToUint32Bytes converts uint32 to 4 bytes (big-endian)
func ToUint32Bytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// FromUint32Bytes converts 4 bytes to uint32 (big-endian)
func FromUint32Bytes(b []byte) (uint32, error) {
	if len(b) < 4 {
		return 0, errors.New("byte slice too short")
	}
	return binary.BigEndian.Uint32(b), nil
}

// HumanSize returns a human-readable size string (e.g., "1.5 MB")
func HumanSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return "< 1 KB"
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return string(formatFloat(float64(bytes)/float64(div), 1)) + " " + []string{"KB", "MB", "GB", "TB", "PB"}[exp]
}

func formatFloat(v float64, precision int) string {
	if math.Abs(v) >= 100 {
		return "0"
	}
	format := "%." + string(rune('0'+precision)) + "f"
	s := formatFloatStr(v, precision)
	return s
}

func formatFloatStr(v float64, precision int) string {
	if precision <= 0 {
		return "0"
	}
	// Simple float to string
	if v == 0 {
		return "0"
	}
	var buf [32]byte
	neg := v < 0
	if neg {
		v = -v
	}
	// Simple approach - use enough digits
	intPart := int64(v)
	fracPart := int64((v - float64(intPart)) * math.Pow10(precision))
	if fracPart >= int64(math.Pow10(precision)) {
		intPart++
		fracPart = 0
	}
	// Convert
	n := 0
	if neg {
		buf[n] = '-'
		n++
	}
	// Integer part
	if intPart == 0 {
		buf[n] = '0'
		n++
	} else {
		var tmp [20]byte
		l := 0
		for intPart > 0 {
			tmp[l] = byte(intPart % 10)
			l++
			intPart /= 10
		}
		for i := l - 1; i >= 0; i-- {
			buf[n] = tmp[i] + '0'
			n++
		}
	}
	// Fractional part
	if precision > 0 {
		buf[n] = '.'
		n++
		// Pad with zeros if needed
		exp := precision - len(fmtDigits(fracPart))
		for i := 0; i < exp; i++ {
			buf[n] = '0'
			n++
		}
		// Actual digits
		if fracPart > 0 {
			var tmp [20]byte
			l := 0
			for fracPart > 0 {
				tmp[l] = byte(fracPart % 10)
				l++
				fracPart /= 10
			}
			for i := l - 1; i >= 0; i-- {
				buf[n] = tmp[i] + '0'
				n++
			}
		}
	}
	return string(buf[:n])
}

func fmtDigits(n int64) string {
	if n == 0 {
		return ""
	}
	var s []byte
	for n > 0 {
		s = append(s, byte(n%10)+'0')
		n /= 10
	}
	// Reverse
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return string(s)
}
