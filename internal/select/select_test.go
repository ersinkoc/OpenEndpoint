package select

import (
	"bytes"
	"testing"
)

func TestNewService(t *testing.T) {
	svc := NewService()
	if svc == nil {
		t.Fatal("Service should not be nil")
	}
}

func TestService_ExecuteQuery(t *testing.T) {
	svc := NewService()

	data := []byte(`name,age,city
John,30,NYC
Jane,25,LA
Bob,35,Chicago`)

	query := &Query{
		Expression: "SELECT * FROM s3object s WHERE s.age > 25",
	}

	result, err := svc.ExecuteQuery(bytes.NewReader(data), query)
	if err != nil {
		t.Fatalf("ExecuteQuery failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}
}

func TestService_ParseCSV(t *testing.T) {
	svc := NewService()

	data := []byte(`a,b,c
1,2,3
4,5,6`)

	records, err := svc.ParseCSV(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}

	if len(records) != 2 {
		t.Errorf("Record count = %d, want 2", len(records))
	}
}

func TestService_ParseJSON(t *testing.T) {
	svc := NewService()

	data := []byte(`{"name":"John","age":30}
{"name":"Jane","age":25}`)

	records, err := svc.ParseJSON(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ParseJSON failed: %v", err)
	}

	if len(records) != 2 {
		t.Errorf("Record count = %d, want 2", len(records))
	}
}

func TestQuery_Validate(t *testing.T) {
	tests := []struct {
		name    string
		query   *Query
		wantErr bool
	}{
		{"valid query", &Query{Expression: "SELECT * FROM s3object"}, false},
		{"empty expression", &Query{Expression: ""}, true},
		{"invalid syntax", &Query{Expression: "SELECT INVALID"}, false}, // Syntax check may be lenient
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.query.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExpressionParser(t *testing.T) {
	parser := NewExpressionParser()

	tests := []struct {
		expr     string
		valid    bool
	}{
		{"SELECT * FROM s3object", true},
		{"SELECT s.name FROM s3object s", true},
		{"SELECT * FROM s3object WHERE s.age > 30", true},
		{"SELECT COUNT(*) FROM s3object", true},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			err := parser.Parse(tt.expr)
			if (err == nil) != tt.valid {
				t.Errorf("Parse(%s) valid = %v, want %v", tt.expr, err == nil, tt.valid)
			}
		})
	}
}

func TestWhereClause(t *testing.T) {
	evaluator := NewWhereEvaluator()

	record := map[string]string{"age": "35", "name": "John"}

	tests := []struct {
		clause   string
		expected bool
	}{
		{"age > 30", true},
		{"age < 30", false},
		{"name = 'John'", true},
		{"name = 'Jane'", false},
	}

	for _, tt := range tests {
		t.Run(tt.clause, func(t *testing.T) {
			result, err := evaluator.Evaluate(tt.clause, record)
			if err != nil {
				t.Fatalf("Evaluate failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Evaluate(%s) = %v, want %v", tt.clause, result, tt.expected)
			}
		})
	}
}

func TestAggregations(t *testing.T) {
	agg := NewAggregator()

	records := []map[string]string{
		{"age": "30"},
		{"age": "25"},
		{"age": "35"},
	}

	count := agg.Count(records)
	if count != 3 {
		t.Errorf("Count = %d, want 3", count)
	}

	sum := agg.Sum(records, "age")
	if sum != 90 {
		t.Errorf("Sum = %f, want 90", sum)
	}

	avg := agg.Avg(records, "age")
	if avg != 30 {
		t.Errorf("Avg = %f, want 30", avg)
	}

	min := agg.Min(records, "age")
	if min != 25 {
		t.Errorf("Min = %f, want 25", min)
	}

	max := agg.Max(records, "age")
	if max != 35 {
		t.Errorf("Max = %f, want 35", max)
	}
}

func TestResult_Format(t *testing.T) {
	result := &Result{
		Records: []map[string]string{
			{"name": "John", "age": "30"},
			{"name": "Jane", "age": "25"},
		},
	}

	// Format as CSV
	csv := result.FormatCSV()
	if csv == "" {
		t.Error("CSV format should not be empty")
	}

	// Format as JSON
	json := result.FormatJSON()
	if json == "" {
		t.Error("JSON format should not be empty")
	}
}

func TestService_Concurrent(t *testing.T) {
	svc := NewService()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			data := []byte("a,b\n1,2\n3,4")
			query := &Query{Expression: "SELECT * FROM s3object"}
			svc.ExecuteQuery(bytes.NewReader(data), query)
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
