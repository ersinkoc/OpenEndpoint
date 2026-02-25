package s3select

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestNewParser(t *testing.T) {
	logger := zap.NewNop()
	parser := NewParser(logger)
	if parser == nil {
		t.Fatal("Parser should not be nil")
	}
}

func TestNewSelectService(t *testing.T) {
	logger := zap.NewNop()
	svc := NewSelectService(logger)
	if svc == nil {
		t.Fatal("Service should not be nil")
	}
}

func TestNewEvaluator(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}}
	evaluator := NewEvaluator(ast, FormatJSON, logger)
	if evaluator == nil {
		t.Fatal("Evaluator should not be nil")
	}
}

func TestParserParseValid(t *testing.T) {
	logger := zap.NewNop()
	parser := NewParser(logger)

	tests := []struct {
		sql       string
		wantCols  []string
		wantWhere string
		wantLimit int64
	}{
		{"SELECT * FROM s3object", []string{"*"}, "", 0},
		{"SELECT name, age FROM s3object", []string{"name", " age"}, "", 0},
		{"SELECT * FROM s3object WHERE age > 18", []string{"*"}, "age > 18", 0},
		{"SELECT * FROM s3object LIMIT 10", []string{"*"}, "", 10},
		{"SELECT * FROM s3object WHERE age > 18 LIMIT 5", []string{"*"}, "age > 18 LIMIT 5", 5},
	}

	for _, tt := range tests {
		ast, err := parser.Parse(tt.sql)
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", tt.sql, err)
			continue
		}

		if len(ast.Columns) != len(tt.wantCols) {
			t.Errorf("Parse(%q) columns count = %d, want %d", tt.sql, len(ast.Columns), len(tt.wantCols))
		}

		if ast.WhereClause != tt.wantWhere {
			t.Errorf("Parse(%q) WhereClause = %q, want %q", tt.sql, ast.WhereClause, tt.wantWhere)
		}

		if ast.Limit != tt.wantLimit {
			t.Errorf("Parse(%q) Limit = %d, want %d", tt.sql, ast.Limit, tt.wantLimit)
		}
	}
}

func TestParserParseInvalid(t *testing.T) {
	logger := zap.NewNop()
	parser := NewParser(logger)

	tests := []string{
		"INVALID SQL",
		"SELECT *",
		"",
	}

	for _, sql := range tests {
		_, err := parser.Parse(sql)
		if err == nil {
			t.Errorf("Parse(%q) should fail", sql)
		}
	}
}

func TestParseColumns(t *testing.T) {
	logger := zap.NewNop()
	parser := NewParser(logger)

	tests := []struct {
		input string
		want  int
	}{
		{"*", 1},
		{"name", 1},
		{"name,age", 2},
		{"name, age, city", 3},
	}

	for _, tt := range tests {
		cols := parser.parseColumns(tt.input)
		if len(cols) != tt.want {
			t.Errorf("parseColumns(%q) returned %d columns, want %d", tt.input, len(cols), tt.want)
		}
	}
}

func TestInputFormatConstants(t *testing.T) {
	if FormatJSON != "JSON" {
		t.Errorf("FormatJSON = %v, want JSON", FormatJSON)
	}
	if FormatCSV != "CSV" {
		t.Errorf("FormatCSV = %v, want CSV", FormatCSV)
	}
	if FormatParquet != "Parquet" {
		t.Errorf("FormatParquet = %v, want Parquet", FormatParquet)
	}
}

func TestOutputFormatConstants(t *testing.T) {
	if OutputJSON != "JSON" {
		t.Errorf("OutputJSON = %v, want JSON", OutputJSON)
	}
	if OutputCSV != "CSV" {
		t.Errorf("OutputCSV = %v, want CSV", OutputCSV)
	}
	if OutputRaw != "RAW" {
		t.Errorf("OutputRaw = %v, want RAW", OutputRaw)
	}
}

func TestExpressionTypeConstants(t *testing.T) {
	if ExpressionTypeSQL != "SQL" {
		t.Errorf("ExpressionTypeSQL = %v, want SQL", ExpressionTypeSQL)
	}
}

func TestEvaluateJSON(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}}
	evaluator := NewEvaluator(ast, FormatJSON, logger)

	jsonData := `{"name":"John","age":30}
{"name":"Jane","age":25}`

	result, err := evaluator.Evaluate(context.Background(), strings.NewReader(jsonData))
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}

	if result == nil {
		t.Fatal("result should not be nil")
	}

	if result.Stats == nil {
		t.Fatal("result.Stats should not be nil")
	}

	if result.Stats.RecordsReturned != 2 {
		t.Errorf("RecordsReturned = %d, want 2", result.Stats.RecordsReturned)
	}
}

func TestEvaluateJSONWithLimit(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}, Limit: 1}
	evaluator := NewEvaluator(ast, FormatJSON, logger)

	jsonData := `{"name":"John","age":30}
{"name":"Jane","age":25}`

	result, err := evaluator.Evaluate(context.Background(), strings.NewReader(jsonData))
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}

	if result.Stats.RecordsReturned != 1 {
		t.Errorf("RecordsReturned = %d, want 1 (limit)", result.Stats.RecordsReturned)
	}
}

func TestEvaluateCSV(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}}
	evaluator := NewEvaluator(ast, FormatCSV, logger)

	csvData := `name,age
John,30
Jane,25`

	result, err := evaluator.Evaluate(context.Background(), strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}

	if result.Stats.RecordsReturned != 2 {
		t.Errorf("RecordsReturned = %d, want 2", result.Stats.RecordsReturned)
	}
}

func TestEvaluateCSVWithColumns(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"name"}}
	evaluator := NewEvaluator(ast, FormatCSV, logger)

	csvData := `name,age
John,30
Jane,25`

	result, err := evaluator.Evaluate(context.Background(), strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}

	if result.Stats.RecordsReturned != 2 {
		t.Errorf("RecordsReturned = %d, want 2", result.Stats.RecordsReturned)
	}
}

func TestSelectColumnsAll(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}}
	evaluator := NewEvaluator(ast, FormatJSON, logger)

	record := map[string]interface{}{"name": "John", "age": 30.0}
	result := evaluator.selectColumns(record)

	if !strings.Contains(result, "John") {
		t.Errorf("selectColumns(*) should contain 'John', got %s", result)
	}
}

func TestSelectColumnsSpecific(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"name"}}
	evaluator := NewEvaluator(ast, FormatJSON, logger)

	record := map[string]interface{}{"name": "John", "age": 30.0}
	result := evaluator.selectColumns(record)

	if result != "John" {
		t.Errorf("selectColumns(name) = %s, want John", result)
	}
}

func TestSelectColumnsMissing(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"missing"}}
	evaluator := NewEvaluator(ast, FormatJSON, logger)

	record := map[string]interface{}{"name": "John"}
	result := evaluator.selectColumns(record)

	if result != "" {
		t.Errorf("selectColumns(missing) = %s, want empty", result)
	}
}

func TestFormatOutputJSON(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}}
	evaluator := NewEvaluator(ast, FormatJSON, logger)

	records := []string{`{"name":"John"}`, `{"name":"Jane"}`}
	output, err := evaluator.formatOutput(records)
	if err != nil {
		t.Fatalf("formatOutput failed: %v", err)
	}

	if len(output) == 0 {
		t.Error("output should not be empty")
	}
}

func TestFormatOutputCSV(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}}
	evaluator := NewEvaluator(ast, FormatCSV, logger)

	records := []string{"John,30", "Jane,25"}
	output, err := evaluator.formatOutput(records)
	if err != nil {
		t.Fatalf("formatOutput failed: %v", err)
	}

	if len(output) == 0 {
		t.Error("output should not be empty")
	}
}

func TestEstimateRecordSize(t *testing.T) {
	record := map[string]interface{}{"name": "John", "age": 30.0}
	size := estimateRecordSize(record)

	if size <= 0 {
		t.Errorf("estimateRecordSize = %d, want > 0", size)
	}
}

func TestSelectServiceExecute(t *testing.T) {
	logger := zap.NewNop()
	svc := NewSelectService(logger)

	req := &SelectRequest{
		Bucket:         "test-bucket",
		Key:            "test.json",
		Expression:     "SELECT * FROM s3object",
		ExpressionType: ExpressionTypeSQL,
		InputSerialization: InputSerialization{
			Format: FormatJSON,
		},
	}

	jsonData := `{"name":"John","age":30}`
	result, err := svc.Execute(context.Background(), req, strings.NewReader(jsonData))
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("result should not be nil")
	}

	if result.Stats.RecordsReturned != 1 {
		t.Errorf("RecordsReturned = %d, want 1", result.Stats.RecordsReturned)
	}
}

func TestSelectServiceExecuteInvalidSQL(t *testing.T) {
	logger := zap.NewNop()
	svc := NewSelectService(logger)

	req := &SelectRequest{
		Bucket:         "test-bucket",
		Key:            "test.json",
		Expression:     "INVALID SQL",
		ExpressionType: ExpressionTypeSQL,
		InputSerialization: InputSerialization{
			Format: FormatJSON,
		},
	}

	_, err := svc.Execute(context.Background(), req, strings.NewReader(""))
	if err == nil {
		t.Error("Execute should fail for invalid SQL")
	}
}

func TestSelectServiceGetStats(t *testing.T) {
	logger := zap.NewNop()
	svc := NewSelectService(logger)

	stats := svc.GetStats()
	if stats.BytesScanned != 0 {
		t.Errorf("initial BytesScanned = %d, want 0", stats.BytesScanned)
	}
}

func TestSelectResultEndMarker(t *testing.T) {
	result := &SelectResult{
		Payload:   []byte("test"),
		Stats:     &SelectStats{},
		EndMarker: true,
	}

	if !result.EndMarker {
		t.Error("EndMarker should be true")
	}
}

func TestScanRange(t *testing.T) {
	scanRange := &ScanRange{Start: 0, End: 1000}

	if scanRange.Start != 0 {
		t.Errorf("Start = %d, want 0", scanRange.Start)
	}
	if scanRange.End != 1000 {
		t.Errorf("End = %d, want 1000", scanRange.End)
	}
}

func TestInputSerialization(t *testing.T) {
	input := InputSerialization{
		Format:          FormatJSON,
		JSON:            &JSONInput{Type: "Document"},
		CompressionType: "NONE",
	}

	if input.Format != FormatJSON {
		t.Errorf("Format = %v, want JSON", input.Format)
	}
	if input.JSON == nil {
		t.Error("JSON should not be nil")
	}
}

func TestCSVInput(t *testing.T) {
	csvInput := &CSVInput{
		FileHeaderInfo:  "Use",
		RecordDelimiter: "\n",
		FieldDelimiter:  ",",
	}

	if csvInput.FileHeaderInfo != "Use" {
		t.Errorf("FileHeaderInfo = %v, want Use", csvInput.FileHeaderInfo)
	}
}

func TestOutputSerialization(t *testing.T) {
	output := OutputSerialization{
		Format: OutputJSON,
		JSON:   &JSONOutput{RecordDelimiter: "\n"},
	}

	if output.Format != OutputJSON {
		t.Errorf("Format = %v, want JSON", output.Format)
	}
}

func TestEvaluateEmptyInput(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}}
	evaluator := NewEvaluator(ast, FormatJSON, logger)

	result, err := evaluator.Evaluate(context.Background(), strings.NewReader(""))
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}

	if result.Stats.RecordsReturned != 0 {
		t.Errorf("RecordsReturned = %d, want 0", result.Stats.RecordsReturned)
	}
}

func TestParseColumnsErrorPath(t *testing.T) {
	logger := zap.NewNop()
	parser := NewParser(logger)

	col := parser.parseColumns("\"unclosed")
	if len(col) != 1 || col[0] != "\"unclosed" {
		t.Errorf("parseColumns with unclosed quote should return single column, got %v", col)
	}
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read error")
}

func TestEvaluateCSVHeaderError(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}}
	evaluator := NewEvaluator(ast, FormatCSV, logger)

	_, err := evaluator.Evaluate(context.Background(), &errorReader{})
	if err == nil {
		t.Error("Evaluate should fail for CSV with read error")
	}
	if !strings.Contains(err.Error(), "failed to read CSV headers") {
		t.Errorf("Error should contain 'failed to read CSV headers', got %v", err)
	}
}

func TestFormatOutputDefault(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}}
	evaluator := NewEvaluator(ast, FormatParquet, logger)

	records := []string{"John,30", "Jane,25"}
	output, err := evaluator.formatOutput(records)
	if err != nil {
		t.Fatalf("formatOutput failed: %v", err)
	}

	if string(output) != "John,30\nJane,25" {
		t.Errorf("formatOutput default = %q, want %q", string(output), "John,30\nJane,25")
	}
}

func TestExecuteEvaluateError(t *testing.T) {
	logger := zap.NewNop()
	svc := NewSelectService(logger)

	req := &SelectRequest{
		Bucket:         "test-bucket",
		Key:            "test.csv",
		Expression:     "SELECT * FROM s3object",
		ExpressionType: ExpressionTypeSQL,
		InputSerialization: InputSerialization{
			Format: FormatCSV,
		},
	}

	_, err := svc.Execute(context.Background(), req, &errorReader{})
	if err == nil {
		t.Error("Execute should fail for CSV with read error")
	}
	if !strings.Contains(err.Error(), "failed to evaluate") {
		t.Errorf("Error should contain 'failed to evaluate', got %v", err)
	}
}

func TestEvaluateCSVRecordError(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}}
	evaluator := NewEvaluator(ast, FormatCSV, logger)

	csvData := "name,age\nJohn,30\n\"unclosed quote"

	result, err := evaluator.Evaluate(context.Background(), strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("Evaluate should not fail for CSV record errors: %v", err)
	}

	if result.Stats.RecordsReturned != 1 {
		t.Errorf("RecordsReturned = %d, want 1 (skipping malformed record)", result.Stats.RecordsReturned)
	}
}

func TestEvaluateJSONWithWhereClause(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}, WhereClause: "age > 18"}
	evaluator := NewEvaluator(ast, FormatJSON, logger)

	jsonData := `{"name":"John","age":30}`

	result, err := evaluator.Evaluate(context.Background(), strings.NewReader(jsonData))
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}

	if result.Stats.RecordsReturned != 1 {
		t.Errorf("RecordsReturned = %d, want 1", result.Stats.RecordsReturned)
	}
}

func TestEvaluateCSVWithLimit(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}, Limit: 1}
	evaluator := NewEvaluator(ast, FormatCSV, logger)

	csvData := `name,age
John,30
Jane,25
Bob,40`

	result, err := evaluator.Evaluate(context.Background(), strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}

	if result.Stats.RecordsReturned != 1 {
		t.Errorf("RecordsReturned = %d, want 1 (limit)", result.Stats.RecordsReturned)
	}
}

func TestEvaluateJSONDecodeError(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}}
	evaluator := NewEvaluator(ast, FormatJSON, logger)

	jsonData := `{"name":"John"}invalid`

	result, err := evaluator.Evaluate(context.Background(), strings.NewReader(jsonData))
	if err != nil {
		t.Fatalf("Evaluate should not fail: %v", err)
	}

	if result.Stats.RecordsReturned != 1 {
		t.Errorf("RecordsReturned = %d, want 1", result.Stats.RecordsReturned)
	}
}

func TestEvaluateFormatOutputError(t *testing.T) {
	logger := zap.NewNop()
	ast := &AST{Columns: []string{"*"}}
	evaluator := NewEvaluator(ast, FormatJSON, logger)
	evaluator.forceFormatErr = true

	jsonData := `{"name":"John"}`

	_, err := evaluator.Evaluate(context.Background(), strings.NewReader(jsonData))
	if err == nil {
		t.Fatal("Evaluate should fail when formatOutput returns error")
	}
	if !strings.Contains(err.Error(), "forced format error") {
		t.Errorf("Error should contain 'forced format error', got %v", err)
	}
}
