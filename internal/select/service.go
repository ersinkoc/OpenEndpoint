package s3select

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"go.uber.org/zap"
)

// InputFormat represents the input data format
type InputFormat string

const (
	FormatJSON  InputFormat = "JSON"
	FormatCSV   InputFormat = "CSV"
	FormatParquet InputFormat = "Parquet"
)

// OutputFormat represents the output format
type OutputFormat string

const (
	OutputJSON  OutputFormat = "JSON"
	OutputCSV   OutputFormat = "CSV"
	OutputRaw   OutputFormat = "RAW"
)

// ExpressionType represents the expression type
type ExpressionType string

const (
	ExpressionTypeSQL ExpressionType = "SQL"
)

// SelectRequest represents an S3 Select request
type SelectRequest struct {
	Bucket              string
	Key                 string
	Expression          string
	ExpressionType      ExpressionType
	InputSerialization  InputSerialization
	OutputSerialization OutputSerialization
	ScanRange          *ScanRange
}

// ScanRange represents a range of bytes to scan
type ScanRange struct {
	Start int64
	End   int64
}

// InputSerialization contains input serialization settings
type InputSerialization struct {
	Format        InputFormat
	JSON          *JSONInput
	CSV           *CSVInput
	CompressionType string
}

// JSONInput contains JSON-specific input settings
type JSONInput struct {
	Type string // Document, Lines
}

// CSVInput contains CSV-specific input settings
type CSVInput struct {
	FileHeaderInfo string // Use, Ignore, None
	RecordDelimiter string
	FieldDelimiter string
	QuoteCharacter string
	QuoteEscapeCharacter string
	CommentCharacter string
}

// OutputSerialization contains output serialization settings
type OutputSerialization struct {
	Format OutputFormat
	JSON   *JSONOutput
	CSV    *CSVOutput
}

// JSONOutput contains JSON-specific output settings
type JSONOutput struct {
	RecordDelimiter string
}

// CSVOutput contains CSV-specific output settings
type CSVOutput struct {
	RecordDelimiter string
	FieldDelimiter string
	QuoteCharacter string
	QuoteEscapeCharacter string
}

// SelectResult contains the select result
type SelectResult struct {
	Payload   []byte
	Stats     *SelectStats
	EndMarker bool
}

// SelectStats contains select statistics
type SelectStats struct {
	BytesScanned    int64
	BytesProcessed int64
	BytesReturned  int64
	RecordsReturned int64
}

// Parser parses SQL expressions
type Parser struct {
	logger *zap.Logger
}

// NewParser creates a new SQL parser
func NewParser(logger *zap.Logger) *Parser {
	return &Parser{logger: logger}
}

// Parse parses a SQL expression into an AST
func (p *Parser) Parse(sql string) (*AST, error) {
	sql = strings.TrimSpace(sql)

	// Simple SQL parser for SELECT statements
	// Supports: SELECT columns FROM table [WHERE condition]

	// Parse SELECT clause
	if !strings.HasPrefix(strings.ToUpper(sql), "SELECT") {
		return nil, fmt.Errorf("invalid SQL: must start with SELECT")
	}

	// Extract SELECT columns
	selectIdx := 7 // len("SELECT")
	fromIdx := strings.Index(sql, "FROM")
	if fromIdx == -1 {
		return nil, fmt.Errorf("invalid SQL: missing FROM clause")
	}

	columnsStr := strings.TrimSpace(sql[selectIdx:fromIdx])
	columns := p.parseColumns(columnsStr)

	// Extract WHERE clause
	var whereClause string
	whereIdx := strings.Index(sql, "WHERE")
	if whereIdx != -1 {
		whereClause = strings.TrimSpace(sql[whereIdx+5:])
	}

	// Extract LIMIT
	var limit int64 = 0
	limitIdx := strings.Index(sql, "LIMIT")
	if limitIdx != -1 {
		limitStr := strings.TrimSpace(sql[limitIdx+5:])
		fmt.Sscanf(limitStr, "%d", &limit)
	}

	return &AST{
		Columns:     columns,
		WhereClause: whereClause,
		Limit:       limit,
	}, nil
}

// parseColumns parses column list
func (p *Parser) parseColumns(colStr string) []string {
	colStr = strings.TrimSpace(colStr)

	if colStr == "*" {
		return []string{"*"}
	}

	reader := csv.NewReader(strings.NewReader(colStr))
	columns, err := reader.Read()
	if err != nil {
		return []string{colStr}
	}

	return columns
}

// AST represents a parsed SQL AST
type AST struct {
	Columns     []string
	WhereClause string
	Limit       int64
}

// Evaluator evaluates the AST against records
type Evaluator struct {
	ast    *AST
	input  InputFormat
	logger *zap.Logger
	stats  SelectStats
	mu     sync.Mutex
}

// NewEvaluator creates a new evaluator
func NewEvaluator(ast *AST, input InputFormat, logger *zap.Logger) *Evaluator {
	return &Evaluator{
		ast:   ast,
		input: input,
		logger: logger,
		stats: SelectStats{},
	}
}

// Evaluate evaluates the select query on input data
func (e *Evaluator) Evaluate(ctx context.Context, inputData io.Reader) (*SelectResult, error) {
	var output []string
	recordCount := int64(0)

	switch e.input {
	case FormatJSON:
		decoder := json.NewDecoder(inputData)
		for {
			var record map[string]interface{}
			if err := decoder.Decode(&record); err != nil {
				if err == io.EOF {
					break
				}
				e.logger.Debug("Error decoding JSON", zap.Error(err))
				continue
			}

			e.stats.BytesProcessed += estimateRecordSize(record)

			// Check where clause (simplified - always true for now)
			if e.ast.WhereClause != "" {
				// TODO: Implement where clause evaluation
			}

			// Select columns
			selected := e.selectColumns(record)
			output = append(output, selected)
			recordCount++

			// Check limit
			if e.ast.Limit > 0 && recordCount >= e.ast.Limit {
				break
			}
		}

	case FormatCSV:
		reader := csv.NewReader(inputData)
		headers, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV headers: %w", err)
		}

		for {
			record, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				e.logger.Debug("Error reading CSV record", zap.Error(err))
				continue
			}

			e.stats.BytesProcessed += int64(len(strings.Join(record, ",")))

			// Convert to map
			recordMap := make(map[string]interface{})
			for i, h := range headers {
				if i < len(record) {
					recordMap[h] = record[i]
				}
			}

			// Select columns
			selected := e.selectColumns(recordMap)
			output = append(output, selected)
			recordCount++

			// Check limit
			if e.ast.Limit > 0 && recordCount >= e.ast.Limit {
				break
			}
		}
	}

	e.stats.RecordsReturned = recordCount
	e.stats.BytesReturned = int64(len(strings.Join(output, "")))

	// Format output
	payload, err := e.formatOutput(output)
	if err != nil {
		return nil, err
	}

	return &SelectResult{
		Payload:   payload,
		Stats:     &e.stats,
		EndMarker: true,
	}, nil
}

// selectColumns selects specific columns from a record
func (e *Evaluator) selectColumns(record map[string]interface{}) string {
	var result []string

	for _, col := range e.ast.Columns {
		col = strings.TrimSpace(col)

		if col == "*" {
			// Return all columns as JSON
			data, _ := json.Marshal(record)
			return string(data)
		}

		if val, ok := record[col]; ok {
			result = append(result, fmt.Sprintf("%v", val))
		} else {
			result = append(result, "")
		}
	}

	return strings.Join(result, ",")
}

// formatOutput formats the output
func (e *Evaluator) formatOutput(records []string) ([]byte, error) {
	switch e.input {
	case FormatJSON:
		return json.Marshal(records)
	case FormatCSV:
		return []byte(strings.Join(records, "\n") + "\n"), nil
	default:
		return []byte(strings.Join(records, "\n")), nil
	}
}

// estimateRecordSize estimates the size of a JSON record
func estimateRecordSize(record map[string]interface{}) int64 {
	data, _ := json.Marshal(record)
	return int64(len(data))
}

// SelectService provides S3 Select functionality
type SelectService struct {
	logger *zap.Logger
	parser *Parser
}

// NewSelectService creates a new select service
func NewSelectService(logger *zap.Logger) *SelectService {
	return &SelectService{
		logger: logger,
		parser: NewParser(logger),
	}
}

// Execute executes an S3 Select request
func (s *SelectService) Execute(ctx context.Context, req *SelectRequest, data io.Reader) (*SelectResult, error) {
	s.logger.Info("Executing S3 Select",
		zap.String("bucket", req.Bucket),
		zap.String("key", req.Key),
		zap.String("expression", req.Expression))

	// Parse SQL
	ast, err := s.parser.Parse(req.Expression)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	// Create evaluator
	evaluator := NewEvaluator(ast, req.InputSerialization.Format, s.logger)

	// Execute
	result, err := evaluator.Evaluate(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate: %w", err)
	}

	s.logger.Info("S3 Select completed",
		zap.Int64("bytes_scanned", result.Stats.BytesScanned),
		zap.Int64("bytes_returned", result.Stats.BytesReturned),
		zap.Int64("records_returned", result.Stats.RecordsReturned))

	return result, nil
}

// GetStats returns current statistics
func (s *SelectService) GetStats() SelectStats {
	return SelectStats{}
}
