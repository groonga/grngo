package gnx

//import (
//	""
//)

// -- TableOptions --

// Constants for TableOptions.
type TableType int

const (
	ArrayTable = TableType(iota)
	HashTable
	PatTable
	DatTable
)

// http://groonga.org/docs/reference/commands/table_create.html
type TableOptions struct {
	TableType
	WithSIS          bool     // KEY_WITH_SIS
	KeyType          string   // http://groonga.org/docs/reference/types.html
	ValueType        string   // http://groonga.org/docs/reference/types.html
	DefaultTokenizer string   // http://groonga.org/docs/reference/tokenizers.html
	Normalizer       string   // http://groonga.org/docs/reference/normalizers.html
	TokenFilters     []string // http://groonga.org/docs/reference/token_filters.html
}

// NewTableOptions() creates a new TableOptions object with the default
// settings.
func NewTableOptions() *TableOptions {
	var options TableOptions
	return &options
}

// -- ColumnOptions --

// Constants for ColumnOptions.
type ColumnType int

const (
	ScalarColumn = ColumnType(iota)
	VectorColumn
	IndexColumn
)

// Constants for ColumnOptions.
type CompressionType int

const (
	NoCompression = CompressionType(iota)
	ZlibCompression
	LzoCompression
)

// http://groonga.org/ja/docs/reference/commands/column_create.html
type ColumnOptions struct {
	ColumnType
	CompressionType
	WithSection  bool // WITH_SECTION
	WithWeight   bool // WITH_WEIGHT
	WithPosition bool // WITH_POSITION
	Source       string
}

// NewColumnOptions() creates a new ColumnOptions object with the default
// settings.
func NewColumnOptions() *ColumnOptions {
	var options ColumnOptions
	return &options
}
