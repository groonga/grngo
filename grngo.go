// Another Groonga binding for Go language.
package grngo

// #cgo pkg-config: groonga
// #include "grngo.h"
import "C"

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

// -- Errors --

func rcString(rc C.grn_rc) string {
	switch rc {
	case C.GRN_SUCCESS:
		return fmt.Sprintf("GRN_SUCCESS (%d)", rc)
	case C.GRN_END_OF_DATA:
		return fmt.Sprintf("GRN_END_OF_DATA (%d)", rc)
	case C.GRN_UNKNOWN_ERROR:
		return fmt.Sprintf("GRN_UNKNOWN_ERROR (%d)", rc)
	case C.GRN_OPERATION_NOT_PERMITTED:
		return fmt.Sprintf("GRN_OPERATION_NOT_PERMITTED (%d)", rc)
	case C.GRN_NO_SUCH_FILE_OR_DIRECTORY:
		return fmt.Sprintf("GRN_NO_SUCH_FILE_OR_DIRECTORY (%d)", rc)
	case C.GRN_NO_SUCH_PROCESS:
		return fmt.Sprintf("GRN_NO_SUCH_PROCESS (%d)", rc)
	case C.GRN_INTERRUPTED_FUNCTION_CALL:
		return fmt.Sprintf("GRN_INTERRUPTED_FUNCTION_CALL (%d)", rc)
	case C.GRN_INPUT_OUTPUT_ERROR:
		return fmt.Sprintf("GRN_INPUT_OUTPUT_ERROR (%d)", rc)
	case C.GRN_NO_SUCH_DEVICE_OR_ADDRESS:
		return fmt.Sprintf("GRN_NO_SUCH_DEVICE_OR_ADDRESS (%d)", rc)
	case C.GRN_ARG_LIST_TOO_LONG:
		return fmt.Sprintf("GRN_ARG_LIST_TOO_LONG (%d)", rc)
	case C.GRN_EXEC_FORMAT_ERROR:
		return fmt.Sprintf("GRN_EXEC_FORMAT_ERROR (%d)", rc)
	case C.GRN_BAD_FILE_DESCRIPTOR:
		return fmt.Sprintf("GRN_BAD_FILE_DESCRIPTOR (%d)", rc)
	case C.GRN_NO_CHILD_PROCESSES:
		return fmt.Sprintf("GRN_NO_CHILD_PROCESSES (%d)", rc)
	case C.GRN_RESOURCE_TEMPORARILY_UNAVAILABLE:
		return fmt.Sprintf("GRN_RESOURCE_TEMPORARILY_UNAVAILABLE (%d)", rc)
	case C.GRN_NOT_ENOUGH_SPACE:
		return fmt.Sprintf("GRN_NOT_ENOUGH_SPACE (%d)", rc)
	case C.GRN_PERMISSION_DENIED:
		return fmt.Sprintf("GRN_PERMISSION_DENIED (%d)", rc)
	case C.GRN_BAD_ADDRESS:
		return fmt.Sprintf("GRN_BAD_ADDRESS (%d)", rc)
	case C.GRN_RESOURCE_BUSY:
		return fmt.Sprintf("GRN_RESOURCE_BUSY (%d)", rc)
	case C.GRN_FILE_EXISTS:
		return fmt.Sprintf("GRN_FILE_EXISTS (%d)", rc)
	case C.GRN_IMPROPER_LINK:
		return fmt.Sprintf("GRN_IMPROPER_LINK (%d)", rc)
	case C.GRN_NO_SUCH_DEVICE:
		return fmt.Sprintf("GRN_NO_SUCH_DEVICE (%d)", rc)
	case C.GRN_NOT_A_DIRECTORY:
		return fmt.Sprintf("GRN_NOT_A_DIRECTORY (%d)", rc)
	case C.GRN_IS_A_DIRECTORY:
		return fmt.Sprintf("GRN_IS_A_DIRECTORY (%d)", rc)
	case C.GRN_INVALID_ARGUMENT:
		return fmt.Sprintf("GRN_INVALID_ARGUMENT (%d)", rc)
	case C.GRN_TOO_MANY_OPEN_FILES_IN_SYSTEM:
		return fmt.Sprintf("GRN_TOO_MANY_OPEN_FILES_IN_SYSTEM (%d)", rc)
	case C.GRN_TOO_MANY_OPEN_FILES:
		return fmt.Sprintf("GRN_TOO_MANY_OPEN_FILES (%d)", rc)
	case C.GRN_INAPPROPRIATE_I_O_CONTROL_OPERATION:
		return fmt.Sprintf("GRN_INAPPROPRIATE_I_O_CONTROL_OPERATION (%d)", rc)
	case C.GRN_FILE_TOO_LARGE:
		return fmt.Sprintf("GRN_FILE_TOO_LARGE (%d)", rc)
	case C.GRN_NO_SPACE_LEFT_ON_DEVICE:
		return fmt.Sprintf("GRN_NO_SPACE_LEFT_ON_DEVICE (%d)", rc)
	case C.GRN_INVALID_SEEK:
		return fmt.Sprintf("GRN_INVALID_SEEK (%d)", rc)
	case C.GRN_READ_ONLY_FILE_SYSTEM:
		return fmt.Sprintf("GRN_READ_ONLY_FILE_SYSTEM (%d)", rc)
	case C.GRN_TOO_MANY_LINKS:
		return fmt.Sprintf("GRN_TOO_MANY_LINKS (%d)", rc)
	case C.GRN_BROKEN_PIPE:
		return fmt.Sprintf("GRN_BROKEN_PIPE (%d)", rc)
	case C.GRN_DOMAIN_ERROR:
		return fmt.Sprintf("GRN_DOMAIN_ERROR (%d)", rc)
	case C.GRN_RESULT_TOO_LARGE:
		return fmt.Sprintf("GRN_RESULT_TOO_LARGE (%d)", rc)
	case C.GRN_RESOURCE_DEADLOCK_AVOIDED:
		return fmt.Sprintf("GRN_RESOURCE_DEADLOCK_AVOIDED (%d)", rc)
	case C.GRN_NO_MEMORY_AVAILABLE:
		return fmt.Sprintf("GRN_NO_MEMORY_AVAILABLE (%d)", rc)
	case C.GRN_FILENAME_TOO_LONG:
		return fmt.Sprintf("GRN_FILENAME_TOO_LONG (%d)", rc)
	case C.GRN_NO_LOCKS_AVAILABLE:
		return fmt.Sprintf("GRN_NO_LOCKS_AVAILABLE (%d)", rc)
	case C.GRN_FUNCTION_NOT_IMPLEMENTED:
		return fmt.Sprintf("GRN_FUNCTION_NOT_IMPLEMENTED (%d)", rc)
	case C.GRN_DIRECTORY_NOT_EMPTY:
		return fmt.Sprintf("GRN_DIRECTORY_NOT_EMPTY (%d)", rc)
	case C.GRN_ILLEGAL_BYTE_SEQUENCE:
		return fmt.Sprintf("GRN_ILLEGAL_BYTE_SEQUENCE (%d)", rc)
	case C.GRN_SOCKET_NOT_INITIALIZED:
		return fmt.Sprintf("GRN_SOCKET_NOT_INITIALIZED (%d)", rc)
	case C.GRN_OPERATION_WOULD_BLOCK:
		return fmt.Sprintf("GRN_OPERATION_WOULD_BLOCK (%d)", rc)
	case C.GRN_ADDRESS_IS_NOT_AVAILABLE:
		return fmt.Sprintf("GRN_ADDRESS_IS_NOT_AVAILABLE (%d)", rc)
	case C.GRN_NETWORK_IS_DOWN:
		return fmt.Sprintf("GRN_NETWORK_IS_DOWN (%d)", rc)
	case C.GRN_NO_BUFFER:
		return fmt.Sprintf("GRN_NO_BUFFER (%d)", rc)
	case C.GRN_SOCKET_IS_ALREADY_CONNECTED:
		return fmt.Sprintf("GRN_SOCKET_IS_ALREADY_CONNECTED (%d)", rc)
	case C.GRN_SOCKET_IS_NOT_CONNECTED:
		return fmt.Sprintf("GRN_SOCKET_IS_NOT_CONNECTED (%d)", rc)
	case C.GRN_SOCKET_IS_ALREADY_SHUTDOWNED:
		return fmt.Sprintf("GRN_SOCKET_IS_ALREADY_SHUTDOWNED (%d)", rc)
	case C.GRN_OPERATION_TIMEOUT:
		return fmt.Sprintf("GRN_OPERATION_TIMEOUT (%d)", rc)
	case C.GRN_CONNECTION_REFUSED:
		return fmt.Sprintf("GRN_CONNECTION_REFUSED (%d)", rc)
	case C.GRN_RANGE_ERROR:
		return fmt.Sprintf("GRN_RANGE_ERROR (%d)", rc)
	case C.GRN_TOKENIZER_ERROR:
		return fmt.Sprintf("GRN_TOKENIZER_ERROR (%d)", rc)
	case C.GRN_FILE_CORRUPT:
		return fmt.Sprintf("GRN_FILE_CORRUPT (%d)", rc)
	case C.GRN_INVALID_FORMAT:
		return fmt.Sprintf("GRN_INVALID_FORMAT (%d)", rc)
	case C.GRN_OBJECT_CORRUPT:
		return fmt.Sprintf("GRN_OBJECT_CORRUPT (%d)", rc)
	case C.GRN_TOO_MANY_SYMBOLIC_LINKS:
		return fmt.Sprintf("GRN_TOO_MANY_SYMBOLIC_LINKS (%d)", rc)
	case C.GRN_NOT_SOCKET:
		return fmt.Sprintf("GRN_NOT_SOCKET (%d)", rc)
	case C.GRN_OPERATION_NOT_SUPPORTED:
		return fmt.Sprintf("GRN_OPERATION_NOT_SUPPORTED (%d)", rc)
	case C.GRN_ADDRESS_IS_IN_USE:
		return fmt.Sprintf("GRN_ADDRESS_IS_IN_USE (%d)", rc)
	case C.GRN_ZLIB_ERROR:
		return fmt.Sprintf("GRN_ZLIB_ERROR (%d)", rc)
	case C.GRN_LZ4_ERROR:
		return fmt.Sprintf("GRN_LZ4_ERROR (%d)", rc)
	case C.GRN_STACK_OVER_FLOW:
		return fmt.Sprintf("GRN_STACK_OVER_FLOW (%d)", rc)
	case C.GRN_SYNTAX_ERROR:
		return fmt.Sprintf("GRN_SYNTAX_ERROR (%d)", rc)
	case C.GRN_RETRY_MAX:
		return fmt.Sprintf("GRN_RETRY_MAX (%d)", rc)
	case C.GRN_INCOMPATIBLE_FILE_FORMAT:
		return fmt.Sprintf("GRN_INCOMPATIBLE_FILE_FORMAT (%d)", rc)
	case C.GRN_UPDATE_NOT_ALLOWED:
		return fmt.Sprintf("GRN_UPDATE_NOT_ALLOWED (%d)", rc)
	case C.GRN_TOO_SMALL_OFFSET:
		return fmt.Sprintf("GRN_TOO_SMALL_OFFSET (%d)", rc)
	case C.GRN_TOO_LARGE_OFFSET:
		return fmt.Sprintf("GRN_TOO_LARGE_OFFSET (%d)", rc)
	case C.GRN_TOO_SMALL_LIMIT:
		return fmt.Sprintf("GRN_TOO_SMALL_LIMIT (%d)", rc)
	case C.GRN_CAS_ERROR:
		return fmt.Sprintf("GRN_CAS_ERROR (%d)", rc)
	case C.GRN_UNSUPPORTED_COMMAND_VERSION:
		return fmt.Sprintf("GRN_UNSUPPORTED_COMMAND_VERSION (%d)", rc)
	case C.GRN_NORMALIZER_ERROR:
		return fmt.Sprintf("GRN_NORMALIZER_ERROR (%d)", rc)
	case C.GRN_TOKEN_FILTER_ERROR:
		return fmt.Sprintf("GRN_TOKEN_FILTER_ERROR (%d)", rc)
	case C.GRN_COMMAND_ERROR:
		return fmt.Sprintf("GRN_COMMAND_ERROR (%d)", rc)
	case C.GRN_PLUGIN_ERROR:
		return fmt.Sprintf("GRN_PLUGIN_ERROR (%d)", rc)
	case C.GRN_SCORER_ERROR:
		return fmt.Sprintf("GRN_SCORER_ERROR (%d)", rc)
	default:
		return fmt.Sprintf("GRN_UNDEFINED_ERROR (%d)", rc)
	}
}

// newCError returns an error related to a Groonga or Grngo operation.
func newCError(opName string, rc C.grn_rc, db *DB) error {
	if db == nil {
		return fmt.Errorf("%s failed: rc = %s", opName, rcString(rc))
	}
	ctx := db.c.ctx
	if ctx.errbuf[0] == 0 {
		return fmt.Errorf("%s failed: rc = %s, ctx.rc = %s",
			opName, rcString(rc), rcString(ctx.rc))
	}
	return fmt.Errorf("%s failed: rc = %s, ctx.rc = %s, ctx.errbuf = %s",
		opName, rcString(rc), rcString(ctx.rc), C.GoString(&ctx.errbuf[0]))
}

// -- Data types --

// GeoPoint represents a coordinate of latitude and longitude.
type GeoPoint struct {
	Latitude  int32 // Latitude in milliseconds.
	Longitude int32 // Longitude in milliseconds.
}

// NilID is an invalid record ID.
// Some functions return NilID if operations failed.
const NilID = uint32(C.GRN_ID_NIL)

// DataType is an enumeration of Groonga built-in data types.
//
// See http://groonga.org/docs/reference/types.html for details.
type DataType int

// Time (int64) represents the number of microseconds elapsed since the Unix
// epoch.
//
// See http://groonga.org/docs/reference/types.html for details.
const (
	Void          = DataType(C.GRN_DB_VOID)            // N/A.
	Bool          = DataType(C.GRN_DB_BOOL)            // bool.
	Int8          = DataType(C.GRN_DB_INT8)            // int64.
	Int16         = DataType(C.GRN_DB_INT16)           // int64.
	Int32         = DataType(C.GRN_DB_INT32)           // int64.
	Int64         = DataType(C.GRN_DB_INT64)           // int64.
	UInt8         = DataType(C.GRN_DB_UINT8)           // int64.
	UInt16        = DataType(C.GRN_DB_UINT16)          // int64.
	UInt32        = DataType(C.GRN_DB_UINT32)          // int64.
	UInt64        = DataType(C.GRN_DB_UINT64)          // int64.
	Float         = DataType(C.GRN_DB_FLOAT)           // float64.
	Time          = DataType(C.GRN_DB_TIME)            // int64.
	ShortText     = DataType(C.GRN_DB_SHORT_TEXT)      // []byte.
	Text          = DataType(C.GRN_DB_TEXT)            // []byte.
	LongText      = DataType(C.GRN_DB_LONG_TEXT)       // []byte.
	TokyoGeoPoint = DataType(C.GRN_DB_TOKYO_GEO_POINT) // GeoPoint.
	WGS84GeoPoint = DataType(C.GRN_DB_WGS84_GEO_POINT) // GeoPoint.
	LazyInt       = DataType(-iota - 1)                // int64.
	LazyGeoPoint                                       // GeoPoint.
)

func (dataType DataType) String() string {
	switch dataType {
	case Void:
		return "Void"
	case Bool:
		return "Bool"
	case Int8:
		return "Int8"
	case Int16:
		return "Int16"
	case Int32:
		return "Int32"
	case Int64:
		return "Int64"
	case UInt8:
		return "UInt8"
	case UInt16:
		return "UInt16"
	case UInt32:
		return "UInt32"
	case UInt64:
		return "UInt64"
	case Float:
		return "Float"
	case Time:
		return "Time"
	case ShortText:
		return "ShortText"
	case Text:
		return "Text"
	case LongText:
		return "LongText"
	case TokyoGeoPoint:
		return "TokyoGeoPoint"
	case WGS84GeoPoint:
		return "WGS84GeoPoint"
	case LazyInt:
		return "Int"
	case LazyGeoPoint:
		return "GeoPoint"
	default:
		return fmt.Sprintf("DataType(%d)", dataType)
	}
}

// -- TableOptions --

// Flags of TableOptions accepts a combination of these constants.
//
// See http://groonga.org/docs/reference/commands/table_create.html#flags for details.
const (
	TableTypeMask = C.GRN_OBJ_TABLE_TYPE_MASK // TableNoKey | TablePatKey | TableDatKey | TableHashKey.
	TableNoKey    = C.GRN_OBJ_TABLE_NO_KEY    // TableNoKey is associated with TABLE_NO_KEY.
	TablePatKey   = C.GRN_OBJ_TABLE_PAT_KEY   // TablePatKey is associated with TABLE_PAT_KEY.
	TableDatKey   = C.GRN_OBJ_TABLE_DAT_KEY   // TableDatKey is associated with TABLE_DAT_KEY.
	TableHashKey  = C.GRN_OBJ_TABLE_HASH_KEY  // TableHashKey is associated with TABLE_HASH_KEY.
	KeyWithSIS    = C.GRN_OBJ_KEY_WITH_SIS    // KeyWithSIS is associated with KEY_WITH_SIS.
)

// TableOptions is a set of options for CreateTable.
// Flags is TableHashKey by default.
//
// See http://groonga.org/docs/reference/commands/table_create.html#parameters for details.
type TableOptions struct {
	Flags            int      // Flags is associated with flags.
	KeyType          string   // KeyType is associated with key_type.
	ValueType        string   // ValueType is associated with value_type.
	DefaultTokenizer string   // DefaultTokenizer is associated with default_tokenizer.
	Normalizer       string   // Normalizer is associated with normalizer.
	TokenFilters     []string // TokenFilters is associated with token_filters.
}

// NewTableOptions returns a new TableOptions with the default settings.
func NewTableOptions() *TableOptions {
	options := new(TableOptions)
	options.Flags = TableHashKey
	return options
}

// -- ColumnOptions --

// Flags of ColumnOptions accepts a combination of these constants.
//
// See http://groonga.org/docs/reference/commands/column_create.html#parameters for details.
const (
	CompressMask = C.GRN_OBJ_COMPRESS_MASK // CompressZlib | CompressLZ4.
	CompressNone = C.GRN_OBJ_COMPRESS_NONE // CompressNone is 0.
	CompressZlib = C.GRN_OBJ_COMPRESS_ZLIB // CompressZlib is associated with COMPRESS_ZLIB.
	CompressLZ4  = C.GRN_OBJ_COMPRESS_LZ4  // CompressLZ4 is associated with COMPRESS_LZ4.
	WithSection  = C.GRN_OBJ_WITH_SECTION  // WithSection is associated with WITH_SECTION.
	WithWeight   = C.GRN_OBJ_WITH_WEIGHT   // WithWeight is associated with WITH_WEIGHT.
	WithPosition = C.GRN_OBJ_WITH_POSITION // WithPosition is associated with WITH_POSITION.
)

// ColumnOptions is a set of options for CreateColumn.
// Flags is CompressNone by default.
//
// See http://groonga.org/docs/reference/commands/column_create.html#parameters for details.
type ColumnOptions struct {
	Flags int
}

// NewColumnOptions returns a new ColumnOptions with the default settings.
func NewColumnOptions() *ColumnOptions {
	options := new(ColumnOptions)
	options.Flags = CompressNone
	return options
}

// -- Groonga --

// grnInitFinDisabled shows whther C.grn_init and C.grn_fin are disabled.
var grnInitFinDisabled = false

// grnInitCount is an internal counter used in GrnInit and GrnFin.
var grnInitCount = 0

// DisableGrnInitFin disables calls of C.grn_init and C.grn_fin in GrnInit()
// and GrnFin().
// DisableGrnInitFin should be used if you manually or another library
// initialize and finalize Groonga.
func DisableGrnInitFin() {
	grnInitFinDisabled = true
}

// GrnInit increments an internal counter grnInitCount and if it changes from
// 0 to 1, calls C.grn_init to initialize Groonga.
//
// Note that CreateDB and OpenDB call GrnInit, so you should not manually call
// GrnInit if not needed.
func GrnInit() error {
	if grnInitCount == 0 {
		if !grnInitFinDisabled {
			if rc := C.grn_init(); rc != C.GRN_SUCCESS {
				return newCError("grn_init()", rc, nil)
			}
		}
	}
	grnInitCount++
	return nil
}

// GrnFin decrements an internal counter grnInitCount and if it changes from
// 1 to 0, calls C.grn_fin to finalize Groonga.
//
// Note that DB.Close calls GrnFin, so you should not manually call GrnFin if
// not needed.
func GrnFin() error {
	switch grnInitCount {
	case 0:
		return fmt.Errorf("Groonga is not initialized yet")
	case 1:
		if !grnInitFinDisabled {
			if rc := C.grn_fin(); rc != C.GRN_SUCCESS {
				return newCError("grn_fin()", rc, nil)
			}
		}
	}
	grnInitCount--
	return nil
}

// -- DB --

// DB is associated with a Groonga database with its context.
type DB struct {
	c      *C.grngo_db       // The associated C object.
	tables map[string]*Table // A cache to find tables by name.
}

// newDB returns a new DB.
func newDB(c *C.grngo_db) *DB {
	db := new(DB)
	db.c = c
	db.tables = make(map[string]*Table)
	return db
}

// CreateDB creates a Groonga database and returns a new DB associated with it.
// If path is empty, CreateDB creates a temporary database.
//
// Note that CreateDB initializes Groonga if the new DB will be the only one
// and implicit initialization is not disabled.
func CreateDB(path string) (*DB, error) {
	if err := GrnInit(); err != nil {
		return nil, err
	}
	pathBytes := []byte(path)
	var cPath *C.char
	if len(pathBytes) != 0 {
		cPath = (*C.char)(unsafe.Pointer(&pathBytes[0]))
	}
	var c *C.grngo_db
	rc := C.grngo_create_db(cPath, C.size_t(len(pathBytes)), &c)
	if rc != C.GRN_SUCCESS {
		GrnFin()
		return nil, newCError("grngo_create_db()", rc, nil)
	}
	return newDB(c), nil
}

// OpenDB opens an existing Groonga database and returns a new DB associated
// with it.
//
// Note that CreateDB initializes Groonga if the new DB will be the only one
// and implicit initialization is not disabled.
func OpenDB(path string) (*DB, error) {
	if err := GrnInit(); err != nil {
		return nil, err
	}
	pathBytes := []byte(path)
	var cPath *C.char
	if len(pathBytes) != 0 {
		cPath = (*C.char)(unsafe.Pointer(&pathBytes[0]))
	}
	var c *C.grngo_db
	rc := C.grngo_open_db(cPath, C.size_t(len(pathBytes)), &c)
	if rc != C.GRN_SUCCESS {
		GrnFin()
		return nil, newCError("grngo_open_db()", rc, nil)
	}
	return newDB(c), nil
}

// Close finalizes a DB.
func (db *DB) Close() error {
	C.grngo_close_db(db.c)
	return GrnFin()
}

// Refresh clears maps for Table and Column name resolution.
//
// If a table or column is renamed or removed, old maps can cause a name
// resolution error. In such a case, you should use Refresh or reopen the
// Groonga database to resolve it.
func (db *DB) Refresh() error {
	for _, table := range db.tables {
		for _, column := range table.columns {
			C.grngo_close_column(column.c)
		}
		table.columns = make(map[string]*Column)
		C.grngo_close_table(table.c)
	}
	db.tables = make(map[string]*Table)
	return nil
}

// Send executes a Groonga command.
// The command must be well-formed.
//
// See http://groonga.org/docs/reference/command.html for details.
func (db *DB) Send(command string) error {
	command = strings.TrimSpace(command)
	if strings.HasPrefix(command, "table_remove") ||
		strings.HasPrefix(command, "table_rename") ||
		strings.HasPrefix(command, "column_remove") ||
		strings.HasPrefix(command, "column_rename") {
		db.Refresh()
	}
	commandBytes := []byte(command)
	var cCommand *C.char
	if len(commandBytes) != 0 {
		cCommand = (*C.char)(unsafe.Pointer(&commandBytes[0]))
	}
	rc := C.grngo_send(db.c, cCommand, C.size_t(len(commandBytes)))
	if rc != C.GRN_SUCCESS {
		return newCError("grngo_send()", rc, db)
	}
	return nil
}

// SendEx executes a Groonga command with separated options.
//
// See http://groonga.org/docs/reference/command.html for details.
func (db *DB) SendEx(name string, options map[string]string) error {
	if name == "" {
		return fmt.Errorf("invalid command: name = <%s>", name)
	}
	for _, r := range name {
		if (r != '_') && (r < 'a') && (r > 'z') {
			return fmt.Errorf("invalid command: name = <%s>", name)
		}
	}
	commandParts := []string{name}
	for key, value := range options {
		if key == "" {
			return fmt.Errorf("invalid option: key = <%s>", key)
		}
		for _, r := range key {
			if (r != '_') && (r < 'a') && (r > 'z') {
				return fmt.Errorf("invalid option: key = <%s>", key)
			}
		}
		value = strings.Replace(value, "\\", "\\\\", -1)
		value = strings.Replace(value, "'", "\\'", -1)
		commandParts = append(commandParts, fmt.Sprintf("--%s '%s'", key, value))
	}
	return db.Send(strings.Join(commandParts, " "))
}

// Recv returns the result of Groonga commands executed by Send and SendEx.
//
// See http://groonga.org/docs/reference/command.html for details.
func (db *DB) Recv() ([]byte, error) {
	var res *C.char
	var resLen C.uint
	rc := C.grngo_recv(db.c, &res, &resLen)
	if rc != C.GRN_SUCCESS {
		return nil, newCError("grngo_recv()", rc, db)
	}
	return C.GoBytes(unsafe.Pointer(res), C.int(resLen)), nil
}

// Query executes a Groonga command and returns the result.
//
// See http://groonga.org/docs/reference/command.html for details.
func (db *DB) Query(command string) ([]byte, error) {
	if err := db.Send(command); err != nil {
		result, _ := db.Recv()
		return result, err
	}
	return db.Recv()
}

// Query executes a Groonga command with separated options and returns the
// result.
//
// See http://groonga.org/docs/reference/command.html for details.
func (db *DB) QueryEx(name string, options map[string]string) (
	[]byte, error) {
	if err := db.SendEx(name, options); err != nil {
		result, _ := db.Recv()
		return result, err
	}
	return db.Recv()
}

// createTableOptionsMap creates an options map for table_create.
//
// See http://groonga.org/docs/reference/commands/table_create.html#parameters for details.
func (db *DB) createTableOptionsMap(name string, options *TableOptions) (map[string]string, error) {
	optionsMap := make(map[string]string)
	// http://groonga.org/docs/reference/commands/table_create.html#name
	optionsMap["name"] = name
	// http://groonga.org/docs/reference/commands/table_create.html#flags
	if options.KeyType == "" {
		optionsMap["flags"] = "TABLE_NO_KEY"
	} else {
		switch options.Flags & TableTypeMask {
		case TableNoKey:
			optionsMap["flags"] = "TABLE_NO_KEY"
		case TableHashKey:
			optionsMap["flags"] = "TABLE_HASH_KEY"
		case TablePatKey:
			optionsMap["flags"] = "TABLE_PAT_KEY"
		case TableDatKey:
			optionsMap["flags"] = "TABLE_DAT_KEY"
		default:
			return nil, fmt.Errorf("undefined table type: options = %+v", options)
		}
	}
	if (options.Flags & KeyWithSIS) == KeyWithSIS {
		optionsMap["flags"] += "|KEY_WITH_SIS"
	}
	// http://groonga.org/docs/reference/commands/table_create.html#key-type
	switch options.KeyType {
	case "":
	case "Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16",
		"UInt32", "UInt64", "Float", "Time", "ShortText", "TokyoGeoPoint",
		"WGS84GeoPoint":
		optionsMap["key_type"] = options.KeyType
	default:
		if _, err := db.FindTable(options.KeyType); err != nil {
			return nil, fmt.Errorf("invalid key type: options = %+v", options)
		}
		optionsMap["key_type"] = options.KeyType
	}
	// http://groonga.org/docs/reference/commands/table_create.html#value-type
	switch options.ValueType {
	case "":
	case "Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16",
		"UInt32", "UInt64", "Float", "Time", "TokyoGeoPoint", "WGS84GeoPoint":
		optionsMap["value_type"] = options.ValueType
	default:
		if _, err := db.FindTable(options.ValueType); err != nil {
			return nil, fmt.Errorf("invalid value type: options = %+v", options)
		}
		optionsMap["value_type"] = options.ValueType
	}
	// http://groonga.org/docs/reference/commands/table_create.html#default-tokenizer
	if options.DefaultTokenizer != "" {
		optionsMap["default_tokenizer"] = options.DefaultTokenizer
	}
	// http://groonga.org/docs/reference/commands/table_create.html#normalizer
	if options.Normalizer != "" {
		optionsMap["normalizer"] = options.Normalizer
	}
	// http://groonga.org/docs/reference/commands/table_create.html#token-filters
	if len(options.TokenFilters) != 0 {
		optionsMap["token_filters"] = strings.Join(options.TokenFilters, ",")
	}
	return optionsMap, nil
}

// CreateTable creates a Groonga table and returns a new Table associated with
// it.
//
// If options is nil, the default parameters are used.
//
// See http://groonga.org/docs/reference/commands/table_create.html for details.
func (db *DB) CreateTable(name string, options *TableOptions) (*Table, error) {
	if options == nil {
		options = NewTableOptions()
	}
	optionsMap, err := db.createTableOptionsMap(name, options)
	if err != nil {
		return nil, err
	}
	bytes, err := db.QueryEx("table_create", optionsMap)
	if err != nil {
		return nil, err
	}
	if string(bytes) != "true" {
		return nil, fmt.Errorf("table_create failed: name = <%s>", name)
	}
	return db.FindTable(name)
}

// FindTable finds a table.
func (db *DB) FindTable(name string) (*Table, error) {
	if table, ok := db.tables[name]; ok {
		return table, nil
	}
	nameBytes := []byte(name)
	var cName *C.char
	if len(nameBytes) != 0 {
		cName = (*C.char)(unsafe.Pointer(&nameBytes[0]))
	}
	var c *C.grngo_table
	rc := C.grngo_open_table(db.c, cName, C.size_t(len(nameBytes)), &c)
	if rc != C.GRN_SUCCESS {
		return nil, newCError("grngo_find_table()", rc, db)
	}
	table := newTable(db, c, name)
	db.tables[name] = table
	return table, nil
}

// InsertRow finds or inserts a row.
func (db *DB) InsertRow(tableName string, key interface{}) (inserted bool, id uint32, err error) {
	table, err := db.FindTable(tableName)
	if err != nil {
		return false, NilID, err
	}
	return table.InsertRow(key)
}

// CreateColumn creates a Groonga column and returns a new Column associated
// with it.
//
// If valueType starts with "[]", COLUMN_VECTOR is enabled and the rest is used
// as the type parameter.
// If valueType contains a dot ('.'), COLUMN_INDEX is enabled and valueType is
// split by the first dot. Then, the former part is used as the type parameter
// and the latter part is used as the source parameter.
// Otherwise, COLUMN_SCALAR is enabled and valueType is used as the type
// parameter.
//
// If options is nil, the default parameters are used.
//
// See http://groonga.org/docs/reference/commands/column_create.html for details.
func (db *DB) CreateColumn(tableName, columnName string, valueType string, options *ColumnOptions) (*Column, error) {
	table, err := db.FindTable(tableName)
	if err != nil {
		return nil, err
	}
	return table.CreateColumn(columnName, valueType, options)
}

// FindColumn finds a column.
func (db *DB) FindColumn(tableName, columnName string) (*Column, error) {
	table, err := db.FindTable(tableName)
	if err != nil {
		return nil, err
	}
	return table.FindColumn(columnName)
}

// SetValue assigns a value.
func (db *DB) SetValue(tableName, columnName string, id uint32, value interface{}) error {
	table, err := db.FindTable(tableName)
	if err != nil {
		return err
	}
	return table.SetValue(columnName, id, value)
}

// GetValue gets a value.
func (db *DB) GetValue(tableName, columnName string, id uint32) (interface{}, error) {
	table, err := db.FindTable(tableName)
	if err != nil {
		return nil, err
	}
	return table.GetValue(columnName, id)
}

// -- Table --

// Table is associated with a Groonga table.
type Table struct {
	db      *DB                // The owner DB.
	c       *C.grngo_table     // The associated C object.
	name    string             // The table name.
	columns map[string]*Column // A cache to find columns by name.
}

// newTable returns a new Table.
func newTable(db *DB, c *C.grngo_table, name string) *Table {
	var table Table
	table.db = db
	table.c = c
	table.name = name
	table.columns = make(map[string]*Column)
	return &table
}

// InsertRow finds or inserts a row.
func (table *Table) InsertRow(key interface{}) (inserted bool, id uint32, err error) {
	var rc C.grn_rc
	var cInserted C.grn_bool
	var cID C.grn_id
	switch key := key.(type) {
	case nil:
		rc = C.grngo_insert_void(table.c, &cInserted, &cID)
	case bool:
		cKey := C.grn_bool(C.GRN_FALSE)
		if key {
			cKey = C.grn_bool(C.GRN_TRUE)
		}
		rc = C.grngo_insert_bool(table.c, cKey, &cInserted, &cID)
	case int64:
		cKey := C.int64_t(key)
		rc = C.grngo_insert_int(table.c, cKey, &cInserted, &cID)
	case float64:
		cKey := C.double(key)
		rc = C.grngo_insert_float(table.c, cKey, &cInserted, &cID)
	case []byte:
		var cKey C.grngo_text
		if len(key) != 0 {
			cKey.ptr = (*C.char)(unsafe.Pointer(&key[0]))
			cKey.size = C.size_t(len(key))
		}
		rc = C.grngo_insert_text(table.c, cKey, &cInserted, &cID)
	case GeoPoint:
		cKey := C.grn_geo_point{C.int(key.Latitude), C.int(key.Longitude)}
		rc = C.grngo_insert_geo_point(table.c, cKey, &cInserted, &cID)
	default:
		return false, NilID, fmt.Errorf(
			"unsupported key type: typeName = <%s>", reflect.TypeOf(key).Name())
	}
	if rc != C.GRN_SUCCESS {
		return false, NilID, newCError("grngo_insert_*()", rc, table.db)
	}
	return cInserted == C.GRN_TRUE, uint32(cID), nil
}

// SetValue assigns a value.
func (table *Table) SetValue(columnName string, id uint32, value interface{}) error {
	column, err := table.FindColumn(columnName)
	if err != nil {
		return err
	}
	return column.SetValue(id, value)
}

// GetValue gets a value.
func (table *Table) GetValue(columnName string, id uint32) (interface{}, error) {
	column, err := table.FindColumn(columnName)
	if err != nil {
		return nil, err
	}
	return column.GetValue(id)
}

// createColumnOptionsMap creates an options map for column_create.
//
// See http://groonga.org/docs/reference/commands/column_create.html#parameters for details.
func (table *Table) createColumnOptionsMap(name string, valueType string, options *ColumnOptions) (map[string]string, error) {
	optionsMap := make(map[string]string)
	optionsMap["table"] = table.name
	optionsMap["name"] = name
	if strings.HasPrefix(valueType, "[]") {
		valueType = valueType[2:]
		optionsMap["flags"] = "COLUMN_VECTOR"
	} else if delimPos := strings.IndexByte(valueType, '.'); delimPos != -1 {
		optionsMap["source"] = valueType[delimPos+1:]
		valueType = valueType[:delimPos]
		optionsMap["flags"] = "COLUMN_INDEX"
	} else {
		optionsMap["flags"] = "COLUMN_SCALAR"
	}
	switch valueType {
	case "Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16",
		"UInt32", "UInt64", "Float", "Time", "ShortText", "Text", "LongText",
		"TokyoGeoPoint", "WGS84GeoPoint":
		optionsMap["type"] = valueType
	default:
		if _, err := table.db.FindTable(valueType); err != nil {
			return nil, fmt.Errorf("unsupported value type: valueType = %s", valueType)
		}
		optionsMap["type"] = valueType
	}
	switch options.Flags & CompressMask {
	case CompressNone:
	case CompressZlib:
		optionsMap["flags"] += "|COMPRESS_ZLIB"
	case CompressLZ4:
		optionsMap["flags"] += "|COMRESS_LZ4"
	default:
		return nil, fmt.Errorf("undefined compression type: options = %+v", options)
	}
	if (options.Flags & WithSection) == WithSection {
		optionsMap["flags"] += "|WITH_SECTION"
	}
	if (options.Flags & WithWeight) == WithWeight {
		optionsMap["flags"] += "|WITH_WEIGHT"
	}
	if (options.Flags & WithPosition) == WithPosition {
		optionsMap["flags"] += "|WITH_POSITION"
	}
	return optionsMap, nil
}

// CreateColumn creates a Groonga column and returns a new Column associated
// with it.
//
// If valueType starts with "[]", COLUMN_VECTOR is enabled and the rest is used
// as the type parameter.
// If valueType contains a dot ('.'), COLUMN_INDEX is enabled and valueType is
// split by the first dot. Then, the former part is used as the type parameter
// and the latter part is used as the source parameter.
// Otherwise, COLUMN_SCALAR is enabled and valueType is used as the type
// parameter.
//
// If options is nil, the default parameters are used.
//
// See http://groonga.org/docs/reference/commands/column_create.html for details.
func (table *Table) CreateColumn(name string, valueType string, options *ColumnOptions) (*Column, error) {
	if options == nil {
		options = NewColumnOptions()
	}
	optionsMap, err := table.createColumnOptionsMap(name, valueType, options)
	if err != nil {
		return nil, err
	}
	bytes, err := table.db.QueryEx("column_create", optionsMap)
	if err != nil {
		return nil, err
	}
	if string(bytes) != "true" {
		return nil, fmt.Errorf("column_create failed: name = <%s>", name)
	}
	return table.FindColumn(name)
}

// FindColumn finds a column.
func (table *Table) FindColumn(name string) (*Column, error) {
	if column, ok := table.columns[name]; ok {
		return column, nil
	}
	nameBytes := []byte(name)
	var cName *C.char
	if len(nameBytes) != 0 {
		cName = (*C.char)(unsafe.Pointer(&nameBytes[0]))
	}
	var c *C.grngo_column
	rc := C.grngo_open_column(table.c, cName, C.size_t(len(nameBytes)), &c)
	if rc != C.GRN_SUCCESS {
		return nil, newCError("grngo_open_column()", rc, table.db)
	}
	column := newColumn(table, c, name)
	table.columns[name] = column
	return column, nil
}

// -- Column --

// Column is associated with a Groonga column or accessor.
type Column struct {
	table *Table          // The owner table.
	c     *C.grngo_column // The associated C object.
	name  string          // The column name.
}

// newColumn returns a new Column.
func newColumn(table *Table, c *C.grngo_column, name string) *Column {
	var column Column
	column.table = table
	column.c = c
	column.name = name
	return &column
}

// SetValue assigns a value.
func (column *Column) SetValue(id uint32, value interface{}) error {
	var rc C.grn_rc
	cID := C.grn_id(id)
	switch value := value.(type) {
	case bool:
		cValue := C.grn_bool(C.GRN_FALSE)
		if value {
			cValue = C.grn_bool(C.GRN_TRUE)
		}
		rc = C.grngo_set_bool(column.c, cID, cValue)
	case int64:
		cValue := C.int64_t(value)
		rc = C.grngo_set_int(column.c, cID, cValue)
	case float64:
		cValue := C.double(value)
		rc = C.grngo_set_float(column.c, cID, cValue)
	case []byte:
		var cValue C.grngo_text
		if len(value) != 0 {
			cValue.ptr = (*C.char)(unsafe.Pointer(&value[0]))
			cValue.size = C.size_t(len(value))
		}
		rc = C.grngo_set_text(column.c, cID, cValue)
	case GeoPoint:
		cValue := C.grn_geo_point{C.int(value.Latitude), C.int(value.Longitude)}
		rc = C.grngo_set_geo_point(column.c, cID, cValue)
	case []bool:
		vector := make([]C.grn_bool, len(value))
		for i := 0; i < len(value); i++ {
			if value[i] {
				vector[i] = C.grn_bool(C.GRN_TRUE)
			}
		}
		var cValue C.grngo_vector
		if len(vector) != 0 {
			cValue.ptr = unsafe.Pointer(&vector[0])
			cValue.size = C.size_t(len(vector))
		}
		rc = C.grngo_set_bool_vector(column.c, cID, cValue)
	case []int64:
		var cValue C.grngo_vector
		if len(value) != 0 {
			cValue.ptr = unsafe.Pointer(&value[0])
			cValue.size = C.size_t(len(value))
		}
		rc = C.grngo_set_int_vector(column.c, cID, cValue)
	case []float64:
		var cValue C.grngo_vector
		if len(value) != 0 {
			cValue.ptr = unsafe.Pointer(&value[0])
			cValue.size = C.size_t(len(value))
		}
		rc = C.grngo_set_float_vector(column.c, cID, cValue)
	case [][]byte:
		vector := make([]C.grngo_text, len(value))
		for i := 0; i < len(value); i++ {
			if len(value[i]) != 0 {
				vector[i].ptr = (*C.char)(unsafe.Pointer(&value[i][0]))
				vector[i].size = C.size_t(len(value[i]))
			}
		}
		var cValue C.grngo_vector
		if len(vector) != 0 {
			cValue.ptr = unsafe.Pointer(&vector[0])
			cValue.size = C.size_t(len(vector))
		}
		rc = C.grngo_set_text_vector(column.c, cID, cValue)
	case []GeoPoint:
		var cValue C.grngo_vector
		if len(value) != 0 {
			cValue.ptr = unsafe.Pointer(&value[0])
			cValue.size = C.size_t(len(value))
		}
		rc = C.grngo_set_geo_point_vector(column.c, cID, cValue)
	default:
		return fmt.Errorf("unsupported value type: name = <%s>",
			reflect.TypeOf(value).Name())
	}
	if rc != C.GRN_SUCCESS {
		return newCError("grngo_set_*()", rc, column.table.db)
	}
	return nil
}

// parseScalar parses a scalar value.
func (column *Column) parseScalar(ptr unsafe.Pointer) (interface{}, error) {
	switch column.c.value_type {
	case C.GRN_DB_BOOL:
		cValue := *(*C.grn_bool)(ptr)
		return cValue == C.GRN_TRUE, nil
	case C.GRN_DB_INT8:
		return int64(*(*C.int8_t)(ptr)), nil
	case C.GRN_DB_INT16:
		return int64(*(*C.int16_t)(ptr)), nil
	case C.GRN_DB_INT32:
		return int64(*(*C.int32_t)(ptr)), nil
	case C.GRN_DB_INT64:
		return int64(*(*C.int64_t)(ptr)), nil
	case C.GRN_DB_UINT8:
		return int64(*(*C.uint8_t)(ptr)), nil
	case C.GRN_DB_UINT16:
		return int64(*(*C.uint16_t)(ptr)), nil
	case C.GRN_DB_UINT32:
		return int64(*(*C.uint32_t)(ptr)), nil
	case C.GRN_DB_UINT64:
		return int64(*(*C.uint64_t)(ptr)), nil
	case C.GRN_DB_FLOAT:
		return float64(*(*C.double)(ptr)), nil
	case C.GRN_DB_TIME:
		return int64(*(*C.int64_t)(ptr)), nil
	case C.GRN_DB_SHORT_TEXT, C.GRN_DB_TEXT, C.GRN_DB_LONG_TEXT:
		cValue := *(*C.grngo_text)(ptr)
		return C.GoBytes(unsafe.Pointer(cValue.ptr), C.int(cValue.size)), nil
	case C.GRN_DB_TOKYO_GEO_POINT, C.GRN_DB_WGS84_GEO_POINT:
		cValue := *(*C.grn_geo_point)(ptr)
		return GeoPoint{int32(cValue.latitude), int32(cValue.longitude)}, nil
	default:
		return nil, fmt.Errorf("unsupported value type")
	}
}

// parseVector parses a vector value.
func (column *Column) parseVector(ptr unsafe.Pointer) (interface{}, error) {
	cVector := *(*C.grngo_vector)(ptr)
	header := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(cVector.ptr)),
		Len:  int(cVector.size),
		Cap:  int(cVector.size),
	}
	switch column.c.value_type {
	case C.GRN_DB_BOOL:
		cValue := *(*[]C.grn_bool)(unsafe.Pointer(&header))
		value := make([]bool, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i] = cValue[i] == C.GRN_TRUE
		}
		return value, nil
	case C.GRN_DB_INT8:
		cValue := *(*[]C.int8_t)(unsafe.Pointer(&header))
		value := make([]int64, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i] = int64(cValue[i])
		}
		return value, nil
	case C.GRN_DB_INT16:
		cValue := *(*[]C.int16_t)(unsafe.Pointer(&header))
		value := make([]int64, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i] = int64(cValue[i])
		}
		return value, nil
	case C.GRN_DB_INT32:
		cValue := *(*[]C.int32_t)(unsafe.Pointer(&header))
		value := make([]int64, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i] = int64(cValue[i])
		}
		return value, nil
	case C.GRN_DB_INT64:
		cValue := *(*[]C.int64_t)(unsafe.Pointer(&header))
		value := make([]int64, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i] = int64(cValue[i])
		}
		return value, nil
	case C.GRN_DB_UINT8:
		cValue := *(*[]C.uint8_t)(unsafe.Pointer(&header))
		value := make([]int64, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i] = int64(cValue[i])
		}
		return value, nil
	case C.GRN_DB_UINT16:
		cValue := *(*[]C.uint16_t)(unsafe.Pointer(&header))
		value := make([]int64, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i] = int64(cValue[i])
		}
		return value, nil
	case C.GRN_DB_UINT32:
		cValue := *(*[]C.uint32_t)(unsafe.Pointer(&header))
		value := make([]int64, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i] = int64(cValue[i])
		}
		return value, nil
	case C.GRN_DB_UINT64:
		cValue := *(*[]C.uint64_t)(unsafe.Pointer(&header))
		value := make([]int64, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i] = int64(cValue[i])
		}
		return value, nil
	case C.GRN_DB_FLOAT:
		cValue := *(*[]C.double)(unsafe.Pointer(&header))
		value := make([]float64, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i] = float64(cValue[i])
		}
		return value, nil
	case C.GRN_DB_TIME:
		cValue := *(*[]C.int64_t)(unsafe.Pointer(&header))
		value := make([]int64, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i] = int64(cValue[i])
		}
		return value, nil
	case C.GRN_DB_SHORT_TEXT, C.GRN_DB_TEXT, C.GRN_DB_LONG_TEXT:
		cValue := *(*[]C.grngo_text)(unsafe.Pointer(&header))
		value := make([][]byte, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i] = C.GoBytes(unsafe.Pointer(cValue[i].ptr), C.int(cValue[i].size))
		}
		return value, nil
	case C.GRN_DB_TOKYO_GEO_POINT, C.GRN_DB_WGS84_GEO_POINT:
		cValue := *(*[]C.grn_geo_point)(unsafe.Pointer(&header))
		value := make([]GeoPoint, len(cValue))
		for i := 0; i < len(value); i++ {
			value[i].Latitude = int32(cValue[i].latitude)
			value[i].Longitude = int32(cValue[i].longitude)
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported value type")
	}
}

// getValueType() returns a reflect.Type associated with the value type.
func (column *Column) getValueType() (reflect.Type, error) {
	switch column.c.value_type {
	case C.GRN_DB_BOOL:
		var dummy bool
		return reflect.TypeOf(dummy), nil
	case C.GRN_DB_INT8, C.GRN_DB_INT16, C.GRN_DB_INT32, C.GRN_DB_INT64,
		C.GRN_DB_UINT8, C.GRN_DB_UINT16, C.GRN_DB_UINT32, C.GRN_DB_UINT64:
		var dummy int64
		return reflect.TypeOf(dummy), nil
	case C.GRN_DB_FLOAT:
		var dummy float64
		return reflect.TypeOf(dummy), nil
	case C.GRN_DB_TIME:
		var dummy int64
		return reflect.TypeOf(dummy), nil
	case C.GRN_DB_SHORT_TEXT, C.GRN_DB_TEXT, C.GRN_DB_LONG_TEXT:
		var dummy []byte
		return reflect.TypeOf(dummy), nil
	case C.GRN_DB_TOKYO_GEO_POINT, C.GRN_DB_WGS84_GEO_POINT:
		var dummy GeoPoint
		return reflect.TypeOf(dummy), nil
	default:
		return nil, fmt.Errorf("unknown data type")
	}
}

// parseDeepVectorIn recursively parses a deep vector
// ((column.c.dimension - depth) >= 2).
func (column *Column) traverse(typ reflect.Type, depth int, ptr unsafe.Pointer) (reflect.Value, error) {
	dimension := int(column.c.dimension)
	if (depth == (dimension - 1)) {
		value, err := column.parseVector(ptr)
		if err != nil {
			return reflect.Zero(reflect.SliceOf(typ)), err
		}
		return reflect.ValueOf(value), nil
	}
	sType := typ
	for i := depth; i < int(column.c.dimension); i++ {
		sType = reflect.SliceOf(sType)
	}
	vector := *(*C.grngo_vector)(ptr)
	header := reflect.SliceHeader{
		Data: uintptr(vector.ptr),
		Len:  int(vector.size),
		Cap:  int(vector.size),
	}
	cValue := *(*[]C.grngo_vector)(unsafe.Pointer(&header))
	value := reflect.MakeSlice(sType, 0, len(cValue))
	for i := 0; i < len(cValue); i++ {
		newValue, err := column.traverse(typ, depth + 1, unsafe.Pointer(&cValue[i]))
		if err != nil {
			return reflect.Zero(sType), err
		}
		value = reflect.Append(value, newValue)
	}
	return value, nil
}

// parseDeepVector parses a deep vector (column.c.dimension >= 2).
func (column *Column) parseDeepVector(ptr unsafe.Pointer) (interface{}, error) {
	valueType, err := column.getValueType()
	if err != nil {
		return nil, err
	}
	value, err := column.traverse(valueType, 0, ptr)
	if err != nil {
		return nil, err
	}
	return value.Interface(), nil
}

// GetValue gets a value.
func (column *Column) GetValue(id uint32) (interface{}, error) {
	var ptr unsafe.Pointer
	rc := C.grngo_get(column.c, C.grn_id(id), &ptr)
	if rc != C.GRN_SUCCESS {
		return nil, newCError("grngo_get()", rc, column.table.db)
	}
	switch column.c.dimension {
	case 0:
		return column.parseScalar(ptr)
	case 1:
		return column.parseVector(ptr)
	default:
		return column.parseDeepVector(ptr)
	}
}
