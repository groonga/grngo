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

func (rc C.grn_rc) String() string {
	switch rc {
	case C.GRN_SUCCESS:
		return "GRN_SUCCESS"
	case C.GRN_END_OF_DATA:
		return "GRN_END_OF_DATA"
	case C.GRN_UNKNOWN_ERROR:
		return "GRN_UNKNOWN_ERROR"
	case C.GRN_OPERATION_NOT_PERMITTED:
		return "GRN_OPERATION_NOT_PERMITTED"
	case C.GRN_NO_SUCH_FILE_OR_DIRECTORY:
		return "GRN_NO_SUCH_FILE_OR_DIRECTORY"
	case C.GRN_NO_SUCH_PROCESS:
		return "GRN_NO_SUCH_PROCESS"
	case C.GRN_INTERRUPTED_FUNCTION_CALL:
		return "GRN_INTERRUPTED_FUNCTION_CALL"
	case C.GRN_INPUT_OUTPUT_ERROR:
		return "GRN_INPUT_OUTPUT_ERROR"
	case C.GRN_NO_SUCH_DEVICE_OR_ADDRESS:
		return "GRN_NO_SUCH_DEVICE_OR_ADDRESS"
	case C.GRN_ARG_LIST_TOO_LONG:
		return "GRN_ARG_LIST_TOO_LONG"
	case C.GRN_EXEC_FORMAT_ERROR:
		return "GRN_EXEC_FORMAT_ERROR"
	case C.GRN_BAD_FILE_DESCRIPTOR:
		return "GRN_BAD_FILE_DESCRIPTOR"
	case C.GRN_NO_CHILD_PROCESSES:
		return "GRN_NO_CHILD_PROCESSES"
	case C.GRN_RESOURCE_TEMPORARILY_UNAVAILABLE:
		return "GRN_RESOURCE_TEMPORARILY_UNAVAILABLE"
	case C.GRN_NOT_ENOUGH_SPACE:
		return "GRN_NOT_ENOUGH_SPACE"
	case C.GRN_PERMISSION_DENIED:
		return "GRN_PERMISSION_DENIED"
	case C.GRN_BAD_ADDRESS:
		return "GRN_BAD_ADDRESS"
	case C.GRN_RESOURCE_BUSY:
		return "GRN_RESOURCE_BUSY"
	case C.GRN_FILE_EXISTS:
		return "GRN_FILE_EXISTS"
	case C.GRN_IMPROPER_LINK:
		return "GRN_IMPROPER_LINK"
	case C.GRN_NO_SUCH_DEVICE:
		return "GRN_NO_SUCH_DEVICE"
	case C.GRN_NOT_A_DIRECTORY:
		return "GRN_NOT_A_DIRECTORY"
	case C.GRN_IS_A_DIRECTORY:
		return "GRN_IS_A_DIRECTORY"
	case C.GRN_INVALID_ARGUMENT:
		return "GRN_INVALID_ARGUMENT"
	case C.GRN_TOO_MANY_OPEN_FILES_IN_SYSTEM:
		return "GRN_TOO_MANY_OPEN_FILES_IN_SYSTEM"
	case C.GRN_TOO_MANY_OPEN_FILES:
		return "GRN_TOO_MANY_OPEN_FILES"
	case C.GRN_INAPPROPRIATE_I_O_CONTROL_OPERATION:
		return "GRN_INAPPROPRIATE_I_O_CONTROL_OPERATION"
	case C.GRN_FILE_TOO_LARGE:
		return "GRN_FILE_TOO_LARGE"
	case C.GRN_NO_SPACE_LEFT_ON_DEVICE:
		return "GRN_NO_SPACE_LEFT_ON_DEVICE"
	case C.GRN_INVALID_SEEK:
		return "GRN_INVALID_SEEK"
	case C.GRN_READ_ONLY_FILE_SYSTEM:
		return "GRN_READ_ONLY_FILE_SYSTEM"
	case C.GRN_TOO_MANY_LINKS:
		return "GRN_TOO_MANY_LINKS"
	case C.GRN_BROKEN_PIPE:
		return "GRN_BROKEN_PIPE"
	case C.GRN_DOMAIN_ERROR:
		return "GRN_DOMAIN_ERROR"
	case C.GRN_RESULT_TOO_LARGE:
		return "GRN_RESULT_TOO_LARGE"
	case C.GRN_RESOURCE_DEADLOCK_AVOIDED:
		return "GRN_RESOURCE_DEADLOCK_AVOIDED"
	case C.GRN_NO_MEMORY_AVAILABLE:
		return "GRN_NO_MEMORY_AVAILABLE"
	case C.GRN_FILENAME_TOO_LONG:
		return "GRN_FILENAME_TOO_LONG"
	case C.GRN_NO_LOCKS_AVAILABLE:
		return "GRN_NO_LOCKS_AVAILABLE"
	case C.GRN_FUNCTION_NOT_IMPLEMENTED:
		return "GRN_FUNCTION_NOT_IMPLEMENTED"
	case C.GRN_DIRECTORY_NOT_EMPTY:
		return "GRN_DIRECTORY_NOT_EMPTY"
	case C.GRN_ILLEGAL_BYTE_SEQUENCE:
		return "GRN_ILLEGAL_BYTE_SEQUENCE"
	case C.GRN_SOCKET_NOT_INITIALIZED:
		return "GRN_SOCKET_NOT_INITIALIZED"
	case C.GRN_OPERATION_WOULD_BLOCK:
		return "GRN_OPERATION_WOULD_BLOCK"
	case C.GRN_ADDRESS_IS_NOT_AVAILABLE:
		return "GRN_ADDRESS_IS_NOT_AVAILABLE"
	case C.GRN_NETWORK_IS_DOWN:
		return "GRN_NETWORK_IS_DOWN"
	case C.GRN_NO_BUFFER:
		return "GRN_NO_BUFFER"
	case C.GRN_SOCKET_IS_ALREADY_CONNECTED:
		return "GRN_SOCKET_IS_ALREADY_CONNECTED"
	case C.GRN_SOCKET_IS_NOT_CONNECTED:
		return "GRN_SOCKET_IS_NOT_CONNECTED"
	case C.GRN_SOCKET_IS_ALREADY_SHUTDOWNED:
		return "GRN_SOCKET_IS_ALREADY_SHUTDOWNED"
	case C.GRN_OPERATION_TIMEOUT:
		return "GRN_OPERATION_TIMEOUT"
	case C.GRN_CONNECTION_REFUSED:
		return "GRN_CONNECTION_REFUSED"
	case C.GRN_RANGE_ERROR:
		return "GRN_RANGE_ERROR"
	case C.GRN_TOKENIZER_ERROR:
		return "GRN_TOKENIZER_ERROR"
	case C.GRN_FILE_CORRUPT:
		return "GRN_FILE_CORRUPT"
	case C.GRN_INVALID_FORMAT:
		return "GRN_INVALID_FORMAT"
	case C.GRN_OBJECT_CORRUPT:
		return "GRN_OBJECT_CORRUPT"
	case C.GRN_TOO_MANY_SYMBOLIC_LINKS:
		return "GRN_TOO_MANY_SYMBOLIC_LINKS"
	case C.GRN_NOT_SOCKET:
		return "GRN_NOT_SOCKET"
	case C.GRN_OPERATION_NOT_SUPPORTED:
		return "GRN_OPERATION_NOT_SUPPORTED"
	case C.GRN_ADDRESS_IS_IN_USE:
		return "GRN_ADDRESS_IS_IN_USE"
	case C.GRN_ZLIB_ERROR:
		return "GRN_ZLIB_ERROR"
	case C.GRN_LZ4_ERROR:
		return "GRN_LZ4_ERROR"
	case C.GRN_STACK_OVER_FLOW:
		return "GRN_STACK_OVER_FLOW"
	case C.GRN_SYNTAX_ERROR:
		return "GRN_SYNTAX_ERROR"
	case C.GRN_RETRY_MAX:
		return "GRN_RETRY_MAX"
	case C.GRN_INCOMPATIBLE_FILE_FORMAT:
		return "GRN_INCOMPATIBLE_FILE_FORMAT"
	case C.GRN_UPDATE_NOT_ALLOWED:
		return "GRN_UPDATE_NOT_ALLOWED"
	case C.GRN_TOO_SMALL_OFFSET:
		return "GRN_TOO_SMALL_OFFSET"
	case C.GRN_TOO_LARGE_OFFSET:
		return "GRN_TOO_LARGE_OFFSET"
	case C.GRN_TOO_SMALL_LIMIT:
		return "GRN_TOO_SMALL_LIMIT"
	case C.GRN_CAS_ERROR:
		return "GRN_CAS_ERROR"
	case C.GRN_UNSUPPORTED_COMMAND_VERSION:
		return "GRN_UNSUPPORTED_COMMAND_VERSION"
	case C.GRN_NORMALIZER_ERROR:
		return "GRN_NORMALIZER_ERROR"
	case C.GRN_TOKEN_FILTER_ERROR:
		return "GRN_TOKEN_FILTER_ERROR"
	case C.GRN_COMMAND_ERROR:
		return "GRN_COMMAND_ERROR"
	case C.GRN_PLUGIN_ERROR:
		return "GRN_PLUGIN_ERROR"
	case C.GRN_SCORER_ERROR:
		return "GRN_SCORER_ERROR"
	default:
		return "N/A"
	}
}

// newGrnError returns an error related to a Groonga or Grngo operation.
func newGrnError(opName string, rc *C.grn_rc, ctx *C.grn_ctx) error {
	switch {
	case rc == nil:
		if ctx == nil {
			return fmt.Errorf("%s failed", opName)
		}
		if ctx.rc == C.GRN_SUCCESS {
			return fmt.Errorf("%s failed: ctx.rc = %s (%d)", opName, ctx.rc, ctx.rc)
		}
		msg := C.GoString(&ctx.errbuf[0])
		return fmt.Errorf("%s failed: ctx.rc = %s (%d), ctx.errbuf = %s",
			opName, ctx.rc, ctx.rc, msg)
	case ctx == nil:
		return fmt.Errorf("%s failed: rc = %s (%d)", opName, *rc, *rc)
	case ctx.rc == C.GRN_SUCCESS:
		return fmt.Errorf("%s failed: rc = %s (%d), ctx.rc = %s (%d)",
			opName, *rc, *rc, ctx.rc, ctx.rc)
	default:
		msg := C.GoString(&ctx.errbuf[0])
		return fmt.Errorf("%s failed: rc = %s (%d), ctx.rc = %s (%d), ctx.errbuf = %s",
			opName, *rc, *rc, ctx.rc, ctx.rc, msg)
	}
}

// newInvalidKeyTypeError returns an error for data type conflict.
func newInvalidKeyTypeError(expected, actual DataType) error {
	return fmt.Errorf("invalid data type: expected = %s, actual = %s", expected, actual)
}

// newInvalidValueTypeError returns an error for data type conflict.
func newInvalidValueTypeError(expectedDataType DataType, expectedIsVector bool, actualDataType DataType, actualIsVector bool) error {
	expected := expectedDataType.String()
	if expectedIsVector {
		expected = "[]" + expected
	}
	actual := actualDataType.String()
	if actualIsVector {
		actual = "[]" + actual
	}
	return fmt.Errorf("invalid data type: expected = %s, actual = %s", expected, actual)
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
				return newGrnError("grn_init()", &rc, nil)
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
				return newGrnError("grn_fin()", &rc, nil)
			}
		}
	}
	grnInitCount--
	return nil
}

// openGrnCtx returns a new grn_ctx.
func openGrnCtx() (*C.grn_ctx, error) {
	if err := GrnInit(); err != nil {
		return nil, err
	}
	ctx := C.grn_ctx_open(0)
	if ctx == nil {
		GrnFin()
		return nil, newGrnError("grn_ctx_open()", nil, nil)
	}
	return ctx, nil
}

// closeGrnCtx finalizes a grn_ctx.
func closeGrnCtx(ctx *C.grn_ctx) error {
	rc := C.grn_ctx_close(ctx)
	GrnFin()
	if rc != C.GRN_SUCCESS {
		return newGrnError("grn_ctx_close()", &rc, nil)
	}
	return nil
}

// -- DB --

// DB is associated with a Groonga database with its context.
type DB struct {
	ctx    *C.grn_ctx        // The associated grn_ctx.
	obj    *C.grn_obj        // The associated database.
	tables map[string]*Table // A cache to find tables by name.
}

// newDB returns a new DB.
func newDB(ctx *C.grn_ctx, obj *C.grn_obj) *DB {
	db := new(DB)
	db.ctx = ctx
	db.obj = obj
	db.tables = make(map[string]*Table)
	return db
}

// CreateDB creates a Groonga database and returns a new DB associated with it.
// If path is empty, CreateDB creates a temporary database.
//
// Note that CreateDB initializes Groonga if the new DB will be the only one
// and implicit initialization is not disabled.
func CreateDB(path string) (*DB, error) {
	ctx, err := openGrnCtx()
	if err != nil {
		return nil, err
	}
	var cPath *C.char
	if path != "" {
		cPath = C.CString(path)
		defer C.free(unsafe.Pointer(cPath))
	}
	obj := C.grn_db_create(ctx, cPath, nil)
	if obj == nil {
		defer closeGrnCtx(ctx)
		return nil, newGrnError("grn_db_create()", nil, ctx)
	}
	return newDB(ctx, obj), nil
}

// OpenDB opens an existing Groonga database and returns a new DB associated
// with it.
//
// Note that CreateDB initializes Groonga if the new DB will be the only one
// and implicit initialization is not disabled.
func OpenDB(path string) (*DB, error) {
	ctx, err := openGrnCtx()
	if err != nil {
		return nil, err
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	obj := C.grn_db_open(ctx, cPath)
	if obj == nil {
		defer closeGrnCtx(ctx)
		return nil, newGrnError("grn_db_open()", nil, ctx)
	}
	return newDB(ctx, obj), nil
}

// Close finalizes a DB.
func (db *DB) Close() error {
	rc := C.grn_obj_close(db.ctx, db.obj)
	if rc != C.GRN_SUCCESS {
		defer closeGrnCtx(db.ctx)
		return newGrnError("grn_obj_close()", &rc, db.ctx)
	}
	return closeGrnCtx(db.ctx)
}

// Refresh clears maps for Table and Column name resolution.
//
// If a table or column is renamed or removed, the maps may cause a name
// resolution error. In such a case, you should use Refresh or reopen the
// Groonga database to resolve it.
func (db *DB) Refresh() error {
	for _, table := range db.tables {
		nameBytes := []byte(table.name)
		cName := (*C.char)(unsafe.Pointer(&nameBytes[0]))
		var tableObj *C.grn_obj
		C.grngo_find_table(db.ctx, cName, C.size_t(len(nameBytes)), &tableObj)
		if tableObj != table.obj {
			continue
		}
		for _, column := range table.columns {
			nameBytes := []byte(column.name)
			cName := (*C.char)(unsafe.Pointer(&nameBytes[0]))
			columnObj := C.grn_obj_column(db.ctx, table.obj, cName, C.uint(len(nameBytes)))
			if columnObj == column.obj {
				C.grn_obj_unlink(db.ctx, column.obj)
			}
		}
		C.grn_obj_unlink(db.ctx, table.obj)
	}
	db.tables = make(map[string]*Table)
	return nil
}

// Send executes a Groonga command.
// The command must be well-formed.
//
// See http://groonga.org/docs/reference/command.html for details.
func (db *DB) Send(command string) error {
	commandBytes := []byte(command)
	var cCommand *C.char
	if len(commandBytes) != 0 {
		cCommand = (*C.char)(unsafe.Pointer(&commandBytes[0]))
	}
	C.grn_ctx_send(db.ctx, cCommand, C.uint(len(commandBytes)), 0)
	if db.ctx.rc != C.GRN_SUCCESS {
		return newGrnError("grn_ctx_send()", nil, db.ctx)
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
	var resultBuffer *C.char
	var resultLength C.uint
	var flags C.int
	C.grn_ctx_recv(db.ctx, &resultBuffer, &resultLength, &flags)
	if db.ctx.rc != C.GRN_SUCCESS {
		return nil, newGrnError("grn_ctx_recv()", nil, db.ctx)
	}
	result := C.GoBytes(unsafe.Pointer(resultBuffer), C.int(resultLength))
	return result, nil
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
	var obj *C.grn_obj
	rc := C.grngo_find_table(db.ctx, cName, C.size_t(len(nameBytes)), &obj)
	if rc != C.GRN_SUCCESS {
		return nil, newGrnError("grngo_find_table()", &rc, db.ctx)
	}
	var keyInfo C.grngo_table_type_info
	rc = C.grngo_table_get_key_info(db.ctx, obj, &keyInfo)
	if rc != C.GRN_SUCCESS {
		return nil, newGrnError("grngo_table_get_key_info()", &rc, db.ctx)
	}
	// Check the key type.
	keyType := DataType(keyInfo.data_type)
	// Find the destination table if the key is table reference.
	var keyTable *Table
	if keyInfo.ref_table != nil {
		defer C.grn_obj_unlink(db.ctx, keyInfo.ref_table)
		var cKeyTableName *C.char
		rc := C.grngo_table_get_name(db.ctx, keyInfo.ref_table, &cKeyTableName)
		if rc != C.GRN_SUCCESS {
			return nil, newGrnError("grngo_table_get_name()", &rc, db.ctx)
		}
		defer C.free(unsafe.Pointer(cKeyTableName))
		var err error
		keyTable, err = db.FindTable(C.GoString(cKeyTableName))
		if err != nil {
			return nil, err
		}
		finalTable := keyTable
		for finalTable.keyTable != nil {
			finalTable = finalTable.keyTable
		}
		keyType = finalTable.keyType
	}
	var valueInfo C.grngo_table_type_info
	rc = C.grngo_table_get_value_info(db.ctx, obj, &valueInfo)
	if rc != C.GRN_SUCCESS {
		return nil, newGrnError("grngo_table_get_value_info()", &rc, db.ctx)
	}
	// Check the value type.
	valueType := DataType(valueInfo.data_type)
	// Find the destination table if the value is table reference.
	var valueTable *Table
	if valueInfo.ref_table != nil {
		defer C.grn_obj_unlink(db.ctx, valueInfo.ref_table)
		var cValueTableName *C.char
		rc := C.grngo_table_get_name(db.ctx, valueInfo.ref_table, &cValueTableName)
		if rc != C.GRN_SUCCESS {
			return nil, newGrnError("grngo_table_get_name()", &rc, db.ctx)
		}
		defer C.free(unsafe.Pointer(cValueTableName))
		var err error
		valueTable, err = db.FindTable(C.GoString(cValueTableName))
		if err != nil {
			return nil, err
		}
		finalTable := valueTable
		for finalTable.keyTable != nil {
			finalTable = finalTable.keyTable
		}
		valueType = finalTable.keyType
	}
	table := newTable(db, obj, name, keyType, keyTable, valueType, valueTable)
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
	db         *DB                // The owner DB.
	obj        *C.grn_obj         // The associated table.
	name       string             // The table name.
	keyType    DataType           // The built-in data type of keys.
	keyTable   *Table             // Keys' reference table or nil if not available.
	valueType  DataType           // The built-in data type of values.
	valueTable *Table             // Values' reference table or nil if not available.
	columns    map[string]*Column // A cache to find columns by name.
}

// newTable returns a new Table.
func newTable(db *DB, obj *C.grn_obj, name string, keyType DataType, keyTable *Table, valueType DataType, valueTable *Table) *Table {
	var table Table
	table.db = db
	table.obj = obj
	table.name = name
	table.keyType = keyType
	table.keyTable = keyTable
	table.valueType = valueType
	table.valueTable = valueTable
	table.columns = make(map[string]*Column)
	return &table
}

// insertVoid inserts an empty row.
func (table *Table) insertVoid() (bool, uint32, error) {
	if table.keyType != Void {
		return false, NilID, newInvalidKeyTypeError(table.keyType, Void)
	}
	rowInfo := C.grngo_table_insert_void(table.db.ctx, table.obj)
	if rowInfo.id == C.GRN_ID_NIL {
		return false, NilID, fmt.Errorf("grngo_table_insert_void() failed")
	}
	return rowInfo.inserted == C.GRN_TRUE, uint32(rowInfo.id), nil
}

// insertBool inserts a row with Bool key.
func (table *Table) insertBool(key bool) (bool, uint32, error) {
	if table.keyType != Bool {
		return false, NilID, newInvalidKeyTypeError(table.keyType, Bool)
	}
	grnKey := C.grn_bool(C.GRN_FALSE)
	if key {
		grnKey = C.grn_bool(C.GRN_TRUE)
	}
	rowInfo := C.grngo_table_insert_bool(table.db.ctx, table.obj, grnKey)
	if rowInfo.id == C.GRN_ID_NIL {
		return false, NilID, fmt.Errorf("grngo_table_insert_bool() failed")
	}
	return rowInfo.inserted == C.GRN_TRUE, uint32(rowInfo.id), nil
}

// insertInt inserts a row with Int key.
func (table *Table) insertInt(key int64) (bool, uint32, error) {
	var rowInfo C.grngo_row_info
	switch table.keyType {
	case Int8:
		grnKey := C.int8_t(key)
		rowInfo = C.grngo_table_insert_int8(table.db.ctx, table.obj, grnKey)
	case Int16:
		grnKey := C.int16_t(key)
		rowInfo = C.grngo_table_insert_int16(table.db.ctx, table.obj, grnKey)
	case Int32:
		grnKey := C.int32_t(key)
		rowInfo = C.grngo_table_insert_int32(table.db.ctx, table.obj, grnKey)
	case Int64:
		grnKey := C.int64_t(key)
		rowInfo = C.grngo_table_insert_int64(table.db.ctx, table.obj, grnKey)
	case UInt8:
		grnKey := C.uint8_t(key)
		rowInfo = C.grngo_table_insert_uint8(table.db.ctx, table.obj, grnKey)
	case UInt16:
		grnKey := C.uint16_t(key)
		rowInfo = C.grngo_table_insert_uint16(table.db.ctx, table.obj, grnKey)
	case UInt32:
		grnKey := C.uint32_t(key)
		rowInfo = C.grngo_table_insert_uint32(table.db.ctx, table.obj, grnKey)
	case UInt64:
		grnKey := C.uint64_t(key)
		rowInfo = C.grngo_table_insert_uint64(table.db.ctx, table.obj, grnKey)
	case Time:
		grnKey := C.int64_t(key)
		rowInfo = C.grngo_table_insert_time(table.db.ctx, table.obj, grnKey)
	default:
		return false, NilID, newInvalidKeyTypeError(table.keyType, LazyInt)
	}
	if rowInfo.id == C.GRN_ID_NIL {
		return false, NilID, fmt.Errorf("grngo_table_insert_int*() failed")
	}
	return rowInfo.inserted == C.GRN_TRUE, uint32(rowInfo.id), nil
}

// insertFloat inserts a row with Float key.
func (table *Table) insertFloat(key float64) (bool, uint32, error) {
	if table.keyType != Float {
		return false, NilID, newInvalidKeyTypeError(table.keyType, Float)
	}
	grnKey := C.double(key)
	rowInfo := C.grngo_table_insert_float(table.db.ctx, table.obj, grnKey)
	if rowInfo.id == C.GRN_ID_NIL {
		return false, NilID, fmt.Errorf("grngo_table_insert_float() failed")
	}
	return rowInfo.inserted == C.GRN_TRUE, uint32(rowInfo.id), nil
}

// insertText inserts a row with Text key.
func (table *Table) insertText(key []byte) (bool, uint32, error) {
	if table.keyType != ShortText {
		return false, NilID, newInvalidKeyTypeError(table.keyType, Text)
	}
	var grnKey C.grngo_text
	if len(key) != 0 {
		grnKey.ptr = (*C.char)(unsafe.Pointer(&key[0]))
		grnKey.size = C.size_t(len(key))
	}
	rowInfo := C.grngo_table_insert_text(table.db.ctx, table.obj, &grnKey)
	if rowInfo.id == C.GRN_ID_NIL {
		return false, NilID, fmt.Errorf("grngo_table_insert_text() failed")
	}
	return rowInfo.inserted == C.GRN_TRUE, uint32(rowInfo.id), nil
}

// insertGeoPoint inserts a row with GeoPoint key.
func (table *Table) insertGeoPoint(key GeoPoint) (bool, uint32, error) {
	switch table.keyType {
	case TokyoGeoPoint, WGS84GeoPoint:
	default:
		return false, NilID, newInvalidKeyTypeError(table.keyType, LazyGeoPoint)
	}
	grnKey := C.grn_geo_point{C.int(key.Latitude), C.int(key.Longitude)}
	rowInfo := C.grngo_table_insert_geo_point(table.db.ctx, table.obj, grnKey)
	if rowInfo.id == C.GRN_ID_NIL {
		return false, NilID, fmt.Errorf("grngo_table_insert_geo_point() failed")
	}
	return rowInfo.inserted == C.GRN_TRUE, uint32(rowInfo.id), nil
}

// InsertRow finds or inserts a row.
func (table *Table) InsertRow(key interface{}) (inserted bool, id uint32, err error) {
	switch value := key.(type) {
	case nil:
		return table.insertVoid()
	case bool:
		return table.insertBool(value)
	case int64:
		return table.insertInt(value)
	case float64:
		return table.insertFloat(value)
	case []byte:
		return table.insertText(value)
	case GeoPoint:
		return table.insertGeoPoint(value)
	default:
		return false, NilID, fmt.Errorf(
			"unsupported key type: typeName = <%s>", reflect.TypeOf(key).Name())
	}
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

// findColumn finds a column.
func (table *Table) findColumn(name string) (*Column, error) {
	if column, ok := table.columns[name]; ok {
		return column, nil
	}
	nameBytes := []byte(name)
	var cName *C.char
	if len(nameBytes) != 0 {
		cName = (*C.char)(unsafe.Pointer(&nameBytes[0]))
	}
	obj := C.grn_obj_column(table.db.ctx, table.obj, cName, C.uint(len(name)))
	if obj == nil {
		return nil, newGrnError("grn_obj_column()", nil, table.db.ctx)
	}
	var valueType DataType
	var valueTable *Table
	var isVector bool
	switch name {
	case "_id":
		valueType = UInt32
	case "_key":
		valueType = table.keyType
		valueTable = table.keyTable
	case "_value":
		valueType = table.valueType
		valueTable = table.valueTable
	default:
		var valueInfo C.grngo_type_info
		if ok := C.grngo_column_get_value_info(table.db.ctx, obj, &valueInfo); ok != C.GRN_TRUE {
			return nil, fmt.Errorf("grngo_column_get_value_info() failed: name = <%s>",
				name)
		}
		// Check the value type.
		valueType = DataType(valueInfo.data_type)
		isVector = valueInfo.dimension > 0
		// Find the destination table if the value is table reference.
		if valueInfo.ref_table != nil {
			if valueType == Void {
				return nil, fmt.Errorf("reference to void: name = <%s>", name)
			}
			var cValueTableName *C.char
			rc := C.grngo_table_get_name(table.db.ctx, valueInfo.ref_table, &cValueTableName)
			if rc != C.GRN_SUCCESS {
				return nil, newGrnError("grngo_table_get_name()", &rc, table.db.ctx)
			}
			defer C.free(unsafe.Pointer(cValueTableName))
			var err error
			valueTable, err = table.db.FindTable(C.GoString(cValueTableName))
			if err != nil {
				return nil, err
			}
		}
	}
	column := newColumn(table, obj, name, valueType, isVector, valueTable)
	table.columns[name] = column
	return column, nil
}

// FindColumn finds a column.
func (table *Table) FindColumn(name string) (*Column, error) {
	if column, ok := table.columns[name]; ok {
		return column, nil
	}
	delimPos := strings.IndexByte(name, '.')
	if delimPos == -1 {
		return table.findColumn(name)
	}
	columnNames := strings.Split(name, ".")
	column, err := table.findColumn(columnNames[0])
	if err != nil {
		return nil, err
	}
	isVector := column.isVector
	valueTable := column.valueTable
	for _, columnName := range columnNames[1:] {
		if column.valueTable == nil {
			return nil, fmt.Errorf("not table reference: column.name = <%s>", column.name)
		}
		column, err = column.valueTable.findColumn(columnName)
		if err != nil {
			return nil, err
		}
		if column.isVector {
			if isVector {
				return nil, fmt.Errorf("vector of vector is not supported")
			}
			isVector = true
		}
	}
	nameBytes := []byte(name)
	var cName *C.char
	if len(nameBytes) != 0 {
		cName = (*C.char)(unsafe.Pointer(&nameBytes[0]))
	}
	obj := C.grn_obj_column(table.db.ctx, table.obj, cName, C.uint(len(name)))
	if obj == nil {
		return nil, fmt.Errorf("grn_obj_column() failed: name = <%s>", name)
	}
	column = newColumn(table, obj, name, column.valueType, isVector, valueTable)
	table.columns[name] = column
	return column, nil
}

// -- Column --

// Column is associated with a Groonga column or accessor.
type Column struct {
	table      *Table     // The owner table.
	obj        *C.grn_obj // The associated column or accessor.
	name       string     // The column name.
	valueType  DataType   // The built-in data type of values.
	isVector   bool       // Whether values are vector or not.
	valueTable *Table     // The reference table or nil if not available.
}

// newColumn returns a new Column.
func newColumn(table *Table, obj *C.grn_obj, name string, valueType DataType, isVector bool, valueTable *Table) *Column {
	var column Column
	column.table = table
	column.obj = obj
	column.name = name
	column.valueType = valueType
	column.isVector = isVector
	column.valueTable = valueTable
	return &column
}

// setBool assigns a Bool value.
func (column *Column) setBool(id uint32, value bool) error {
	if (column.valueType != Bool) || column.isVector {
		return newInvalidValueTypeError(column.valueType, column.isVector, Bool, false)
	}
	var grnValue C.grn_bool = C.GRN_FALSE
	if value {
		grnValue = C.GRN_TRUE
	}
	if ok := C.grngo_column_set_bool(column.table.db.ctx, column.obj,
		C.grn_id(id), grnValue); ok != C.GRN_TRUE {
		return fmt.Errorf("grngo_column_set_bool() failed")
	}
	return nil
}

// setInt assigns an Int value.
func (column *Column) setInt(id uint32, value int64) error {
	if column.isVector {
		return newInvalidValueTypeError(column.valueType, column.isVector, LazyInt, false)
	}
	ctx := column.table.db.ctx
	var ok C.grn_bool
	switch column.valueType {
	case Int8:
		grnValue := C.int8_t(value)
		ok = C.grngo_column_set_int8(ctx, column.obj, C.grn_id(id), grnValue)
	case Int16:
		grnValue := C.int16_t(value)
		ok = C.grngo_column_set_int16(ctx, column.obj, C.grn_id(id), grnValue)
	case Int32:
		grnValue := C.int32_t(value)
		ok = C.grngo_column_set_int32(ctx, column.obj, C.grn_id(id), grnValue)
	case Int64:
		grnValue := C.int64_t(value)
		ok = C.grngo_column_set_int64(ctx, column.obj, C.grn_id(id), grnValue)
	case UInt8:
		grnValue := C.uint8_t(value)
		ok = C.grngo_column_set_uint8(ctx, column.obj, C.grn_id(id), grnValue)
	case UInt16:
		grnValue := C.uint16_t(value)
		ok = C.grngo_column_set_uint16(ctx, column.obj, C.grn_id(id), grnValue)
	case UInt32:
		grnValue := C.uint32_t(value)
		ok = C.grngo_column_set_uint32(ctx, column.obj, C.grn_id(id), grnValue)
	case UInt64:
		grnValue := C.uint64_t(value)
		ok = C.grngo_column_set_uint64(ctx, column.obj, C.grn_id(id), grnValue)
	case Time:
		grnValue := C.int64_t(value)
		ok = C.grngo_column_set_time(ctx, column.obj, C.grn_id(id), grnValue)
	default:
		return newInvalidValueTypeError(column.valueType, column.isVector, LazyInt, false)
	}
	if ok != C.GRN_TRUE {
		return fmt.Errorf("grngo_column_set_int*() failed")
	}
	return nil
}

// setFloat assigns a Float value.
func (column *Column) setFloat(id uint32, value float64) error {
	if (column.valueType != Float) || column.isVector {
		return newInvalidValueTypeError(column.valueType, column.isVector, Float, false)
	}
	grnValue := C.double(value)
	if ok := C.grngo_column_set_float(column.table.db.ctx, column.obj,
		C.grn_id(id), grnValue); ok != C.GRN_TRUE {
		return fmt.Errorf("grngo_column_set_float() failed")
	}
	return nil
}

// setText assigns a Text value.
func (column *Column) setText(id uint32, value []byte) error {
	switch column.valueType {
	case ShortText, Text, LongText:
	default:
		return newInvalidValueTypeError(column.valueType, column.isVector, Text, false)
	}
	if column.isVector {
		return newInvalidValueTypeError(column.valueType, column.isVector, Text, false)
	}
	var grnValue C.grngo_text
	if len(value) != 0 {
		grnValue.ptr = (*C.char)(unsafe.Pointer(&value[0]))
		grnValue.size = C.size_t(len(value))
	}
	if ok := C.grngo_column_set_text(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return fmt.Errorf("grngo_column_set_text() failed")
	}
	return nil
}

// setGeoPoint assigns a GeoPoint value.
func (column *Column) setGeoPoint(id uint32, value GeoPoint) error {
	switch column.valueType {
	case TokyoGeoPoint, WGS84GeoPoint:
	default:
		return newInvalidValueTypeError(column.valueType, column.isVector, LazyGeoPoint, false)
	}
	if column.isVector {
		return fmt.Errorf("value type conflict")
	}
	grnValue := C.grn_geo_point{C.int(value.Latitude), C.int(value.Longitude)}
	if ok := C.grngo_column_set_geo_point(column.table.db.ctx, column.obj,
		C.grn_builtin_type(column.valueType),
		C.grn_id(id), grnValue); ok != C.GRN_TRUE {
		return fmt.Errorf("grngo_column_set_geo_point() failed")
	}
	return nil
}

// setBoolVector assigns a Bool vector.
func (column *Column) setBoolVector(id uint32, value []bool) error {
	if (column.valueType != Bool) || !column.isVector {
		return newInvalidValueTypeError(column.valueType, column.isVector, Bool, true)
	}
	grnValue := make([]C.grn_bool, len(value))
	for i, v := range value {
		if v {
			grnValue[i] = C.GRN_TRUE
		}
	}
	var grnVector C.grngo_vector
	if len(grnValue) != 0 {
		grnVector.ptr = unsafe.Pointer(&grnValue[0])
		grnVector.size = C.size_t(len(grnValue))
	}
	if ok := C.grngo_column_set_bool_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return fmt.Errorf("grngo_column_set_bool_vector() failed")
	}
	return nil
}

// setIntVector assigns an Int vector.
func (column *Column) setIntVector(id uint32, value []int64) error {
	if !column.isVector {
		return newInvalidValueTypeError(column.valueType, column.isVector, LazyInt, true)
	}
	var grnVector C.grngo_vector
	if len(value) != 0 {
		grnVector.ptr = unsafe.Pointer(&value[0])
		grnVector.size = C.size_t(len(value))
	}
	ctx := column.table.db.ctx
	obj := column.obj
	var ok C.grn_bool
	switch column.valueType {
	case Int8:
		ok = C.grngo_column_set_int8_vector(ctx, obj, C.grn_id(id), &grnVector)
	case Int16:
		ok = C.grngo_column_set_int16_vector(ctx, obj, C.grn_id(id), &grnVector)
	case Int32:
		ok = C.grngo_column_set_int32_vector(ctx, obj, C.grn_id(id), &grnVector)
	case Int64:
		ok = C.grngo_column_set_int64_vector(ctx, obj, C.grn_id(id), &grnVector)
	case UInt8:
		ok = C.grngo_column_set_uint8_vector(ctx, obj, C.grn_id(id), &grnVector)
	case UInt16:
		ok = C.grngo_column_set_uint16_vector(ctx, obj, C.grn_id(id), &grnVector)
	case UInt32:
		ok = C.grngo_column_set_uint32_vector(ctx, obj, C.grn_id(id), &grnVector)
	case UInt64:
		ok = C.grngo_column_set_uint64_vector(ctx, obj, C.grn_id(id), &grnVector)
	case Time:
		ok = C.grngo_column_set_time_vector(ctx, obj, C.grn_id(id), &grnVector)
	default:
		return newInvalidValueTypeError(column.valueType, column.isVector, LazyInt, true)
	}
	if ok != C.GRN_TRUE {
		return fmt.Errorf("grngo_column_set_int*_vector() failed")
	}
	return nil
}

// setFloatVector assigns a Float vector.
func (column *Column) setFloatVector(id uint32, value []float64) error {
	if (column.valueType != Float) || !column.isVector {
		return newInvalidValueTypeError(column.valueType, column.isVector, Float, true)
	}
	var grnVector C.grngo_vector
	if len(value) != 0 {
		grnVector.ptr = unsafe.Pointer(&value[0])
		grnVector.size = C.size_t(len(value))
	}
	if ok := C.grngo_column_set_float_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return fmt.Errorf("grngo_column_set_float_vector() failed")
	}
	return nil
}

// setTextVector assigns a Text vector.
func (column *Column) setTextVector(id uint32, value [][]byte) error {
	if !column.isVector {
		return newInvalidValueTypeError(column.valueType, column.isVector, Text, true)
	}
	switch column.valueType {
	case ShortText, Text, LongText:
	default:
		return newInvalidValueTypeError(column.valueType, column.isVector, Text, true)
	}
	grnValue := make([]C.grngo_text, len(value))
	for i, v := range value {
		if len(v) != 0 {
			grnValue[i].ptr = (*C.char)(unsafe.Pointer(&v[0]))
			grnValue[i].size = C.size_t(len(v))
		}
	}
	var grnVector C.grngo_vector
	if len(grnValue) != 0 {
		grnVector.ptr = unsafe.Pointer(&grnValue[0])
		grnVector.size = C.size_t(len(grnValue))
	}
	if ok := C.grngo_column_set_text_vector(column.table.db.ctx,
		column.obj, C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return fmt.Errorf("grngo_column_set_text_vector() failed")
	}
	return nil
}

// setGeoPointVector assigns a GeoPoint vector.
func (column *Column) setGeoPointVector(id uint32, value []GeoPoint) error {
	if !column.isVector {
		return newInvalidValueTypeError(column.valueType, column.isVector, LazyGeoPoint, true)
	}
	switch column.valueType {
	case TokyoGeoPoint, WGS84GeoPoint:
	default:
		return newInvalidValueTypeError(column.valueType, column.isVector, LazyGeoPoint, true)
	}
	var grnVector C.grngo_vector
	if len(value) != 0 {
		grnVector.ptr = unsafe.Pointer(&value[0])
		grnVector.size = C.size_t(len(value))
	}
	if ok := C.grngo_column_set_geo_point_vector(column.table.db.ctx,
		column.obj, C.grn_builtin_type(column.valueType),
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return fmt.Errorf("grngo_column_set_geo_point_vector() failed")
	}
	return nil
}

// SetValue assigns a value.
func (column *Column) SetValue(id uint32, value interface{}) error {
	switch v := value.(type) {
	case bool:
		return column.setBool(id, v)
	case int64:
		return column.setInt(id, v)
	case float64:
		return column.setFloat(id, v)
	case []byte:
		return column.setText(id, v)
	case GeoPoint:
		return column.setGeoPoint(id, v)
	case []bool:
		return column.setBoolVector(id, v)
	case []int64:
		return column.setIntVector(id, v)
	case []float64:
		return column.setFloatVector(id, v)
	case [][]byte:
		return column.setTextVector(id, v)
	case []GeoPoint:
		return column.setGeoPointVector(id, v)
	default:
		return fmt.Errorf("unsupported value type: name = <%s>", reflect.TypeOf(value).Name())
	}
}

// getBool gets a Bool value.
func (column *Column) getBool(id uint32) (interface{}, error) {
	var grnValue C.grn_bool
	if ok := C.grngo_column_get_bool(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_bool() failed")
	}
	return grnValue == C.GRN_TRUE, nil
}

// getInt gets an Int value.
func (column *Column) getInt(id uint32) (interface{}, error) {
	var grnValue C.int64_t
	if ok := C.grngo_column_get_int(column.table.db.ctx, column.obj,
		C.grn_builtin_type(column.valueType),
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_int() failed")
	}
	return int64(grnValue), nil
}

// getFloat gets a Float value.
func (column *Column) getFloat(id uint32) (interface{}, error) {
	var grnValue C.double
	if ok := C.grngo_column_get_float(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_float() failed")
	}
	return float64(grnValue), nil
}

// getText gets a Text value.
func (column *Column) getText(id uint32) (interface{}, error) {
	var grnValue C.grngo_text
	if ok := C.grngo_column_get_text(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_text() failed")
	}
	if grnValue.size == 0 {
		return make([]byte, 0), nil
	}
	value := make([]byte, int(grnValue.size))
	grnValue.ptr = (*C.char)(unsafe.Pointer(&value[0]))
	if ok := C.grngo_column_get_text(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_text() failed")
	}
	return value, nil
}

// getGeoPoint gets a GeoPoint value.
func (column *Column) getGeoPoint(id uint32) (interface{}, error) {
	var grnValue C.grn_geo_point
	if ok := C.grngo_column_get_geo_point(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_geo_point() failed")
	}
	return GeoPoint{int32(grnValue.latitude), int32(grnValue.longitude)}, nil
}

// getBoolVector gets a BoolVector.
func (column *Column) getBoolVector(id uint32) (interface{}, error) {
	var grnVector C.grngo_vector
	if ok := C.grngo_column_get_bool_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_bool_vector() failed")
	}
	if grnVector.size == 0 {
		return make([]bool, 0), nil
	}
	grnValue := make([]C.grn_bool, int(grnVector.size))
	grnVector.ptr = unsafe.Pointer(&grnValue[0])
	if ok := C.grngo_column_get_bool_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_bool_vector() failed")
	}
	value := make([]bool, int(grnVector.size))
	for i, v := range grnValue {
		value[i] = (v == C.GRN_TRUE)
	}
	return value, nil
}

// getIntVector gets a IntVector.
func (column *Column) getIntVector(id uint32) (interface{}, error) {
	var grnValue C.grngo_vector
	if ok := C.grngo_column_get_int_vector(column.table.db.ctx, column.obj,
		C.grn_builtin_type(column.valueType),
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_int_vector() failed")
	}
	if grnValue.size == 0 {
		return make([]int64, 0), nil
	}
	value := make([]int64, int(grnValue.size))
	grnValue.ptr = unsafe.Pointer(&value[0])
	if ok := C.grngo_column_get_int_vector(column.table.db.ctx, column.obj,
		C.grn_builtin_type(column.valueType),
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_int_vector() failed")
	}
	return value, nil
}

// getFloatVector gets a FloatVector.
func (column *Column) getFloatVector(id uint32) (interface{}, error) {
	var grnValue C.grngo_vector
	if ok := C.grngo_column_get_float_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_float_vector() failed")
	}
	if grnValue.size == 0 {
		return make([]float64, 0), nil
	}
	value := make([]float64, int(grnValue.size))
	grnValue.ptr = unsafe.Pointer(&value[0])
	if ok := C.grngo_column_get_float_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_float_vector() failed")
	}
	return value, nil
}

// getTextVector gets a TextVector.
func (column *Column) getTextVector(id uint32) (interface{}, error) {
	var grnVector C.grngo_vector
	if ok := C.grngo_column_get_text_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_text_vector() failed")
	}
	if grnVector.size == 0 {
		return make([][]byte, 0), nil
	}
	grnValues := make([]C.grngo_text, int(grnVector.size))
	grnVector.ptr = unsafe.Pointer(&grnValues[0])
	if ok := C.grngo_column_get_text_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_text_vector() failed")
	}
	value := make([][]byte, int(grnVector.size))
	for i, grnValue := range grnValues {
		if grnValue.size != 0 {
			value[i] = make([]byte, int(grnValue.size))
			grnValues[i].ptr = (*C.char)(unsafe.Pointer(&value[i][0]))
		}
	}
	if ok := C.grngo_column_get_text_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_text_vector() failed")
	}
	return value, nil
}

// getGeoPointVector gets a GeoPointVector.
func (column *Column) getGeoPointVector(id uint32) (interface{}, error) {
	var grnValue C.grngo_vector
	if ok := C.grngo_column_get_geo_point_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_geo_point_vector() failed")
	}
	if grnValue.size == 0 {
		return make([]GeoPoint, 0), nil
	}
	value := make([]GeoPoint, int(grnValue.size))
	grnValue.ptr = unsafe.Pointer(&value[0])
	if ok := C.grngo_column_get_geo_point_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grngo_column_get_geo_point_vector() failed")
	}
	return value, nil
}

// GetValue gets a value.
func (column *Column) GetValue(id uint32) (interface{}, error) {
	if !column.isVector {
		switch column.valueType {
		case Bool:
			return column.getBool(id)
		case Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64:
			return column.getInt(id)
		case Float:
			return column.getFloat(id)
		case Time:
			return column.getInt(id)
		case ShortText, Text, LongText:
			return column.getText(id)
		case TokyoGeoPoint, WGS84GeoPoint:
			return column.getGeoPoint(id)
		}
	} else {
		switch column.valueType {
		case Bool:
			return column.getBoolVector(id)
		case Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64:
			return column.getIntVector(id)
		case Float:
			return column.getFloatVector(id)
		case Time:
			return column.getIntVector(id)
		case ShortText, Text, LongText:
			return column.getTextVector(id)
		case TokyoGeoPoint, WGS84GeoPoint:
			return column.getGeoPointVector(id)
		}
	}
	return nil, fmt.Errorf("undefined value type: valueType = %d", column.valueType)
}
