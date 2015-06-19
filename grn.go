package gnx

/*
#cgo pkg-config: groonga
#include "grn_cgo.h"
*/
import "C"

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

// -- Groonga --

// grnInitCount is a counter for automatically initializing and finalizing
// Groonga.
var grnInitCount = 0

// DisableGrnInitCount() disables grnInitCount.
// This is useful if you want to manyally initialize and finalize Groonga.
func DisableGrnInitCount() {
	grnInitCount = -1
}

// GrnInit() initializes Groonga if needed.
// grnInitCount is incremented and when it changes from 0 to 1, Groonga is
// initialized.
func GrnInit() error {
	switch grnInitCount {
	case -1: // Disabled.
		return nil
	case 0:
		if rc := C.grn_init(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("grn_init() failed: rc = %d", rc)
		}
	}
	grnInitCount++
	return nil
}

// GrnFin() finalizes Groonga if needed.
// grnInitCount is decremented and when it changes from 1 to 0, Groonga is
// finalized.
func GrnFin() error {
	switch grnInitCount {
	case -1: // Disabled.
		return nil
	case 0:
		return fmt.Errorf("Groonga is not initialized yet")
	case 1:
		if rc := C.grn_fin(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("grn_fin() failed: rc = %d", rc)
		}
	}
	grnInitCount--
	return nil
}

// openGrnCtx() allocates memory for grn_ctx and initializes it.
func openGrnCtx() (*C.grn_ctx, error) {
	if err := GrnInit(); err != nil {
		return nil, err
	}
	ctx := C.grn_ctx_open(0)
	if ctx == nil {
		GrnFin()
		return nil, fmt.Errorf("grn_ctx_open() failed")
	}
	return ctx, nil
}

// closeGrnCtx() finalizes grn_ctx and frees allocated memory.
func closeGrnCtx(ctx *C.grn_ctx) error {
	rc := C.grn_ctx_close(ctx)
	GrnFin()
	if rc != C.GRN_SUCCESS {
		return fmt.Errorf("grn_ctx_close() failed: rc = %d", rc)
	}
	return nil
}

// -- GrnDB --

type GrnDB struct {
	ctx    *C.grn_ctx
	obj    *C.grn_obj
	tables map[string]*GrnTable
}

// newGrnDB() creates a new GrnDB object.
func newGrnDB(ctx *C.grn_ctx, obj *C.grn_obj) *GrnDB {
	return &GrnDB{ctx, obj, make(map[string]*GrnTable)}
}

// CreateGrnDB() creates a Groonga database and returns a handle to it.
// A temporary database is created if path is empty.
func CreateGrnDB(path string) (*GrnDB, error) {
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
		closeGrnCtx(ctx)
		errMsg := C.GoString(&ctx.errbuf[0])
		return nil, fmt.Errorf("grn_db_create() failed: err = %s", errMsg)
	}
	return newGrnDB(ctx, obj), nil
}

// OpenGrnDB() opens an existing Groonga database and returns a handle.
func OpenGrnDB(path string) (*GrnDB, error) {
	ctx, err := openGrnCtx()
	if err != nil {
		return nil, err
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	obj := C.grn_db_open(ctx, cPath)
	if obj == nil {
		closeGrnCtx(ctx)
		errMsg := C.GoString(&ctx.errbuf[0])
		return nil, fmt.Errorf("grn_db_open() failed: err = %s", errMsg)
	}
	return newGrnDB(ctx, obj), nil
}

// Close() closes a handle.
func (db *GrnDB) Close() error {
  rc := C.grn_obj_close(db.ctx, db.obj)
  if rc != C.GRN_SUCCESS {
    closeGrnCtx(db.ctx)
    return fmt.Errorf("grn_obj_close() failed: rc = %d", rc)
  }
	return closeGrnCtx(db.ctx)
}

// Send() sends a raw command.
// The given command must be well-formed.
func (db *GrnDB) Send(command string) error {
	commandBytes := []byte(command)
	var cCommand *C.char
	if len(commandBytes) != 0 {
		cCommand = (*C.char)(unsafe.Pointer(&commandBytes[0]))
	}
	rc := C.grn_ctx_send(db.ctx, cCommand, C.uint(len(commandBytes)), 0)
	switch {
	case rc != C.GRN_SUCCESS:
		errMsg := C.GoString(&db.ctx.errbuf[0])
		return fmt.Errorf("grn_ctx_send() failed: rc = %d, err = %s", rc, errMsg)
	case db.ctx.rc != C.GRN_SUCCESS:
		errMsg := C.GoString(&db.ctx.errbuf[0])
		return fmt.Errorf("grn_ctx_send() failed: ctx.rc = %d, err = %s",
			db.ctx.rc, errMsg)
	}
	return nil
}

// SendEx() sends a command with separated options.
func (db *GrnDB) SendEx(name string, options map[string]string) error {
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

// Recv() receives the result of commands sent by Send().
func (db *GrnDB) Recv() ([]byte, error) {
	var resultBuffer *C.char
	var resultLength C.uint
	var flags C.int
	rc := C.grn_ctx_recv(db.ctx, &resultBuffer, &resultLength, &flags)
	switch {
	case rc != C.GRN_SUCCESS:
		errMsg := C.GoString(&db.ctx.errbuf[0])
		return nil, fmt.Errorf(
			"grn_ctx_recv() failed: rc = %d, err = %s", rc, errMsg)
	case db.ctx.rc != C.GRN_SUCCESS:
		errMsg := C.GoString(&db.ctx.errbuf[0])
		return nil, fmt.Errorf(
			"grn_ctx_recv() failed: ctx.rc = %d, err = %s", db.ctx.rc, errMsg)
	}
	result := C.GoBytes(unsafe.Pointer(resultBuffer), C.int(resultLength))
	return result, nil
}

// Query() sends a raw command and receive the result.
func (db *GrnDB) Query(command string) ([]byte, error) {
	if err := db.Send(command); err != nil {
		result, _ := db.Recv()
		return result, err
	}
	return db.Recv()
}

// QueryEx() sends a command with separated options and receives the result.
func (db *GrnDB) QueryEx(name string, options map[string]string) (
	[]byte, error) {
	if err := db.SendEx(name, options); err != nil {
		result, _ := db.Recv()
		return result, err
	}
	return db.Recv()
}

// CreateTable() creates a table.
func (db *GrnDB) CreateTable(name string, options *TableOptions) (*GrnTable, error) {
	if options == nil {
		options = NewTableOptions()
	}
	optionsMap := make(map[string]string)
	optionsMap["name"] = name
	switch options.TableType {
	case ArrayTable:
		optionsMap["flags"] = "TABLE_NO_KEY"
	case HashTable:
		optionsMap["flags"] = "TABLE_HASH_KEY"
	case PatTable:
		optionsMap["flags"] = "TABLE_PAT_KEY"
	case DatTable:
		optionsMap["flags"] = "TABLE_DAT_KEY"
	default:
		return nil, fmt.Errorf("undefined table type: options = %+v", options)
	}
	if options.WithSIS {
		optionsMap["flags"] += "|KEY_WITH_SIS"
	}
	if options.KeyType != "" {
		switch options.KeyType {
		case "Bool":
			optionsMap["key_type"] = "Bool"
		case "Int":
			optionsMap["key_type"] = "Int64"
		case "Float":
			optionsMap["key_type"] = "Float"
		case "GeoPoint":
			optionsMap["key_type"] = "WGS84GeoPoint"
		case "Text":
			optionsMap["key_type"] = "ShortText"
		default:
			if _, err := db.FindTable(options.KeyType); err != nil {
				return nil, fmt.Errorf("unsupported key type: options = %+v", options)
			}
			optionsMap["key_type"] = options.KeyType
		}
	}
	if options.ValueType != "" {
		switch options.ValueType {
		case "Bool":
			optionsMap["value_type"] = "Bool"
		case "Int":
			optionsMap["value_type"] = "Int64"
		case "Float":
			optionsMap["value_type"] = "Float"
		case "GeoPoint":
			optionsMap["value_type"] = "WGS84GeoPoint"
		default:
			if _, err := db.FindTable(options.ValueType); err != nil {
				return nil, fmt.Errorf("unsupported value type: options = %+v",
					options)
			}
			optionsMap["value_type"] = options.ValueType
		}
	}
	if options.DefaultTokenizer != "" {
		optionsMap["default_tokenizer"] = options.DefaultTokenizer
	}
	if options.Normalizer != "" {
		optionsMap["normalizer"] = options.Normalizer
	}
	if len(options.TokenFilters) != 0 {
		optionsMap["token_filters"] = strings.Join(options.TokenFilters, ",")
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

// FindTable() finds a table.
func (db *GrnDB) FindTable(name string) (*GrnTable, error) {
	if table, ok := db.tables[name]; ok {
		return table, nil
	}
	nameBytes := []byte(name)
	var cName *C.char
	if len(nameBytes) != 0 {
		cName = (*C.char)(unsafe.Pointer(&nameBytes[0]))
	}
	obj := C.grn_cgo_find_table(db.ctx, cName, C.int(len(nameBytes)))
	if obj == nil {
		return nil, fmt.Errorf("table not found: name = <%s>", name)
	}
	var keyInfo C.grn_cgo_type_info
	if ok := C.grn_cgo_table_get_key_info(db.ctx, obj, &keyInfo); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_table_get_key_info() failed: name = <%s>",
			name)
	}
	// Check the key type.
	var keyType TypeID
	switch keyInfo.data_type {
	case C.GRN_DB_VOID:
		keyType = VoidID
	case C.GRN_DB_BOOL:
		keyType = BoolID
	case C.GRN_DB_INT64:
		keyType = IntID
	case C.GRN_DB_FLOAT:
		keyType = FloatID
	case C.GRN_DB_WGS84_GEO_POINT:
		keyType = GeoPointID
	case C.GRN_DB_SHORT_TEXT:
		keyType = TextID
	default:
		return nil, fmt.Errorf("unsupported key type: data_type = %d",
			keyInfo.data_type)
	}
	// Find the destination table if the key is table reference.
	var keyTable *GrnTable
	if keyInfo.ref_table != nil {
		if keyType == VoidID {
			return nil, fmt.Errorf("reference to void: name = <%s>", name)
		}
		cKeyTableName := C.grn_cgo_table_get_name(db.ctx, keyInfo.ref_table)
		if cKeyTableName == nil {
			return nil, fmt.Errorf("grn_cgo_table_get_name() failed")
		}
		defer C.free(unsafe.Pointer(cKeyTableName))
		var err error
		keyTable, err = db.FindTable(C.GoString(cKeyTableName))
		if err != nil {
			return nil, err
		}
	}
	var valueInfo C.grn_cgo_type_info
	if ok := C.grn_cgo_table_get_value_info(db.ctx, obj, &valueInfo); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_table_get_value_info() failed: name = <%s>",
			name)
	}
	// Check the value type.
	var valueType TypeID
	switch valueInfo.data_type {
	case C.GRN_DB_VOID:
		valueType = VoidID
	case C.GRN_DB_BOOL:
		valueType = BoolID
	case C.GRN_DB_INT64:
		valueType = IntID
	case C.GRN_DB_FLOAT:
		valueType = FloatID
	case C.GRN_DB_WGS84_GEO_POINT:
		valueType = GeoPointID
	case C.GRN_DB_SHORT_TEXT:
		valueType = TextID
	default:
		return nil, fmt.Errorf("unsupported value type: data_type = %d",
			valueInfo.data_type)
	}
	// Find the destination table if the value is table reference.
	var valueTable *GrnTable
	if valueInfo.ref_table != nil {
		if valueType == VoidID {
			return nil, fmt.Errorf("reference to void: name = <%s>", name)
		}
		cValueTableName := C.grn_cgo_table_get_name(db.ctx, valueInfo.ref_table)
		if cValueTableName == nil {
			return nil, fmt.Errorf("grn_cgo_table_get_name() failed")
		}
		defer C.free(unsafe.Pointer(cValueTableName))
		var err error
		valueTable, err = db.FindTable(C.GoString(cValueTableName))
		if err != nil {
			return nil, err
		}
	}
	table := newGrnTable(db, obj, name, keyType, keyTable, valueType, valueTable)
	db.tables[name] = table
	return table, nil
}

// InsertRow() inserts a row.
func (db *GrnDB) InsertRow(tableName string, key interface{}) (bool, Int, error) {
	table, err := db.FindTable(tableName)
	if err != nil {
		return false, NullInt(), err
	}
	return table.InsertRow(key)
}

// CreateColumn() creates a column.
func (db *GrnDB) CreateColumn(tableName, columnName string, valueType string,
	options *ColumnOptions) (*GrnColumn, error) {
	table, err := db.FindTable(tableName)
	if err != nil {
		return nil, err
	}
	return table.CreateColumn(columnName, valueType, options)
}

// FindColumn() finds a column.
func (db *GrnDB) FindColumn(tableName, columnName string) (*GrnColumn, error) {
	table, err := db.FindTable(tableName)
	if err != nil {
		return nil, err
	}
	return table.FindColumn(columnName)
}

// -- GrnTable --

type GrnTable struct {
	db         *GrnDB
	obj        *C.grn_obj
	name       string
	keyType    TypeID
	keyTable   *GrnTable
	valueType  TypeID
	valueTable *GrnTable
	columns    map[string]*GrnColumn
}

// newGrnTable() creates a new GrnTable object.
func newGrnTable(db *GrnDB, obj *C.grn_obj, name string, keyType TypeID,
	keyTable *GrnTable, valueType TypeID, valueTable *GrnTable) *GrnTable {
	var table GrnTable
	table.db = db
	table.obj = obj
	table.name = name
	table.keyType = keyType
	table.keyTable = keyTable
	table.valueType = valueType
	table.valueTable = valueTable
	table.columns = make(map[string]*GrnColumn)
	return &table
}

// insertVoid() inserts an empty row.
func (table *GrnTable) insertVoid() (bool, Int, error) {
	if table.keyType != VoidID {
		return false, NullInt(), fmt.Errorf("key type conflict")
	}
	rowInfo := C.grn_cgo_table_insert_void(table.db.ctx, table.obj)
	if rowInfo.id == C.GRN_ID_NIL {
		return false, NullInt(), fmt.Errorf("grn_cgo_table_insert_void() failed")
	}
	return rowInfo.inserted == C.GRN_TRUE, Int(rowInfo.id), nil
}

// insertBool() inserts a row with Bool key.
func (table *GrnTable) insertBool(key Bool) (bool, Int, error) {
	if table.keyType != BoolID {
		return false, NullInt(), fmt.Errorf("key type conflict")
	}
	grnKey := C.grn_bool(C.GRN_FALSE)
	if key == True {
		grnKey = C.grn_bool(C.GRN_TRUE)
	}
	rowInfo := C.grn_cgo_table_insert_bool(table.db.ctx, table.obj, grnKey)
	if rowInfo.id == C.GRN_ID_NIL {
		return false, NullInt(), fmt.Errorf("grn_cgo_table_insert_bool() failed")
	}
	return rowInfo.inserted == C.GRN_TRUE, Int(rowInfo.id), nil
}

// insertInt() inserts a row with Int key.
func (table *GrnTable) insertInt(key Int) (bool, Int, error) {
	if table.keyType != IntID {
		return false, NullInt(), fmt.Errorf("key type conflict")
	}
	grnKey := C.int64_t(key)
	rowInfo := C.grn_cgo_table_insert_int(table.db.ctx, table.obj, grnKey)
	if rowInfo.id == C.GRN_ID_NIL {
		return false, NullInt(), fmt.Errorf("grn_cgo_table_insert_int() failed")
	}
	return rowInfo.inserted == C.GRN_TRUE, Int(rowInfo.id), nil
}

// insertFloat() inserts a row with Float key.
func (table *GrnTable) insertFloat(key Float) (bool, Int, error) {
	if table.keyType != FloatID {
		return false, NullInt(), fmt.Errorf("key type conflict")
	}
	grnKey := C.double(key)
	rowInfo := C.grn_cgo_table_insert_float(table.db.ctx, table.obj, grnKey)
	if rowInfo.id == C.GRN_ID_NIL {
		return false, NullInt(), fmt.Errorf("grn_cgo_table_insert_float() failed")
	}
	return rowInfo.inserted == C.GRN_TRUE, Int(rowInfo.id), nil
}

// insertGeoPoint() inserts a row with GeoPoint key.
func (table *GrnTable) insertGeoPoint(key GeoPoint) (bool, Int, error) {
	if table.keyType != GeoPointID {
		return false, NullInt(), fmt.Errorf("key type conflict")
	}
	grnKey := C.grn_geo_point{C.int(key.Latitude), C.int(key.Longitude)}
	rowInfo := C.grn_cgo_table_insert_geo_point(table.db.ctx, table.obj, grnKey)
	if rowInfo.id == C.GRN_ID_NIL {
		return false, NullInt(), fmt.Errorf("grn_cgo_table_insert_geo_point() failed")
	}
	return rowInfo.inserted == C.GRN_TRUE, Int(rowInfo.id), nil
}

// insertText() inserts a row with Text key.
func (table *GrnTable) insertText(key Text) (bool, Int, error) {
	if table.keyType != TextID {
		return false, NullInt(), fmt.Errorf("key type conflict")
	}
	var grnKey C.grn_cgo_text
	if len(key) != 0 {
		grnKey.ptr = (*C.char)(unsafe.Pointer(&key[0]))
		grnKey.size = C.size_t(len(key))
	}
	rowInfo := C.grn_cgo_table_insert_text(table.db.ctx, table.obj, &grnKey)
	if rowInfo.id == C.GRN_ID_NIL {
		return false, NullInt(), fmt.Errorf("grn_cgo_table_insert_text() failed")
	}
	return rowInfo.inserted == C.GRN_TRUE, Int(rowInfo.id), nil
}

// InsertRow() inserts a row.
// The first return value specifies whether a row is inserted or not.
// The second return value is the ID of the inserted or found row.
func (table *GrnTable) InsertRow(key interface{}) (bool, Int, error) {
	switch value := key.(type) {
	case nil:
		return table.insertVoid()
	case Bool:
		return table.insertBool(value)
	case Int:
		return table.insertInt(value)
	case Float:
		return table.insertFloat(value)
	case GeoPoint:
		return table.insertGeoPoint(value)
	case Text:
		return table.insertText(value)
	default:
		return false, NullInt(), fmt.Errorf(
			"unsupported key type: typeName = <%s>", reflect.TypeOf(key).Name())
	}
}

// CreateColumn() creates a column.
func (table *GrnTable) CreateColumn(name string, valueType string,
	options *ColumnOptions) (*GrnColumn, error) {
	if options == nil {
		options = NewColumnOptions()
	}
	optionsMap := make(map[string]string)
	optionsMap["table"] = table.name
	optionsMap["name"] = name
	switch valueType {
	case "Bool":
		optionsMap["type"] = "Bool"
	case "Int":
		optionsMap["type"] = "Int64"
	case "Float":
		optionsMap["type"] = "Float"
	case "GeoPoint":
		optionsMap["type"] = "WGS84GeoPoint"
	case "Text":
		optionsMap["type"] = "LongText"
	default:
		if _, err := table.db.FindTable(valueType); err != nil {
			return nil, fmt.Errorf("unsupported value type: valueType = %s", valueType)
		}
		optionsMap["type"] = valueType
	}
	switch options.ColumnType {
	case ScalarColumn:
		optionsMap["flags"] = "COLUMN_SCALAR"
	case VectorColumn:
		optionsMap["flags"] = "COLUMN_VECTOR"
	case IndexColumn:
		optionsMap["flags"] = "COLUMN_INDEX"
	default:
		return nil, fmt.Errorf("undefined column type: options = %+v", options)
	}
	switch options.CompressionType {
	case NoCompression:
	case ZlibCompression:
		optionsMap["flags"] = "|COMPRESS_ZLIB"
	case LzoCompression:
		optionsMap["flags"] = "|COMRESS_LZO"
	default:
		return nil, fmt.Errorf("undefined compression type: options = %+v", options)
	}
	if options.WithSection {
		optionsMap["flags"] += "|WITH_SECTION"
	}
	if options.WithWeight {
		optionsMap["flags"] += "|WITH_WEIGHT"
	}
	if options.WithPosition {
		optionsMap["flags"] += "|WITH_POSITION"
	}
	if options.Source != "" {
		optionsMap["source"] = options.Source
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

// findColumn() finds a column.
func (table *GrnTable) findColumn(name string) (*GrnColumn, error) {
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
		return nil, fmt.Errorf("grn_obj_column() failed: table = %+v, name = <%s>", table, name)
	}
	var valueType TypeID
	var valueTable *GrnTable
	var isVector bool
	switch name {
	case "_id":
		valueType = IntID
	case "_key":
		valueType = table.keyType
		valueTable = table.keyTable
	case "_value":
		valueType = table.valueType
		valueTable = table.valueTable
	default:
		var valueInfo C.grn_cgo_type_info
		if ok := C.grn_cgo_column_get_value_info(table.db.ctx, obj, &valueInfo); ok != C.GRN_TRUE {
			return nil, fmt.Errorf("grn_cgo_column_get_value_info() failed: name = <%s>",
				name)
		}
		// Check the value type.
		switch valueInfo.data_type {
		case C.GRN_DB_BOOL:
			valueType = BoolID
		case C.GRN_DB_INT64:
			valueType = IntID
		case C.GRN_DB_FLOAT:
			valueType = FloatID
		case C.GRN_DB_WGS84_GEO_POINT:
			valueType = GeoPointID
		case C.GRN_DB_SHORT_TEXT, C.GRN_DB_LONG_TEXT:
			valueType = TextID
		default:
			return nil, fmt.Errorf("unsupported value type: data_type = %d",
				valueInfo.data_type)
		}
		isVector = valueInfo.dimension > 0
		// Find the destination table if the value is table reference.
		if valueInfo.ref_table != nil {
			if valueType == VoidID {
				return nil, fmt.Errorf("reference to void: name = <%s>", name)
			}
			cValueTableName := C.grn_cgo_table_get_name(table.db.ctx, valueInfo.ref_table)
			if cValueTableName == nil {
				return nil, fmt.Errorf("grn_cgo_table_get_name() failed")
			}
			defer C.free(unsafe.Pointer(cValueTableName))
			var err error
			valueTable, err = table.db.FindTable(C.GoString(cValueTableName))
			if err != nil {
				return nil, err
			}
		}
	}
	column := newGrnColumn(table, obj, name, valueType, isVector, valueTable)
	table.columns[name] = column
	return column, nil
}

// FindColumn() finds a column.
func (table *GrnTable) FindColumn(name string) (*GrnColumn, error) {
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
	column = newGrnColumn(table, obj, name, column.valueType, isVector, valueTable)
	table.columns[name] = column
	return column, nil
}

// -- GrnColumn --

type GrnColumn struct {
	table      *GrnTable
	obj        *C.grn_obj
	name       string
	valueType  TypeID
	isVector   bool
	valueTable *GrnTable
}

// newGrnColumn() creates a new GrnColumn object.
func newGrnColumn(table *GrnTable, obj *C.grn_obj, name string,
	valueType TypeID, isVector bool, valueTable *GrnTable) *GrnColumn {
	var column GrnColumn
	column.table = table
	column.obj = obj
	column.name = name
	column.valueType = valueType
	column.isVector = isVector
	column.valueTable = valueTable
	return &column
}

// setBool() assigns a Bool value.
func (column *GrnColumn) setBool(id Int, value Bool) error {
	if (column.valueType != BoolID) || column.isVector {
		return fmt.Errorf("value type conflict")
	}
	var grnValue C.grn_bool = C.GRN_FALSE
	if value == True {
		grnValue = C.GRN_TRUE
	}
	if ok := C.grn_cgo_column_set_bool(column.table.db.ctx, column.obj,
		C.grn_id(id), grnValue); ok != C.GRN_TRUE {
		return fmt.Errorf("grn_cgo_column_set_bool() failed")
	}
	return nil
}

// setInt() assigns an Int value.
func (column *GrnColumn) setInt(id Int, value Int) error {
	if (column.valueType != IntID) || column.isVector {
		return fmt.Errorf("value type conflict")
	}
	grnValue := C.int64_t(value)
	if ok := C.grn_cgo_column_set_int(column.table.db.ctx, column.obj,
		C.grn_id(id), grnValue); ok != C.GRN_TRUE {
		return fmt.Errorf("grn_cgo_column_set_int() failed")
	}
	return nil
}

// setFloat() assigns a Float value.
func (column *GrnColumn) setFloat(id Int, value Float) error {
	if (column.valueType != FloatID) || column.isVector {
		return fmt.Errorf("value type conflict")
	}
	grnValue := C.double(value)
	if ok := C.grn_cgo_column_set_float(column.table.db.ctx, column.obj,
		C.grn_id(id), grnValue); ok != C.GRN_TRUE {
		return fmt.Errorf("grn_cgo_column_set_float() failed")
	}
	return nil
}

// setGeoPoint() assigns a GeoPoint value.
func (column *GrnColumn) setGeoPoint(id Int, value GeoPoint) error {
	if (column.valueType != GeoPointID) || column.isVector {
		return fmt.Errorf("value type conflict")
	}
	grnValue := C.grn_geo_point{C.int(value.Latitude), C.int(value.Longitude)}
	if ok := C.grn_cgo_column_set_geo_point(column.table.db.ctx, column.obj,
		C.grn_id(id), grnValue); ok != C.GRN_TRUE {
		return fmt.Errorf("grn_cgo_column_set_geo_point() failed")
	}
	return nil
}

// setText() assigns a Text value.
func (column *GrnColumn) setText(id Int, value Text) error {
	if (column.valueType != TextID) || column.isVector {
		return fmt.Errorf("value type conflict")
	}
	var grnValue C.grn_cgo_text
	if len(value) != 0 {
		grnValue.ptr = (*C.char)(unsafe.Pointer(&value[0]))
		grnValue.size = C.size_t(len(value))
	}
	if ok := C.grn_cgo_column_set_text(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return fmt.Errorf("grn_cgo_column_set_text() failed")
	}
	return nil
}

// setBoolVector() assigns a Bool vector.
func (column *GrnColumn) setBoolVector(id Int, value []Bool) error {
	grnValue := make([]C.grn_bool, len(value))
	for i, v := range value {
		if v == True {
			grnValue[i] = C.GRN_TRUE
		}
	}
	var grnVector C.grn_cgo_vector
	if len(grnValue) != 0 {
		grnVector.ptr = unsafe.Pointer(&grnValue[0])
		grnVector.size = C.size_t(len(grnValue))
	}
	if ok := C.grn_cgo_column_set_bool_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return fmt.Errorf("grn_cgo_column_set_bool_vector() failed")
	}
	return nil
}

// setIntVector() assigns an Int vector.
func (column *GrnColumn) setIntVector(id Int, value []Int) error {
	var grnVector C.grn_cgo_vector
	if len(value) != 0 {
		grnVector.ptr = unsafe.Pointer(&value[0])
		grnVector.size = C.size_t(len(value))
	}
	if ok := C.grn_cgo_column_set_int_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return fmt.Errorf("grn_cgo_column_set_int_vector() failed")
	}
	return nil
}

// setFloatVector() assigns a Float vector.
func (column *GrnColumn) setFloatVector(id Int, value []Float) error {
	var grnVector C.grn_cgo_vector
	if len(value) != 0 {
		grnVector.ptr = unsafe.Pointer(&value[0])
		grnVector.size = C.size_t(len(value))
	}
	if ok := C.grn_cgo_column_set_float_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return fmt.Errorf("grn_cgo_column_set_float_vector() failed")
	}
	return nil
}

// setGeoPointVector() assigns a GeoPoint vector.
func (column *GrnColumn) setGeoPointVector(id Int, value []GeoPoint) error {
	var grnVector C.grn_cgo_vector
	if len(value) != 0 {
		grnVector.ptr = unsafe.Pointer(&value[0])
		grnVector.size = C.size_t(len(value))
	}
	if ok := C.grn_cgo_column_set_geo_point_vector(column.table.db.ctx,
		column.obj, C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return fmt.Errorf("grn_cgo_column_set_geo_point_vector() failed")
	}
	return nil
}

// setTextVector() assigns a Text vector.
func (column *GrnColumn) setTextVector(id Int, value []Text) error {
	grnValue := make([]C.grn_cgo_text, len(value))
	for i, v := range value {
		if len(v) != 0 {
			grnValue[i].ptr = (*C.char)(unsafe.Pointer(&v[0]))
			grnValue[i].size = C.size_t(len(v))
		}
	}
	var grnVector C.grn_cgo_vector
	if len(grnValue) != 0 {
		grnVector.ptr = unsafe.Pointer(&grnValue[0])
		grnVector.size = C.size_t(len(grnValue))
	}
	if ok := C.grn_cgo_column_set_text_vector(column.table.db.ctx,
		column.obj, C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return fmt.Errorf("grn_cgo_column_set_text_vector() failed")
	}
	return nil
}

// SetValue() assigns a value.
func (column *GrnColumn) SetValue(id Int, value interface{}) error {
	switch v := value.(type) {
	case Bool:
		return column.setBool(id, v)
	case Int:
		return column.setInt(id, v)
	case Float:
		return column.setFloat(id, v)
	case GeoPoint:
		return column.setGeoPoint(id, v)
	case Text:
		return column.setText(id, v)
	case []Bool:
		return column.setBoolVector(id, v)
	case []Int:
		return column.setIntVector(id, v)
	case []Float:
		return column.setFloatVector(id, v)
	case []GeoPoint:
		return column.setGeoPointVector(id, v)
	case []Text:
		return column.setTextVector(id, v)
	default:
		return fmt.Errorf("unsupported value type: name = <%s>",
			reflect.TypeOf(value).Name())
	}
}

// getBool() gets a Bool value.
func (column *GrnColumn) getBool(id Int) (interface{}, error) {
	var grnValue C.grn_bool
	if ok := C.grn_cgo_column_get_bool(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_bool() failed")
	}
	if grnValue == C.GRN_TRUE {
		return True, nil
	} else {
		return False, nil
	}
}

// getInt() gets an Int value.
func (column *GrnColumn) getInt(id Int) (interface{}, error) {
	var grnValue C.int64_t
	if ok := C.grn_cgo_column_get_int(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_int() failed")
	}
	return Int(grnValue), nil
}

// getFloat() gets a Float value.
func (column *GrnColumn) getFloat(id Int) (interface{}, error) {
	var grnValue C.double
	if ok := C.grn_cgo_column_get_float(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_float() failed")
	}
	return Float(grnValue), nil
}

// getGeoPoint() gets a GeoPoint value.
func (column *GrnColumn) getGeoPoint(id Int) (interface{}, error) {
	var grnValue C.grn_geo_point
	if ok := C.grn_cgo_column_get_geo_point(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_geo_point() failed")
	}
	return GeoPoint{int32(grnValue.latitude), int32(grnValue.longitude)}, nil
}

// getText() gets a Text value.
func (column *GrnColumn) getText(id Int) (interface{}, error) {
	var grnValue C.grn_cgo_text
	if ok := C.grn_cgo_column_get_text(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_text() failed")
	}
	if grnValue.size == 0 {
		return make(Text, 0), nil
	}
	value := make(Text, int(grnValue.size))
	grnValue.ptr = (*C.char)(unsafe.Pointer(&value[0]))
	if ok := C.grn_cgo_column_get_text(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_text() failed")
	}
	return value, nil
}

// getBoolVector() gets a BoolVector.
func (column *GrnColumn) getBoolVector(id Int) (interface{}, error) {
	var grnVector C.grn_cgo_vector
	if ok := C.grn_cgo_column_get_bool_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_bool_vector() failed")
	}
	if grnVector.size == 0 {
		return make([]Bool, 0), nil
	}
	grnValue := make([]C.grn_bool, int(grnVector.size))
	grnVector.ptr = unsafe.Pointer(&grnValue[0])
	if ok := C.grn_cgo_column_get_bool_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_bool_vector() failed")
	}
	value := make([]Bool, int(grnVector.size))
	for i, v := range grnValue {
		if v == C.GRN_TRUE {
			value[i] = True
		}
	}
	return value, nil
}

// getIntVector() gets a IntVector.
func (column *GrnColumn) getIntVector(id Int) (interface{}, error) {
	var grnValue C.grn_cgo_vector
	if ok := C.grn_cgo_column_get_int_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_int_vector() failed")
	}
	if grnValue.size == 0 {
		return make([]Int, 0), nil
	}
	value := make([]Int, int(grnValue.size))
	grnValue.ptr = unsafe.Pointer(&value[0])
	if ok := C.grn_cgo_column_get_int_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_int_vector() failed")
	}
	return value, nil
}

// getFloatVector() gets a FloatVector.
func (column *GrnColumn) getFloatVector(id Int) (interface{}, error) {
	var grnValue C.grn_cgo_vector
	if ok := C.grn_cgo_column_get_float_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_float_vector() failed")
	}
	if grnValue.size == 0 {
		return make([]Float, 0), nil
	}
	value := make([]Float, int(grnValue.size))
	grnValue.ptr = unsafe.Pointer(&value[0])
	if ok := C.grn_cgo_column_get_float_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_float_vector() failed")
	}
	return value, nil
}

// getGeoPointVector() gets a GeoPointVector.
func (column *GrnColumn) getGeoPointVector(id Int) (interface{}, error) {
	var grnValue C.grn_cgo_vector
	if ok := C.grn_cgo_column_get_geo_point_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_geo_point_vector() failed")
	}
	if grnValue.size == 0 {
		return make([]GeoPoint, 0), nil
	}
	value := make([]GeoPoint, int(grnValue.size))
	grnValue.ptr = unsafe.Pointer(&value[0])
	if ok := C.grn_cgo_column_get_geo_point_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnValue); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_geo_point_vector() failed")
	}
	return value, nil
}

// getTextVector() gets a TextVector.
func (column *GrnColumn) getTextVector(id Int) (interface{}, error) {
	var grnVector C.grn_cgo_vector
	if ok := C.grn_cgo_column_get_text_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_text_vector() failed")
	}
	if grnVector.size == 0 {
		return make([]Text, 0), nil
	}
	grnValues := make([]C.grn_cgo_text, int(grnVector.size))
	grnVector.ptr = unsafe.Pointer(&grnValues[0])
	if ok := C.grn_cgo_column_get_text_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_text_vector() failed")
	}
	value := make([]Text, int(grnVector.size))
	for i, grnValue := range grnValues {
		if grnValue.size != 0 {
			value[i] = make(Text, int(grnValue.size))
			grnValues[i].ptr = (*C.char)(unsafe.Pointer(&value[i][0]))
		}
	}
	if ok := C.grn_cgo_column_get_text_vector(column.table.db.ctx, column.obj,
		C.grn_id(id), &grnVector); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_text_vector() failed")
	}
	return value, nil
}

// GetValue() gets a value.
// TODO: GetValue() should use allocated spaces for better performance.
func (column *GrnColumn) GetValue(id Int) (interface{}, error) {
	if !column.isVector {
		switch column.valueType {
		case BoolID:
			return column.getBool(id)
		case IntID:
			return column.getInt(id)
		case FloatID:
			return column.getFloat(id)
		case GeoPointID:
			return column.getGeoPoint(id)
		case TextID:
			return column.getText(id)
		}
	} else {
		switch column.valueType {
		case BoolID:
			return column.getBoolVector(id)
		case IntID:
			return column.getIntVector(id)
		case FloatID:
			return column.getFloatVector(id)
		case GeoPointID:
			return column.getGeoPointVector(id)
		case TextID:
			return column.getTextVector(id)
		}
	}
	return nil, fmt.Errorf("undefined value type: valueType = %d", column.valueType)
}

func (column *GrnColumn) getBools(ids []Int) (interface{}, error) {
	grnValues := make([]C.grn_bool, len(ids))
	if ok := C.grn_cgo_column_get_bools(column.table.db.ctx, column.obj,
		C.size_t(len(ids)), (*C.int64_t)(unsafe.Pointer(&ids[0])),
		&grnValues[0]); ok != C.GRN_TRUE {
		return nil, fmt.Errorf("grn_cgo_column_get_bools() failed")
	}
	values := make([]Bool, len(ids))
	for i, _ := range values {
		if grnValues[i] == C.GRN_TRUE {
			values[i] = True
		}
	}
	return values, nil
}

func (column *GrnColumn) GetValues(ids []Int) (interface{}, error) {
	if !column.isVector {
		switch column.valueType {
		case BoolID:
			return column.getBools(ids)
//		case IntID:
//			return column.getInts(ids)
//		case FloatID:
//			return column.getFloats(ids)
//		case GeoPointID:
//			return column.getGeoPoints(ids)
//		case TextID:
//			return column.getTexts(ids)
		}
	} else {
//		switch column.valueType {
//		case BoolID:
//			return column.getBoolVectors(ids)
//		case IntID:
//			return column.getIntVectors(ids)
//		case FloatID:
//			return column.getFloatVectors(ids)
//		case GeoPointID:
//			return column.getGeoPointVectors(ids)
//		case TextID:
//			return column.getTextVectors(ids)
//		}
	}
	return nil, fmt.Errorf("undefined value type: valueType = %d", column.valueType)
}
