package grngo

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// Functions for random key/value generation.

func generateRandomTime() int64 {
	const (
		MinTime = int64(0)
		MaxTime = int64(1892160000000000)
	)
	return MinTime + rand.Int63n(MaxTime-MinTime+1)
}

func generateRandomText() []byte {
	return []byte(strconv.Itoa(rand.Int()))
}

func generateRandomGeoPoint() GeoPoint {
	const (
		MinLatitude  = 73531000
		MaxLatitude  = 164006000
		MinLongitude = 439451000
		MaxLongitude = 554351000
	)
	latitude := MinLatitude + rand.Intn(MaxLatitude-MinLatitude+1)
	longitude := MinLongitude + rand.Intn(MaxLongitude-MinLongitude+1)
	return GeoPoint{int32(latitude), int32(longitude)}
}

func generateRandomScalar(valueType string) interface{} {
	switch valueType {
	case "Bool":
		return (rand.Int() & 1) == 1
	case "Int8":
		return int64(int8(rand.Int()))
	case "Int16":
		return int64(int16(rand.Int()))
	case "Int32":
		return int64(int32(rand.Int()))
	case "Int64":
		return rand.Int63()
	case "UInt8":
		return int64(uint8(rand.Int()))
	case "UInt16":
		return int64(uint16(rand.Int()))
	case "UInt32":
		return int64(rand.Uint32())
	case "UInt64":
		return rand.Int63()
	case "Float":
		return rand.Float64()
	case "Time":
		return generateRandomTime()
	case "ShortText", "Text", "LongText":
		return generateRandomText()
	case "TokyoGeoPoint", "WGS84GeoPoint":
		return generateRandomGeoPoint()
	}
	return nil
}

func generateRandomVector(valueType string) interface{} {
	size := rand.Int() % 10
	switch valueType {
	case "Bool":
		value := make([]bool, size)
		for i := 0; i < size; i++ {
			value[i] = (rand.Int() & 1) == 1
		}
		return value
	case "Int8":
		value := make([]int64, size)
		for i := 0; i < size; i++ {
			value[i] = int64(int8(rand.Int()))
		}
		return value
	case "Int16":
		value := make([]int64, size)
		for i := 0; i < size; i++ {
			value[i] = int64(int16(rand.Int()))
		}
		return value
	case "Int32":
		value := make([]int64, size)
		for i := 0; i < size; i++ {
			value[i] = int64(int32(rand.Int()))
		}
		return value
	case "Int64":
		value := make([]int64, size)
		for i := 0; i < size; i++ {
			value[i] = rand.Int63()
		}
		return value
	case "UInt8":
		value := make([]int64, size)
		for i := 0; i < size; i++ {
			value[i] = int64(uint8(rand.Int()))
		}
		return value
	case "UInt16":
		value := make([]int64, size)
		for i := 0; i < size; i++ {
			value[i] = int64(uint16(rand.Int()))
		}
		return value
	case "UInt32":
		value := make([]int64, size)
		for i := 0; i < size; i++ {
			value[i] = int64(rand.Uint32())
		}
		return value
	case "UInt64":
		value := make([]int64, size)
		for i := 0; i < size; i++ {
			value[i] = rand.Int63()
		}
		return value
	case "Float":
		value := make([]float64, size)
		for i := 0; i < size; i++ {
			value[i] = rand.Float64()
		}
		return value
	case "Time":
		value := make([]int64, size)
		for i := 0; i < size; i++ {
			value[i] = generateRandomTime()
		}
		return value
	case "ShortText", "Text", "LongText":
		value := make([][]byte, size)
		for i := 0; i < size; i++ {
			value[i] = generateRandomText()
		}
		return value
	case "TokyoGeoPoint", "WGS84GeoPoint":
		value := make([]GeoPoint, size)
		for i := 0; i < size; i++ {
			value[i] = generateRandomGeoPoint()
		}
		return value
	}
	return nil
}

func generateRandomValue(valueType string) interface{} {
	if strings.HasPrefix(valueType, "[]") {
		return generateRandomVector(valueType[2:])
	}
	return generateRandomScalar(valueType)
}

func generateRandomKey(keyType string) interface{} {
	switch keyType {
	case "Bool":
		return (rand.Int() & 1) == 1
	case "Int8":
		return int64(int8(rand.Int()))
	case "Int16":
		return int64(int16(rand.Int()))
	case "Int32":
		return int64(int32(rand.Int()))
	case "Int64":
		return rand.Int63()
	case "UInt8":
		return int64(uint8(rand.Int()))
	case "UInt16":
		return int64(uint16(rand.Int()))
	case "UInt32":
		return int64(rand.Uint32())
	case "UInt64":
		return rand.Int63()
	case "Float":
		return rand.Float64()
	case "Time":
		return generateRandomTime()
	case "ShortText":
		return generateRandomText()
	case "TokyoGeoPoint", "WGS84GeoPoint":
		return generateRandomGeoPoint()
	default:
		return nil
	}
}

// Functions to create/remove temporary DB objects.

// createTempDB() creates a database for tests.
// The database must be removed with removeTempDB().
func createTempDB(tb testing.TB) (string, string, *DB) {
	dirPath, err := ioutil.TempDir("", "grngo_test")
	if err != nil {
		tb.Fatalf("ioutil.TempDir() failed: %v", err)
	}
	dbPath := dirPath + "/db"
	db, err := CreateDB(dbPath)
	if err != nil {
		os.RemoveAll(dirPath)
		tb.Fatalf("CreateDB() failed: %v", err)
	}
	return dirPath, dbPath, db
}

// createTempTable() creates a database and a table for tests.
// createTempTable() uses createTempDB() to create a database, so the
// database must be removed with removeTempDB().
func createTempTable(tb testing.TB, name string, options *TableOptions) (
	string, string, *DB, *Table) {
	dirPath, dbPath, db := createTempDB(tb)
	table, err := db.CreateTable(name, options)
	if err != nil {
		removeTempDB(tb, dirPath, db)
		tb.Fatalf("DB.CreateTable() failed: %v", err)
	}
	return dirPath, dbPath, db, table
}

// createTempColumn() creates a database, a table, and a column for tests.
// createTempColumn() uses createTempDB() to create a database, so the
// database must be removed with removeTempDB().
func createTempColumn(tb testing.TB, tableName string,
	tableOptions *TableOptions, columnName string, valueType string,
	columnOptions *ColumnOptions) (
	string, string, *DB, *Table, *Column) {
	dirPath, dbPath, db, table := createTempTable(tb, tableName, tableOptions)
	column, err := table.CreateColumn(columnName, valueType, columnOptions)
	if err != nil {
		removeTempDB(tb, dirPath, db)
		tb.Fatalf("DB.CreateTable() failed: %v", err)
	}
	return dirPath, dbPath, db, table, column
}

// removeTempDB() removes a database created with createTempDB().
func removeTempDB(tb testing.TB, dirPath string, db *DB) {
	if err := db.Close(); err != nil {
		os.RemoveAll(dirPath)
		tb.Fatalf("DB.Close() failed: %v", err)
	}
	if err := os.RemoveAll(dirPath); err != nil {
		tb.Fatalf("os.RemoveAll() failed: %v", err)
	}
}

// Tests.

func TestDB(t *testing.T) {
	dirPath, dbPath, db := createTempDB(t)
	defer os.RemoveAll(dirPath)
	if err := db.Close(); err != nil {
		t.Fatalf("DB.Close() failed: %v", err)
	}
	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB() failed: %v", err)
	}
	defer db.Close()
	db2, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB() failed: %v", err)
	}
	defer db2.Close()
}

func TestDBRefresh(t *testing.T) {
	dirPath, _, db, _, _ := createTempColumn(t, "Table", nil, "Value", "Bool", nil)
	defer removeTempDB(t, dirPath, db)
	if _, err := db.Query("column_remove Table Value"); err != nil {
		t.Fatalf("DB.Query() failed: %v", err)
	}
	if err := db.Refresh(); err != nil {
		t.Fatalf("DB.Refresh() failed: %v", err)
	}
	if _, err := db.FindTable("Table"); err != nil {
		t.Fatalf("DB.FindTable() failed: %v", err)
	}
	if _, err := db.FindColumn("Table", "Column"); err == nil {
		t.Fatalf("DB.FindColumn() succeeded for deleted column")
	}
	if _, err := db.Query("table_remove Table"); err != nil {
		t.Fatalf("DB.Query() failed: %v", err)
	}
	if err := db.Refresh(); err != nil {
		t.Fatalf("DB.Refresh() failed: %v", err)
	}
	if _, err := db.FindTable("Table"); err == nil {
		t.Fatalf("DB.FindTable() succeeded for deleted table")
	}
}

func testKeyValue(t *testing.T, db *DB, keyType, valueType string) bool {
	options := NewTableOptions()
	options.KeyType = keyType
	options.ValueType = valueType
	table, err := db.CreateTable("Table", options)
	if err != nil {
		t.Log("DB.CreateTable() failed:", err)
		return false
	}
	defer db.Query("table_remove Table")
	keyColumn, err := table.FindColumn("_key")
	if (keyType == "") && (err == nil) {
		t.Log("Table.FindColumn() succeeded for non-existent _key")
		return false
	}
	if (keyType != "") && (err != nil) {
		t.Log("Table.FindColumn() failed", err)
		return false
	}
	valueColumn, err := table.FindColumn("_value")
	if (valueType == "") && (err == nil) {
		t.Log("Table.FindColumn() succeeded for non-existent _value")
		return false
	}
	if (valueType != "") && (err != nil) {
		t.Log("Table.FindColumn() failed:", err)
		return false
	}
	for i := 0; i < 100; i++ {
		key := generateRandomKey(keyType)
		_, id, err := table.InsertRow(key)
		if err != nil {
			t.Log("Table.InsertRow() failed:", err)
			return false
		}
		if keyColumn != nil {
			storedKey, err := keyColumn.GetValue(id)
			if err != nil {
				t.Log("Column.GetValue() failed:", err)
				return false
			}
			if !reflect.DeepEqual(key, storedKey) {
				t.Logf("DeepEqual() failed: key = %v, storedKey = %v", key, storedKey)
				return false
			}
		}
		if valueColumn != nil {
			value := generateRandomValue(valueType)
			if err := valueColumn.SetValue(id, value); err != nil {
				t.Log("Column.SetValue() failed:", err)
				return false
			}
			storedValue, err := valueColumn.GetValue(id)
			if err != nil {
				t.Log("Column.GetValue() failed:", err)
				return false
			}
			if !reflect.DeepEqual(value, storedValue) {
				t.Logf("DeepEqual() failed: value = %v, storedValue = %v",
					value, storedValue)
				return false
			}
		}
	}
	return true
}

func TestKeyValue(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	keyTypes := []string{
		"", "Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32",
		"UInt64", "Float", "Time", "ShortText", "TokyoGeoPoint", "WGS84GeoPoint",
	}
	valueTypes := []string{
		"", "Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32",
		"UInt64", "Float", "Time", "TokyoGeoPoint", "WGS84GeoPoint",
	}
	for _, keyType := range keyTypes {
		for _, valueType := range valueTypes {
			if !testKeyValue(t, db, keyType, valueType) {
				t.Logf("[ fail ] keyType = \"%s\", valueType = \"%s\"",
					keyType, valueType)
				t.Fail()
			}
		}
	}
}

func testRefKey(t *testing.T, db *DB, depth int, keyType string) bool {
	for i := depth; i > 0; i-- {
		tableName := fmt.Sprintf("Table%d", i)
		options := NewTableOptions()
		options.KeyType = keyType
		_, err := db.CreateTable(tableName, options)
		if err != nil {
			t.Log("DB.CreateTable() failed:", err)
			return false
		}
		defer db.Query(fmt.Sprintf("table_remove %s", tableName))
	}
	options := NewTableOptions()
	options.KeyType = "Table1"
	table, err := db.CreateTable("Table", options)
	if err != nil {
		t.Log("DB.CreateTable() failed:", err)
		return false
	}
	defer db.Query("table_remove Table")
	keyColumn, err := table.FindColumn("_key")
	if err != nil {
		t.Log("Table.FindColumn() failed:", err)
		return false
	}
	for i := 0; i < 100; i++ {
		key := generateRandomKey(keyType)
		_, id, err := table.InsertRow(key)
		if err != nil {
			t.Log("Table.InsertRow() failed:", err)
			return false
		}
		storedKey, err := keyColumn.GetValue(id)
		if err != nil {
			t.Log("Column.GetValue() failed:", err)
			return false
		}
		if !reflect.DeepEqual(key, storedKey) {
			t.Logf("DeepEqual() failed: key = %v, storedKey = %v", key, storedKey)
			return false
		}
	}
	return true
}

func TestRefKey(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	keyTypes := []string{
		"Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32",
		"UInt64", "Float", "Time", "ShortText", "TokyoGeoPoint", "WGS84GeoPoint",
	}
	maxDepth := 3
	for depth := 1; depth <= maxDepth; depth++ {
		for _, keyType := range keyTypes {
			if !testRefKey(t, db, depth, keyType) {
				t.Logf("[ fail ] depth = %d, keyType = \"%s\"", depth, keyType)
				t.Fail()
			}
		}
	}
}

func testColumn(t *testing.T, table *Table, valueType string, ids []uint32) bool {
	columnName := valueType
	if strings.HasPrefix(valueType, "[]") {
		columnName = valueType[2:] + "Vector"
	}
	columnName += "Value"
	column, err := table.CreateColumn(columnName, valueType, nil)
	if err != nil {
		t.Log("Table.CreateColumn() failed:", err)
		return false
	}
	for _, id := range ids {
		value := generateRandomValue(valueType)
		if err := column.SetValue(id, value); err != nil {
			t.Log("Column.SetValue() failed:", err)
			return false
		}
		storedValue, err := column.GetValue(id)
		if err != nil {
			t.Log("Column.GetValue() failed:", err)
			return false
		}
		if !reflect.DeepEqual(value, storedValue) {
			t.Logf("DeepEqual() failed: value = %v, storedValue = %v",
				value, storedValue)
			return false
		}
	}
	return true
}

func TestColumn(t *testing.T) {
	dirPath, _, db, table := createTempTable(t, "Table", nil)
	defer removeTempDB(t, dirPath, db)
	valueTypes := []string{
		"Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32",
		"UInt64", "Float", "Time", "ShortText", "Text", "LongText",
		"TokyoGeoPoint", "WGS84GeoPoint",
		"[]Bool", "[]Int8", "[]Int16", "[]Int32", "[]Int64", "[]UInt8", "[]UInt16",
		"[]UInt32", "[]UInt64", "[]Float", "[]Time", "[]ShortText", "[]Text",
		"[]LongText", "[]TokyoGeoPoint", "[]WGS84GeoPoint",
	}
	ids := make([]uint32, 100)
	for i := 0; i < len(ids); i++ {
		inserted, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
		if !inserted {
			t.Fatalf("Table.InsertRow() failed")
		}
		ids[i] = id
	}
	for _, valueType := range valueTypes {
		if !testColumn(t, table, valueType, ids) {
			t.Logf("[ fail ] valueType = \"%s\"", valueType)
			t.Fail()
		}
	}
}

func testRefColumn(t *testing.T, db *DB, keyType, refType string) bool {
	// Create a referred table.
	options := NewTableOptions()
	options.KeyType = keyType
	_, err := db.CreateTable("Table", options)
	if err != nil {
		t.Log("DB.CreateTable() failed:", err)
		return false
	}
	defer db.Query("table_remove Table")

	// Create a referrer table.
	refTable, err := db.CreateTable("RefTable", nil)
	if err != nil {
		t.Log("DB.CreateTable() failed:", err)
		return false
	}
	defer db.Query("table_remove RefTable")
	refColumn, err := refTable.CreateColumn("Ref", refType, nil)
	if err != nil {
		t.Log("Table.CreateColumn() failed:", err)
		return false
	}
	valueType := strings.Replace(refType, "Table", keyType, 1)

	for i := 0; i < 10; i++ {
		_, id, err := refTable.InsertRow(nil)
		if err != nil {
			t.Log("Table.InsertRow() failed:", err)
			return false
		}
		value := generateRandomValue(valueType)
		if err := refColumn.SetValue(id, value); err != nil {
			t.Log("Column.SetValue() failed:", err)
			return false
		}
		storedValue, err := refColumn.GetValue(id)
		if err != nil {
			t.Log("Column.GetValue() failed:", err)
			return false
		}
		if !reflect.DeepEqual(value, storedValue) {
			t.Logf("DeepEqual() failed: value = %v, storedValue = %v",
				value, storedValue)
			return false
		}
	}
	return true
}

func TestRefColumn(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	keyTypes := []string{
		"Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32",
		"UInt64", "Float", "Time", "ShortText",
		//"TokyoGeoPoint", "WGS84GeoPoint",
	}
	refTypes := []string{"Table"}
	//refTypes := []string{ "Table", "[]Table" }
	for _, keyType := range keyTypes {
		for _, refType := range refTypes {
			if !testRefColumn(t, db, keyType, refType) {
				t.Logf("[ fail ] keyType = \"%s\", refType = \"%s\"", keyType, refType)
				t.Fail()
			}
		}
	}
}

func TestInvalidRows(t *testing.T) {
	dirPath, _, db, table, column :=
		createTempColumn(t, "Table", nil, "Value", "Int32", nil)
	defer removeTempDB(t, dirPath, db)
	for id := uint32(1); id <= 100; id++ {
		if _, err := column.GetValue(id); err == nil {
			t.Fatalf("Column.GetValue() succeeded for an invalid row")
		}
		if err := column.SetValue(id, int64(id)+100); err == nil {
			t.Fatalf("Column.SetValue() succeeded for an invalid row")
		}
	}
	for id := uint32(1); id <= 100; id++ {
		_, _, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
	}
	for id := uint32(2); id <= 100; id += 2 {
		_, err := db.Query("delete Table --id " + strconv.Itoa(int(id)))
		if err != nil {
			t.Fatalf("DB.Query() failed: %v", err)
		}
	}
	for id := uint32(1); id <= 100; id++ {
		if (id % 2) == 0 {
			if _, err := column.GetValue(id); err == nil {
				t.Fatalf("Column.GetValue() succeeded for an invalid row")
			}
			if err := column.SetValue(id, int64(id)+200); err == nil {
				t.Fatalf("Column.SetValue() succeeded for an invalid row")
			}
		} else {
			if _, err := column.GetValue(id); err != nil {
				t.Fatalf("Column.GetValue() failed: %v", err)
			}
			if err := column.SetValue(id, int64(id)+200); err != nil {
				t.Fatalf("Column.GetValue() failed: %v", err)
			}
		}
	}
}

func TestRef(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	options := NewTableOptions()
	options.KeyType = "ShortText"
	table, err := db.CreateTable("Table", options)
	if err != nil {
		t.Fatalf("DB.CreateTable() failed: %v", err)
	}
	for i := 0; i < 100; i++ {
		key := []byte(strconv.Itoa(i))
		if _, _, err := table.InsertRow(key); err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
	}
	column, err := table.CreateColumn("Ref", "Table", nil)
	for i := 0; i < 100; i++ {
		id := uint32(i + 1)
		value := []byte(strconv.Itoa((i + 1) % 100))
		if err := column.SetValue(id, value); err != nil {
			t.Fatalf("Column.SetValue() failed: %v", err)
		}
		storedValue, err := column.GetValue(id)
		if err != nil {
			t.Fatalf("Column.GetValue() failed: %v", err)
		}
		if !reflect.DeepEqual(value, storedValue) {
			t.Fatalf("Column.GetValue() failed: value = %v, storedValue = %v",
				value, storedValue)
		}
	}
	column, err = table.FindColumn("Ref._key")
	for i := 0; i < 100; i++ {
		id := uint32(i + 1)
		value := []byte(strconv.Itoa((i + 1) % 100))
		storedValue, err := column.GetValue(id)
		if err != nil {
			t.Fatalf("Column.GetValue() failed: %v", err)
		}
		if !reflect.DeepEqual(value, storedValue) {
			t.Fatalf("Column.GetValue() failed: value = %v, storedValue = %v",
				value, storedValue)
		}
	}
	column, err = table.FindColumn("Ref.Ref.Ref")
	for i := 0; i < 100; i++ {
		id := uint32(i + 1)
		value := []byte(strconv.Itoa((i + 3) % 100))
		storedValue, err := column.GetValue(id)
		if err != nil {
			t.Fatalf("Column.GetValue() failed: %v", err)
		}
		if !reflect.DeepEqual(value, storedValue) {
			t.Fatalf("Column.GetValue() failed: value = %v, storedValue = %v",
				value, storedValue)
		}
	}
}

func TestRefs(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	options := NewTableOptions()
	options.KeyType = "ShortText"
	table, err := db.CreateTable("Table", options)
	if err != nil {
		t.Fatalf("DB.CreateTable() failed: %v", err)
	}
	for i := 0; i < 100; i++ {
		key := []byte(strconv.Itoa(i))
		if _, _, err := table.InsertRow(key); err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
	}
	column, err := table.CreateColumn("Value", "Float", nil)
	for i := 0; i < 100; i++ {
		id := uint32(i + 1)
		value := float64(i) / 10.0
		if err := column.SetValue(id, value); err != nil {
			t.Fatalf("Column.SetValue() failed: %v", err)
		}
	}
	column, err = table.CreateColumn("Ref", "[]Table", nil)
	for i := 0; i < 100; i++ {
		id := uint32(i + 1)
		value := [][]byte{
			[]byte(strconv.Itoa((i + 1) % 100)),
			[]byte(strconv.Itoa((i + 2) % 100)),
			[]byte(strconv.Itoa((i + 3) % 100)),
		}
		if err := column.SetValue(id, value); err != nil {
			t.Fatalf("Column.SetValue() failed: %v", err)
		}
		storedValue, err := column.GetValue(id)
		if err != nil {
			t.Fatalf("Column.GetValue() failed: %v", err)
		}
		if !reflect.DeepEqual(value, storedValue) {
			t.Fatalf("Column.GetValue() failed: value = %v, storedValue = %v",
				value, storedValue)
		}
	}
	column, err = table.FindColumn("Ref")
	for i := 0; i < 100; i++ {
		id := uint32(i + 1)
		value := [][]byte{
			[]byte(strconv.Itoa((i + 1) % 100)),
			[]byte(strconv.Itoa((i + 2) % 100)),
			[]byte(strconv.Itoa((i + 3) % 100)),
		}
		storedValue, err := column.GetValue(id)
		if err != nil {
			t.Fatalf("Column.GetValue() failed: %v", err)
		}
		if !reflect.DeepEqual(value, storedValue) {
			t.Fatalf("Column.GetValue() failed: value = %v, storedValue = %v",
				value, storedValue)
		}
	}
	column, err = table.FindColumn("Ref._key")
	for i := 0; i < 100; i++ {
		id := uint32(i + 1)
		value := [][]byte{
			[]byte(strconv.Itoa((i + 1) % 100)),
			[]byte(strconv.Itoa((i + 2) % 100)),
			[]byte(strconv.Itoa((i + 3) % 100)),
		}
		storedValue, err := column.GetValue(id)
		if err != nil {
			t.Fatalf("Column.GetValue() failed: %v", err)
		}
		if !reflect.DeepEqual(value, storedValue) {
			t.Fatalf("Column.GetValue() failed: value = %v, storedValue = %v",
				value, storedValue)
		}
	}
	column, err = table.FindColumn("Ref.Value")
	for i := 0; i < 100; i++ {
		id := uint32(i + 1)
		value := []float64{
			float64((i+1)%100) / 10.0,
			float64((i+2)%100) / 10.0,
			float64((i+3)%100) / 10.0,
		}
		storedValue, err := column.GetValue(id)
		if err != nil {
			t.Fatalf("Column.GetValue() failed: %v", err)
		}
		if !reflect.DeepEqual(value, storedValue) {
			t.Fatalf("Column.GetValue() failed: value = %v, storedValue = %v",
				value, storedValue)
		}
	}
}

func TestDeepVector(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	options := NewTableOptions()
	options.KeyType = "ShortText"
	table, err := db.CreateTable("Table", options)
	if err != nil {
		t.Fatalf("DB.CreateTable() failed: %v", err)
	}
	var keys [][]byte
	keys = append(keys, []byte("ABC"))
	keys = append(keys, []byte("DEF"))
	keys = append(keys, []byte("GHI"))
	for _, key := range keys {
		if _, _, err := table.InsertRow(key); err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
	}
	column, err := table.CreateColumn("Ref", "[]Table", nil)
	var values [][][]byte
	values = append(values, [][]byte{keys[1], keys[2]})
	values = append(values, [][]byte{keys[2], keys[0]})
	values = append(values, [][]byte{keys[0], keys[1]})
	for i, value := range values {
		if err := column.SetValue(uint32(i+1), value); err != nil {
			t.Fatalf("Column.SetValue() failed: %v", err)
		}
	}
	column, err = table.FindColumn("Ref.Ref")
	if err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	}
	for i := 0; i < len(keys); i++ {
		id := uint32(i + 1)
		storedValue, err := column.GetValue(id)
		if err != nil {
			t.Fatalf("Column.GetValue() failed: %v", err)
		}
		var value [][][]byte
		value = append(value, values[(i+1)%len(keys)])
		value = append(value, values[(i+2)%len(keys)])
		if !reflect.DeepEqual(value, storedValue) {
			t.Fatalf("Column.GetValue() failed: value = %v, storedValue = %v",
				value, storedValue)
		}
	}
	column, err = table.FindColumn("Ref.Ref.Ref.Ref.Ref")
	if err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	}
	storedValue, err := column.GetValue(uint32(1))
	if err != nil {
		t.Fatalf("Column.GetValue() failed: %v", err)
	}
	t.Logf("Ref.Ref.Ref.Ref.Ref: %v", storedValue)
}

// Benchmarks.

var numTestRows = 100000

func benchmarkColumnSetValue(b *testing.B, valueType string) {
	b.StopTimer()
	dirPath, _, db, table :=
		createTempTable(b, "Table", nil)
	defer removeTempDB(b, dirPath, db)
	ids := make([]uint32, numTestRows)
	values := make([]interface{}, numTestRows)
	for i, _ := range ids {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			b.Fatalf("Table.InsertRow() failed: %s", err)
		}
		ids[i] = id
		values[i] = generateRandomValue(valueType)
	}

	for i := 0; i < b.N; i++ {
		column, err := table.CreateColumn(fmt.Sprintf("V%d", i), valueType, nil)
		if err != nil {
			b.Fatalf("Table.CreateColumn() failed(): %s", err)
		}
		b.StartTimer()
		for i, id := range ids {
			if err := column.SetValue(id, values[i]); err != nil {
				b.Fatalf("Column.SetValue() failed: %s", err)
			}
			ids[i] = id
		}
		b.StopTimer()
	}
}

func BenchmarkColumnSetValueForBool(b *testing.B) {
	benchmarkColumnSetValue(b, "Bool")
}

func BenchmarkColumnSetValueForInt8(b *testing.B) {
	benchmarkColumnSetValue(b, "Int8")
}

func BenchmarkColumnSetValueForInt16(b *testing.B) {
	benchmarkColumnSetValue(b, "Int16")
}

func BenchmarkColumnSetValueForInt32(b *testing.B) {
	benchmarkColumnSetValue(b, "Int32")
}

func BenchmarkColumnSetValueForInt64(b *testing.B) {
	benchmarkColumnSetValue(b, "Int64")
}

func BenchmarkColumnSetValueForUInt8(b *testing.B) {
	benchmarkColumnSetValue(b, "UInt8")
}

func BenchmarkColumnSetValueForUInt16(b *testing.B) {
	benchmarkColumnSetValue(b, "UInt16")
}

func BenchmarkColumnSetValueForUInt32(b *testing.B) {
	benchmarkColumnSetValue(b, "UInt32")
}

func BenchmarkColumnSetValueForUInt64(b *testing.B) {
	benchmarkColumnSetValue(b, "UInt64")
}

func BenchmarkColumnSetValueForFloat(b *testing.B) {
	benchmarkColumnSetValue(b, "Float")
}

func BenchmarkColumnSetValueForTime(b *testing.B) {
	benchmarkColumnSetValue(b, "Time")
}

func BenchmarkColumnSetValueForShortText(b *testing.B) {
	benchmarkColumnSetValue(b, "ShortText")
}

func BenchmarkColumnSetValueForText(b *testing.B) {
	benchmarkColumnSetValue(b, "Text")
}

func BenchmarkColumnSetValueForLongText(b *testing.B) {
	benchmarkColumnSetValue(b, "LongText")
}

func BenchmarkColumnSetValueForTokyoGeoPoint(b *testing.B) {
	benchmarkColumnSetValue(b, "TokyoGeoPoint")
}

func BenchmarkColumnSetValueForWGS84GeoPoint(b *testing.B) {
	benchmarkColumnSetValue(b, "WGS84GeoPoint")
}

func BenchmarkColumnSetValueForBoolVector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]Bool")
}

func BenchmarkColumnSetValueForInt8Vector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]Int8")
}

func BenchmarkColumnSetValueForInt16Vector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]Int16")
}

func BenchmarkColumnSetValueForInt32Vector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]Int32")
}

func BenchmarkColumnSetValueForInt64Vector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]Int64")
}

func BenchmarkColumnSetValueForUInt8Vector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]UInt8")
}

func BenchmarkColumnSetValueForUInt16Vector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]UInt16")
}

func BenchmarkColumnSetValueForUInt32Vector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]UInt32")
}

func BenchmarkColumnSetValueForUInt64Vector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]UInt64")
}

func BenchmarkColumnSetValueForFloatVector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]Float")
}

func BenchmarkColumnSetValueForTimeVector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]Time")
}

func BenchmarkColumnSetValueForShortTextVector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]ShortText")
}

func BenchmarkColumnSetValueForTextVector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]Text")
}

func BenchmarkColumnSetValueForLongTextVector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]LongText")
}

func BenchmarkColumnSetValueForTokyoGeoPointVector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]TokyoGeoPoint")
}

func BenchmarkColumnSetValueForWGS84GeoPointVector(b *testing.B) {
	benchmarkColumnSetValue(b, "[]WGS84GeoPoint")
}

func benchmarkColumnGetValue(b *testing.B, valueType string) {
	dirPath, _, db, table, column :=
		createTempColumn(b, "Table", nil, "Value", valueType, nil)
	defer removeTempDB(b, dirPath, db)
	ids := make([]uint32, numTestRows)
	for i, _ := range ids {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			b.Fatalf("Table.InsertRow() failed: %s", err)
		}
		if err := column.SetValue(id, generateRandomValue(valueType)); err != nil {
			b.Fatalf("Column.SetValue() failed: %s", err)
		}
		ids[i] = id
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, id := range ids {
			if _, err := column.GetValue(id); err != nil {
				b.Fatalf("Column.GetValue() failed: %s", err)
			}
		}
	}
}

func BenchmarkColumnGetValueForBool(b *testing.B) {
	benchmarkColumnGetValue(b, "Bool")
}

func BenchmarkColumnGetValueForInt8(b *testing.B) {
	benchmarkColumnGetValue(b, "Int8")
}

func BenchmarkColumnGetValueForInt16(b *testing.B) {
	benchmarkColumnGetValue(b, "Int16")
}

func BenchmarkColumnGetValueForInt32(b *testing.B) {
	benchmarkColumnGetValue(b, "Int32")
}

func BenchmarkColumnGetValueForInt64(b *testing.B) {
	benchmarkColumnGetValue(b, "Int64")
}

func BenchmarkColumnGetValueForUInt8(b *testing.B) {
	benchmarkColumnGetValue(b, "UInt8")
}

func BenchmarkColumnGetValueForUInt16(b *testing.B) {
	benchmarkColumnGetValue(b, "UInt16")
}

func BenchmarkColumnGetValueForUInt32(b *testing.B) {
	benchmarkColumnGetValue(b, "UInt32")
}

func BenchmarkColumnGetValueForUInt64(b *testing.B) {
	benchmarkColumnGetValue(b, "UInt64")
}

func BenchmarkColumnGetValueForFloat(b *testing.B) {
	benchmarkColumnGetValue(b, "Float")
}

func BenchmarkColumnGetValueForTime(b *testing.B) {
	benchmarkColumnGetValue(b, "Time")
}

func BenchmarkColumnGetValueForShortText(b *testing.B) {
	benchmarkColumnGetValue(b, "ShortText")
}

func BenchmarkColumnGetValueForText(b *testing.B) {
	benchmarkColumnGetValue(b, "Text")
}

func BenchmarkColumnGetValueForLongText(b *testing.B) {
	benchmarkColumnGetValue(b, "LongText")
}

func BenchmarkColumnGetValueForTokyoGeoPoint(b *testing.B) {
	benchmarkColumnGetValue(b, "TokyoGeoPoint")
}

func BenchmarkColumnGetValueForWGS84GeoPoint(b *testing.B) {
	benchmarkColumnGetValue(b, "WGS84GeoPoint")
}

func BenchmarkColumnGetValueForBoolVector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]Bool")
}

func BenchmarkColumnGetValueForInt8Vector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]Int8")
}

func BenchmarkColumnGetValueForInt16Vector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]Int16")
}

func BenchmarkColumnGetValueForInt32Vector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]Int32")
}

func BenchmarkColumnGetValueForInt64Vector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]Int64")
}

func BenchmarkColumnGetValueForUInt8Vector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]UInt8")
}

func BenchmarkColumnGetValueForUInt16Vector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]UInt16")
}

func BenchmarkColumnGetValueForUInt32Vector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]UInt32")
}

func BenchmarkColumnGetValueForUInt64Vector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]UInt64")
}

func BenchmarkColumnGetValueForFloatVector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]Float")
}

func BenchmarkColumnGetValueForTimeVector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]Time")
}

func BenchmarkColumnGetValueForShortTextVector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]ShortText")
}

func BenchmarkColumnGetValueForTextVector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]Text")
}

func BenchmarkColumnGetValueForLongTextVector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]LongText")
}

func BenchmarkColumnGetValueForTokyoGeoPointVector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]TokyoGeoPoint")
}

func BenchmarkColumnGetValueForWGS84GeoPointVector(b *testing.B) {
	benchmarkColumnGetValue(b, "[]WGS84GeoPoint")
}

func benchmarkDBSelect(b *testing.B, valueType string) {
	dirPath, _, db, table, column :=
		createTempColumn(b, "Table", nil, "Value", valueType, nil)
	defer removeTempDB(b, dirPath, db)
	ids := make([]uint32, numTestRows)
	for i, _ := range ids {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			b.Fatalf("Table.InsertRow() failed: %s", err)
		}
		if err := column.SetValue(id, generateRandomValue(valueType)); err != nil {
			b.Fatalf("Column.SetValue() failed: %s", err)
		}
		ids[i] = id
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Query("select Table --output_columns Value --limit -1 --cache no")
		if err != nil {
			b.Fatalf("DB.Query() failed: %s", err)
		}
	}
}

func BenchmarkDBSelectForBool(b *testing.B) {
	benchmarkDBSelect(b, "Bool")
}

func BenchmarkDBSelectForInt8(b *testing.B) {
	benchmarkDBSelect(b, "Int8")
}

func BenchmarkDBSelectForInt16(b *testing.B) {
	benchmarkDBSelect(b, "Int16")
}

func BenchmarkDBSelectForInt32(b *testing.B) {
	benchmarkDBSelect(b, "Int32")
}

func BenchmarkDBSelectForInt64(b *testing.B) {
	benchmarkDBSelect(b, "Int64")
}

func BenchmarkDBSelectForUInt8(b *testing.B) {
	benchmarkDBSelect(b, "UInt8")
}

func BenchmarkDBSelectForUInt16(b *testing.B) {
	benchmarkDBSelect(b, "UInt16")
}

func BenchmarkDBSelectForUInt32(b *testing.B) {
	benchmarkDBSelect(b, "UInt32")
}

func BenchmarkDBSelectForUInt64(b *testing.B) {
	benchmarkDBSelect(b, "UInt64")
}

func BenchmarkDBSelectForFloat(b *testing.B) {
	benchmarkDBSelect(b, "Float")
}

func BenchmarkDBSelectForTime(b *testing.B) {
	benchmarkDBSelect(b, "Time")
}

func BenchmarkDBSelectForShortText(b *testing.B) {
	benchmarkDBSelect(b, "ShortText")
}

func BenchmarkDBSelectForText(b *testing.B) {
	benchmarkDBSelect(b, "Text")
}

func BenchmarkDBSelectForLongText(b *testing.B) {
	benchmarkDBSelect(b, "LongText")
}

func BenchmarkDBSelectForTokyoGeoPoint(b *testing.B) {
	benchmarkDBSelect(b, "TokyoGeoPoint")
}

func BenchmarkDBSelectForWGS84GeoPoint(b *testing.B) {
	benchmarkDBSelect(b, "WGS84GeoPoint")
}

func BenchmarkDBSelectForBoolVector(b *testing.B) {
	benchmarkDBSelect(b, "[]Bool")
}

func BenchmarkDBSelectForInt8Vector(b *testing.B) {
	benchmarkDBSelect(b, "[]Int8")
}

func BenchmarkDBSelectForInt16Vector(b *testing.B) {
	benchmarkDBSelect(b, "[]Int16")
}

func BenchmarkDBSelectForInt32Vector(b *testing.B) {
	benchmarkDBSelect(b, "[]Int32")
}

func BenchmarkDBSelectForInt64Vector(b *testing.B) {
	benchmarkDBSelect(b, "[]Int64")
}

func BenchmarkDBSelectForUInt8Vector(b *testing.B) {
	benchmarkDBSelect(b, "[]UInt8")
}

func BenchmarkDBSelectForUInt16Vector(b *testing.B) {
	benchmarkDBSelect(b, "[]UInt16")
}

func BenchmarkDBSelectForUInt32Vector(b *testing.B) {
	benchmarkDBSelect(b, "[]UInt32")
}

func BenchmarkDBSelectForUInt64Vector(b *testing.B) {
	benchmarkDBSelect(b, "[]UInt64")
}

func BenchmarkDBSelectForFloatVector(b *testing.B) {
	benchmarkDBSelect(b, "[]Float")
}

func BenchmarkDBSelectForTimeVector(b *testing.B) {
	benchmarkDBSelect(b, "[]Time")
}

func BenchmarkDBSelectForShortTextVector(b *testing.B) {
	benchmarkDBSelect(b, "[]ShortText")
}

func BenchmarkDBSelectForTextVector(b *testing.B) {
	benchmarkDBSelect(b, "[]Text")
}

func BenchmarkDBSelectForLongTextVector(b *testing.B) {
	benchmarkDBSelect(b, "[]LongText")
}

func BenchmarkDBSelectForTokyoGeoPointVector(b *testing.B) {
	benchmarkDBSelect(b, "[]TokyoGeoPoint")
}

func BenchmarkDBSelectForWGS84GeoPointVector(b *testing.B) {
	benchmarkDBSelect(b, "[]WGS84GeoPoint")
}
