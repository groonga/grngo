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

func TestCreateDB(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
}

func TestOpenDB(t *testing.T) {
	dirPath, dbPath, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
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
		t.Fatalf("DB.FindColumn() succeeded")
	}
	if _, err := db.Query("table_remove Table"); err != nil {
		t.Fatalf("DB.Query() failed: %v", err)
	}
	if err := db.Refresh(); err != nil {
		t.Fatalf("DB.Refresh() failed: %v", err)
	}
	if _, err := db.FindTable("Table"); err == nil {
		t.Fatalf("DB.FindTable() succeeded: %v", err)
	}
}

func TestDBCreateTableWithoutKeyValue(t *testing.T) {
	dirPath, _, db, _ := createTempTable(t, "Table", nil)
	defer removeTempDB(t, dirPath, db)
}

func testDBCreateTableWithKey(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.KeyType = keyType
	dirPath, _, db, _ := createTempTable(t, "Table", options)
	defer removeTempDB(t, dirPath, db)
}

func TestDBCreateTableWithBoolKey(t *testing.T) {
	testDBCreateTableWithKey(t, "Bool")
}

func TestDBCreateTableWithIntKey(t *testing.T) {
	testDBCreateTableWithKey(t, "Int64")
}

func TestDBCreateTableWithFloatKey(t *testing.T) {
	testDBCreateTableWithKey(t, "Float")
}

func TestDBCreateTableWithShortTextKey(t *testing.T) {
	testDBCreateTableWithKey(t, "ShortText")
}

func TestDBCreateTableWithTokyoGeoPointKey(t *testing.T) {
	testDBCreateTableWithKey(t, "TokyoGeoPoint")
}

func TestDBCreateTableWithWGS84GeoPointKey(t *testing.T) {
	testDBCreateTableWithKey(t, "WGS84GeoPoint")
}

func testDBCreateTableWithValue(t *testing.T, valueType string) {
	options := NewTableOptions()
	options.ValueType = valueType
	dirPath, _, db, _ := createTempTable(t, "Table", options)
	defer removeTempDB(t, dirPath, db)
}

func TestDBCreateTableWithBoolValue(t *testing.T) {
	testDBCreateTableWithValue(t, "Bool")
}

func TestDBCreateTableWithInt8Value(t *testing.T) {
	testDBCreateTableWithValue(t, "Int8")
}

func TestDBCreateTableWithInt16Value(t *testing.T) {
	testDBCreateTableWithValue(t, "Int16")
}

func TestDBCreateTableWithInt32Value(t *testing.T) {
	testDBCreateTableWithValue(t, "Int32")
}

func TestDBCreateTableWithInt64Value(t *testing.T) {
	testDBCreateTableWithValue(t, "Int64")
}

func TestDBCreateTableWithUInt8Value(t *testing.T) {
	testDBCreateTableWithValue(t, "UInt8")
}

func TestDBCreateTableWithUInt16Value(t *testing.T) {
	testDBCreateTableWithValue(t, "UInt16")
}

func TestDBCreateTableWithUInt32Value(t *testing.T) {
	testDBCreateTableWithValue(t, "UInt32")
}

func TestDBCreateTableWithUInt64Value(t *testing.T) {
	testDBCreateTableWithValue(t, "UInt64")
}

func TestDBCreateTableWithFloatValue(t *testing.T) {
	testDBCreateTableWithValue(t, "Float")
}

func TestDBCreateTableWithTimeValue(t *testing.T) {
	testDBCreateTableWithValue(t, "Time")
}

func TestDBCreateTableWithTokyoGeoPointValue(t *testing.T) {
	testDBCreateTableWithValue(t, "TokyoGeoPoint")
}

func TestDBCreateTableWithWGS84GeoPointValue(t *testing.T) {
	testDBCreateTableWithValue(t, "WGS84GeoPoint")
}

func testDBCreateTableWithRefKey(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.KeyType = keyType
	dirPath, _, db, _ := createTempTable(t, "To", options)
	defer removeTempDB(t, dirPath, db)

	options = NewTableOptions()
	options.KeyType = "To"
	_, err := db.CreateTable("From", options)
	if err != nil {
		t.Fatalf("DB.CreateTable() failed: %v", err)
	}
}

func TestDBCreateTableWithBoolRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Bool")
}

func TestDBCreateTableWithInt8RefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Int8")
}

func TestDBCreateTableWithInt16RefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Int16")
}

func TestDBCreateTableWithInt32RefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Int32")
}

func TestDBCreateTableWithInt64RefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Int64")
}

func TestDBCreateTableWithUInt8RefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "UInt8")
}

func TestDBCreateTableWithUInt16RefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "UInt16")
}

func TestDBCreateTableWithUInt32RefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "UInt32")
}

func TestDBCreateTableWithUInt64RefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "UInt64")
}

func TestDBCreateTableWithFloatRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Float")
}

func TestDBCreateTableWithTimeRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Time")
}

func TestDBCreateTableWithShortTextRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "ShortText")
}

func TestDBCreateTableWithTokyoGeoPointRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "TokyoGeoPoint")
}

func TestDBCreateTableWithWGS84GeoPointRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "WGS84GeoPoint")
}

func testDBCreateTableWithRefValue(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.KeyType = keyType
	dirPath, _, db, _ := createTempTable(t, "To", options)
	defer removeTempDB(t, dirPath, db)

	options = NewTableOptions()
	options.ValueType = ""
	_, err := db.CreateTable("From", options)
	if err != nil {
		t.Fatalf("DB.CreateTable() failed: %v", err)
	}
}

func TestDBCreateTableWithBoolRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Bool")
}

func TestDBCreateTableWithInt8RefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Int8")
}

func TestDBCreateTableWithInt16RefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Int16")
}

func TestDBCreateTableWithInt32RefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Int32")
}

func TestDBCreateTableWithInt64RefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Int64")
}

func TestDBCreateTableWithUInt8RefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "UInt8")
}

func TestDBCreateTableWithUInt16RefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "UInt16")
}

func TestDBCreateTableWithUInt32RefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "UInt32")
}

func TestDBCreateTableWithUInt64RefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "UInt64")
}

func TestDBCreateTableWithFloatRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Float")
}

func TestDBCreateTableWithTimeRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Time")
}

func TestDBCreateTableWithShortTextRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "ShortText")
}

func TestDBCreateTableWithTokyoGeoPointRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "TokyoGeoPoint")
}

func TestDBCreateTableWithWGS84GeoPointRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "WGS84GeoPoint")
}

func testTableInsertRow(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.KeyType = keyType
	dirPath, _, db, table := createTempTable(t, "Table", options)
	defer removeTempDB(t, dirPath, db)

	count := 0
	for i := 0; i < 100; i++ {
		inserted, _, err := table.InsertRow(generateRandomKey(keyType))
		if err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
		if inserted {
			count++
		}
	}
	t.Logf("keyType = <%s>, count = %d", keyType, count)
}

func TestTableInsertRowWithoutKey(t *testing.T) {
	testTableInsertRow(t, "")
}

func TestTableInsertRowWithBoolKey(t *testing.T) {
	testTableInsertRow(t, "Bool")
}

func TestTableInsertRowWithInt8Key(t *testing.T) {
	testTableInsertRow(t, "Int8")
}

func TestTableInsertRowWithInt16Key(t *testing.T) {
	testTableInsertRow(t, "Int16")
}

func TestTableInsertRowWithInt32Key(t *testing.T) {
	testTableInsertRow(t, "Int32")
}

func TestTableInsertRowWithInt64Key(t *testing.T) {
	testTableInsertRow(t, "Int64")
}

func TestTableInsertRowWithUInt8Key(t *testing.T) {
	testTableInsertRow(t, "UInt8")
}

func TestTableInsertRowWithUInt16Key(t *testing.T) {
	testTableInsertRow(t, "UInt16")
}

func TestTableInsertRowWithUInt32Key(t *testing.T) {
	testTableInsertRow(t, "UInt32")
}

func TestTableInsertRowWithUInt64Key(t *testing.T) {
	testTableInsertRow(t, "UInt64")
}

func TestTableInsertRowWithFloatKey(t *testing.T) {
	testTableInsertRow(t, "Float")
}

func TestTableInsertRowWithTimeKey(t *testing.T) {
	testTableInsertRow(t, "Time")
}

func TestTableInsertRowWithShortTextKey(t *testing.T) {
	testTableInsertRow(t, "ShortText")
}

func TestTableInsertRowWithTokyoGeoPointKey(t *testing.T) {
	testTableInsertRow(t, "TokyoGeoPoint")
}

func TestTableInsertRowWithWGS84GeoPointKey(t *testing.T) {
	testTableInsertRow(t, "WGS84GeoPoint")
}

func testTableCreateColumn(t *testing.T, valueType string) {
	dirPath, _, db, table, _ :=
		createTempColumn(t, "Table", nil, "Value", valueType, nil)
	defer removeTempDB(t, dirPath, db)

	if column, err := table.FindColumn("_id"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("_id: %+v", column)
	}
	if column, err := table.FindColumn("Value"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("Value: %+v", column)
	}
}

func TestTableCreateColumnForBool(t *testing.T) {
	testTableCreateColumn(t, "Bool")
}

func TestTableCreateColumnForInt8(t *testing.T) {
	testTableCreateColumn(t, "Int8")
}

func TestTableCreateColumnForInt16(t *testing.T) {
	testTableCreateColumn(t, "Int16")
}

func TestTableCreateColumnForInt32(t *testing.T) {
	testTableCreateColumn(t, "Int32")
}

func TestTableCreateColumnForInt64(t *testing.T) {
	testTableCreateColumn(t, "Int64")
}

func TestTableCreateColumnForUInt8(t *testing.T) {
	testTableCreateColumn(t, "UInt8")
}

func TestTableCreateColumnForUInt16(t *testing.T) {
	testTableCreateColumn(t, "UInt16")
}

func TestTableCreateColumnForUInt32(t *testing.T) {
	testTableCreateColumn(t, "UInt32")
}

func TestTableCreateColumnForUInt64(t *testing.T) {
	testTableCreateColumn(t, "UInt64")
}

func TestTableCreateColumnForFloat(t *testing.T) {
	testTableCreateColumn(t, "Float")
}

func TestTableCreateColumnForTime(t *testing.T) {
	testTableCreateColumn(t, "Time")
}

func TestTableCreateColumnForShortText(t *testing.T) {
	testTableCreateColumn(t, "ShortText")
}

func TestTableCreateColumnForText(t *testing.T) {
	testTableCreateColumn(t, "Text")
}

func TestTableCreateColumnForLongText(t *testing.T) {
	testTableCreateColumn(t, "LongText")
}

func TestTableCreateColumnForTokyoGeoPoint(t *testing.T) {
	testTableCreateColumn(t, "TokyoGeoPoint")
}

func TestTableCreateColumnForWGS84GeoPoint(t *testing.T) {
	testTableCreateColumn(t, "WGS84GeoPoint")
}

func TestTableCreateColumnForBoolVector(t *testing.T) {
	testTableCreateColumn(t, "[]Bool")
}

func TestTableCreateColumnForInt8Vector(t *testing.T) {
	testTableCreateColumn(t, "[]Int8")
}

func TestTableCreateColumnForInt16Vector(t *testing.T) {
	testTableCreateColumn(t, "[]Int16")
}

func TestTableCreateColumnForInt32Vector(t *testing.T) {
	testTableCreateColumn(t, "[]Int32")
}

func TestTableCreateColumnForInt64Vector(t *testing.T) {
	testTableCreateColumn(t, "[]Int64")
}

func TestTableCreateColumnForUInt8Vector(t *testing.T) {
	testTableCreateColumn(t, "[]UInt8")
}

func TestTableCreateColumnForUInt16Vector(t *testing.T) {
	testTableCreateColumn(t, "[]UInt16")
}

func TestTableCreateColumnForUInt32Vector(t *testing.T) {
	testTableCreateColumn(t, "[]UInt32")
}

func TestTableCreateColumnForUInt64Vector(t *testing.T) {
	testTableCreateColumn(t, "[]UInt64")
}

func TestTableCreateColumnForFloatVector(t *testing.T) {
	testTableCreateColumn(t, "[]Float")
}

func TestTableCreateColumnForTimeVector(t *testing.T) {
	testTableCreateColumn(t, "[]Time")
}

func TestTableCreateColumnForShortTextVector(t *testing.T) {
	testTableCreateColumn(t, "[]ShortText")
}

func TestTableCreateColumnForTextVector(t *testing.T) {
	testTableCreateColumn(t, "[]Text")
}

func TestTableCreateColumnForLongTextVector(t *testing.T) {
	testTableCreateColumn(t, "[]LongText")
}

func TestTableCreateColumnForTokyoGeoPointVector(t *testing.T) {
	testTableCreateColumn(t, "[]TokyoGeoPoint")
}

func TestTableCreateColumnForWGS84GeoPointVector(t *testing.T) {
	testTableCreateColumn(t, "[]WGS84GeoPoint")
}

func testTableCreateRefColumn(t *testing.T, keyType string) {
	valueType := "Table"
	if strings.HasPrefix(keyType, "[]") {
		keyType = keyType[2:]
		valueType = "[]Table"
	}
	options := NewTableOptions()
	options.KeyType = keyType
	dirPath, _, db, table, _ :=
		createTempColumn(t, "Table", options, "Value", valueType, nil)
	defer removeTempDB(t, dirPath, db)

	if column, err := table.FindColumn("Value"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("Value: %+v", column)
	}
	if column, err := table.FindColumn("Value._id"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("Value._id: %+v", column)
	}
	if column, err := table.FindColumn("Value._key"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("Value._key: %+v", column)
	}
}

func TestTableCreateColumnForRefToBool(t *testing.T) {
	testTableCreateRefColumn(t, "Bool")
}

func TestTableCreateColumnForRefToInt8(t *testing.T) {
	testTableCreateRefColumn(t, "Int8")
}

func TestTableCreateColumnForRefToInt16(t *testing.T) {
	testTableCreateRefColumn(t, "Int16")
}

func TestTableCreateColumnForRefToInt32(t *testing.T) {
	testTableCreateRefColumn(t, "Int32")
}

func TestTableCreateColumnForRefToInt64(t *testing.T) {
	testTableCreateRefColumn(t, "Int64")
}

func TestTableCreateColumnForRefToUInt8(t *testing.T) {
	testTableCreateRefColumn(t, "UInt8")
}

func TestTableCreateColumnForRefToUInt16(t *testing.T) {
	testTableCreateRefColumn(t, "UInt16")
}

func TestTableCreateColumnForRefToUInt32(t *testing.T) {
	testTableCreateRefColumn(t, "UInt32")
}

func TestTableCreateColumnForRefToUInt64(t *testing.T) {
	testTableCreateRefColumn(t, "UInt64")
}

func TestTableCreateColumnForRefToFloat(t *testing.T) {
	testTableCreateRefColumn(t, "Float")
}

func TestTableCreateColumnForRefToTime(t *testing.T) {
	testTableCreateRefColumn(t, "Time")
}

func TestTableCreateColumnForRefToShortText(t *testing.T) {
	testTableCreateRefColumn(t, "ShortText")
}

func TestTableCreateColumnForRefToTokyoGeoPoint(t *testing.T) {
	testTableCreateRefColumn(t, "TokyoGeoPoint")
}

func TestTableCreateColumnForRefToWGS84GeoPoint(t *testing.T) {
	testTableCreateRefColumn(t, "WGS84GeoPoint")
}

func TestTableCreateColumnForRefToBoolVector(t *testing.T) {
	testTableCreateRefColumn(t, "[]Bool")
}

func TestTableCreateColumnForRefToInt8Vector(t *testing.T) {
	testTableCreateRefColumn(t, "[]Int8")
}

func TestTableCreateColumnForRefToInt16Vector(t *testing.T) {
	testTableCreateRefColumn(t, "[]Int16")
}

func TestTableCreateColumnForRefToInt32Vector(t *testing.T) {
	testTableCreateRefColumn(t, "[]Int32")
}

func TestTableCreateColumnForRefToInt64Vector(t *testing.T) {
	testTableCreateRefColumn(t, "[]Int64")
}

func TestTableCreateColumnForRefToUInt8Vector(t *testing.T) {
	testTableCreateRefColumn(t, "[]UInt8")
}

func TestTableCreateColumnForRefToUInt16Vector(t *testing.T) {
	testTableCreateRefColumn(t, "[]UInt16")
}

func TestTableCreateColumnForRefToUInt32Vector(t *testing.T) {
	testTableCreateRefColumn(t, "[]UInt32")
}

func TestTableCreateColumnForRefToUInt64Vector(t *testing.T) {
	testTableCreateRefColumn(t, "[]UInt64")
}

func TestTableCreateColumnForRefToFloatVector(t *testing.T) {
	testTableCreateRefColumn(t, "[]Float")
}

func TestTableCreateColumnForRefToTimeVector(t *testing.T) {
	testTableCreateRefColumn(t, "[]Time")
}

func TestTableCreateColumnForRefToShortTextVector(t *testing.T) {
	testTableCreateRefColumn(t, "[]ShortText")
}

func TestTableCreateColumnForRefToTokyoGeoPointVector(t *testing.T) {
	testTableCreateRefColumn(t, "[]TokyoGeoPoint")
}

func TestTableCreateColumnForRefToWGS84GeoPointVector(t *testing.T) {
	testTableCreateRefColumn(t, "[]WGS84GeoPoint")
}

func testColumnSetValue(t *testing.T, valueType string) {
	dirPath, _, db, table, column :=
		createTempColumn(t, "Table", nil, "Value", valueType, nil)
	defer removeTempDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
		if err := column.SetValue(id, generateRandomValue(valueType)); err != nil {
			t.Fatalf("Column.SetValue() failed: %v", err)
		}
	}

	bytes, _ := db.Query("select Table --limit 3")
	t.Logf("valueType = <%s>, result = %s", valueType, string(bytes))
}

func TestColumnSetValueForBool(t *testing.T) {
	testColumnSetValue(t, "Bool")
}

func TestColumnSetValueForInt8(t *testing.T) {
	testColumnSetValue(t, "Int8")
}

func TestColumnSetValueForInt16(t *testing.T) {
	testColumnSetValue(t, "Int16")
}

func TestColumnSetValueForInt32(t *testing.T) {
	testColumnSetValue(t, "Int32")
}

func TestColumnSetValueForInt64(t *testing.T) {
	testColumnSetValue(t, "Int64")
}

func TestColumnSetValueForUInt8(t *testing.T) {
	testColumnSetValue(t, "UInt8")
}

func TestColumnSetValueForUInt16(t *testing.T) {
	testColumnSetValue(t, "UInt16")
}

func TestColumnSetValueForUInt32(t *testing.T) {
	testColumnSetValue(t, "UInt32")
}

func TestColumnSetValueForUInt64(t *testing.T) {
	testColumnSetValue(t, "UInt64")
}

func TestColumnSetValueForFloat(t *testing.T) {
	testColumnSetValue(t, "Float")
}

func TestColumnSetValueForTime(t *testing.T) {
	testColumnSetValue(t, "Time")
}

func TestColumnSetValueForShortText(t *testing.T) {
	testColumnSetValue(t, "ShortText")
}

func TestColumnSetValueForText(t *testing.T) {
	testColumnSetValue(t, "Text")
}

func TestColumnSetValueForLongText(t *testing.T) {
	testColumnSetValue(t, "LongText")
}

func TestColumnSetValueForTokyoGeoPoint(t *testing.T) {
	testColumnSetValue(t, "TokyoGeoPoint")
}

func TestColumnSetValueForWGS84GeoPoint(t *testing.T) {
	testColumnSetValue(t, "WGS84GeoPoint")
}

func TestColumnSetValueForBoolVector(t *testing.T) {
	testColumnSetValue(t, "[]Bool")
}

func TestColumnSetValueForInt8Vector(t *testing.T) {
	testColumnSetValue(t, "[]Int8")
}

func TestColumnSetValueForInt16Vector(t *testing.T) {
	testColumnSetValue(t, "[]Int16")
}

func TestColumnSetValueForInt32Vector(t *testing.T) {
	testColumnSetValue(t, "[]Int32")
}

func TestColumnSetValueForInt64Vector(t *testing.T) {
	testColumnSetValue(t, "[]Int64")
}

func TestColumnSetValueForUInt8Vector(t *testing.T) {
	testColumnSetValue(t, "[]UInt8")
}

func TestColumnSetValueForUInt16Vector(t *testing.T) {
	testColumnSetValue(t, "[]UInt16")
}

func TestColumnSetValueForUInt32Vector(t *testing.T) {
	testColumnSetValue(t, "[]UInt32")
}

func TestColumnSetValueForUInt64Vector(t *testing.T) {
	testColumnSetValue(t, "[]UInt64")
}

func TestColumnSetValueForFloatVector(t *testing.T) {
	testColumnSetValue(t, "[]Float")
}

func TestColumnSetValueForTimeVector(t *testing.T) {
	testColumnSetValue(t, "[]Time")
}

func TestColumnSetValueForShortTextVector(t *testing.T) {
	testColumnSetValue(t, "[]ShortText")
}

func TestColumnSetValueForTextVector(t *testing.T) {
	testColumnSetValue(t, "[]Text")
}

func TestColumnSetValueForLongTextVector(t *testing.T) {
	testColumnSetValue(t, "[]LongText")
}

func TestColumnSetValueForTokyoGeoPointVector(t *testing.T) {
	testColumnSetValue(t, "[]TokyoGeoPoint")
}

func TestColumnSetValueForWGS84GeoPointVector(t *testing.T) {
	testColumnSetValue(t, "[]WGS84GeoPoint")
}

func testColumnGetValue(t *testing.T, valueType string) {
	dirPath, _, db, table, column :=
		createTempColumn(t, "Table", nil, "Value", valueType, nil)
	defer removeTempDB(t, dirPath, db)
	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
		value := generateRandomValue(valueType)
		if err := column.SetValue(id, value); err != nil {
			t.Fatalf("Column.SetValue() failed: %v", err)
		}
		if storedValue, err := column.GetValue(id); err != nil {
			t.Fatalf("Column.GetValue() failed: %v", err)
		} else if !reflect.DeepEqual(value, storedValue) {
			t.Fatalf("Column.GetValue() failed: value = %v, storedValue = %v",
				value, storedValue)
		}
	}
}

func TestColumnGetValueForBool(t *testing.T) {
	testColumnGetValue(t, "Bool")
}

func TestColumnGetValueForInt8(t *testing.T) {
	testColumnGetValue(t, "Int8")
}

func TestColumnGetValueForInt16(t *testing.T) {
	testColumnGetValue(t, "Int16")
}

func TestColumnGetValueForInt32(t *testing.T) {
	testColumnGetValue(t, "Int32")
}

func TestColumnGetValueForInt64(t *testing.T) {
	testColumnGetValue(t, "Int64")
}

func TestColumnGetValueForUInt8(t *testing.T) {
	testColumnGetValue(t, "UInt8")
}

func TestColumnGetValueForUInt16(t *testing.T) {
	testColumnGetValue(t, "UInt16")
}

func TestColumnGetValueForUInt32(t *testing.T) {
	testColumnGetValue(t, "UInt32")
}

func TestColumnGetValueForUInt64(t *testing.T) {
	testColumnGetValue(t, "UInt64")
}

func TestColumnGetValueForFloat(t *testing.T) {
	testColumnGetValue(t, "Float")
}

func TestColumnGetValueForTime(t *testing.T) {
	testColumnGetValue(t, "Time")
}

func TestColumnGetValueForShortText(t *testing.T) {
	testColumnGetValue(t, "ShortText")
}

func TestColumnGetValueForText(t *testing.T) {
	testColumnGetValue(t, "Text")
}

func TestColumnGetValueForLongText(t *testing.T) {
	testColumnGetValue(t, "LongText")
}

func TestColumnGetValueForTokyoGeoPoint(t *testing.T) {
	testColumnGetValue(t, "TokyoGeoPoint")
}

func TestColumnGetValueForWGS84GeoPoint(t *testing.T) {
	testColumnGetValue(t, "WGS84GeoPoint")
}

func TestColumnGetValueForBoolVector(t *testing.T) {
	testColumnGetValue(t, "[]Bool")
}

func TestColumnGetValueForInt8Vector(t *testing.T) {
	testColumnGetValue(t, "[]Int8")
}

func TestColumnGetValueForInt16Vector(t *testing.T) {
	testColumnGetValue(t, "[]Int16")
}

func TestColumnGetValueForInt32Vector(t *testing.T) {
	testColumnGetValue(t, "[]Int32")
}

func TestColumnGetValueForInt64Vector(t *testing.T) {
	testColumnGetValue(t, "[]Int64")
}

func TestColumnGetValueForUInt8Vector(t *testing.T) {
	testColumnGetValue(t, "[]UInt8")
}

func TestColumnGetValueForUInt16Vector(t *testing.T) {
	testColumnGetValue(t, "[]UInt16")
}

func TestColumnGetValueForUInt32Vector(t *testing.T) {
	testColumnGetValue(t, "[]UInt32")
}

func TestColumnGetValueForUInt64Vector(t *testing.T) {
	testColumnGetValue(t, "[]UInt64")
}

func TestColumnGetValueForFloatVector(t *testing.T) {
	testColumnGetValue(t, "[]Float")
}

func TestColumnGetValueForTimeVector(t *testing.T) {
	testColumnGetValue(t, "[]Time")
}

func TestColumnGetValueForShortTextVector(t *testing.T) {
	testColumnGetValue(t, "[]ShortText")
}

func TestColumnGetValueForTextVector(t *testing.T) {
	testColumnGetValue(t, "[]Text")
}

func TestColumnGetValueForLongTextVector(t *testing.T) {
	testColumnGetValue(t, "[]LongText")
}

func TestColumnGetValueForTokyoGeoPointVector(t *testing.T) {
	testColumnGetValue(t, "[]TokyoGeoPoint")
}

func TestColumnGetValueForWGS84GeoPointVector(t *testing.T) {
	testColumnGetValue(t, "[]WGS84GeoPoint")
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
