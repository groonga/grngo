package grngo

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
)

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

func TestCreateDB(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	removeTempDB(t, dirPath, db)
}

func TestOpenDB(t *testing.T) {
	dirPath, dbPath, db := createTempDB(t)
	db2, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB() failed: %v", err)
	}
	db2.Close()
	removeTempDB(t, dirPath, db)
}

func testDBCreateTableWithKey(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.TableType = PatTable
	options.KeyType = keyType
	dirPath, _, db, _ := createTempTable(t, "Table", options)
	removeTempDB(t, dirPath, db)
}

func testDBCreateTableWithValue(t *testing.T, valueType string) {
	options := NewTableOptions()
	options.TableType = PatTable
	options.ValueType = valueType
	dirPath, _, db, _ := createTempTable(t, "Table", options)
	removeTempDB(t, dirPath, db)
}

func testDBCreateTableWithRefKey(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.TableType = PatTable
	options.KeyType = keyType
	dirPath, _, db, _ := createTempTable(t, "To", options)
	defer removeTempDB(t, dirPath, db)

	options = NewTableOptions()
	options.TableType = PatTable
	options.KeyType = "To"
	_, err := db.CreateTable("From", options)
	if err != nil {
		t.Fatalf("DB.CreateTable() failed: %v", err)
	}
}

func testDBCreateTableWithRefValue(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.TableType = PatTable
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

func TestDBCreateTableWithoutKeyValue(t *testing.T) {
	dirPath, _, db, _ := createTempTable(t, "Table", nil)
	removeTempDB(t, dirPath, db)
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

func TestDBCreateTableWithGeoPointKey(t *testing.T) {
	testDBCreateTableWithKey(t, "WGS84GeoPoint")
}

func TestDBCreateTableWithTextKey(t *testing.T) {
	testDBCreateTableWithKey(t, "ShortText")
}

func TestDBCreateTableWithBoolValue(t *testing.T) {
	testDBCreateTableWithValue(t, "Bool")
}

func TestDBCreateTableWithIntValue(t *testing.T) {
	testDBCreateTableWithValue(t, "Int64")
}

func TestDBCreateTableWithFloatValue(t *testing.T) {
	testDBCreateTableWithValue(t, "Float")
}

func TestDBCreateTableWithGeoPointValue(t *testing.T) {
	testDBCreateTableWithValue(t, "WGS84GeoPoint")
}

func TestDBCreateTableWithBoolRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Bool")
}

func TestDBCreateTableWithIntRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Int64")
}

func TestDBCreateTableWithFloatRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Float")
}

func TestDBCreateTableWithGeoPointRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "WGS84GeoPoint")
}

func TestDBCreateTableWithTextRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "ShortText")
}

func TestDBCreateTableWithBoolRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Bool")
}

func TestDBCreateTableWithIntRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Int64")
}

func TestDBCreateTableWithFloatRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Float")
}

func TestDBCreateTableWithGeoPointRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "WGS84GeoPoint")
}

func TestDBCreateTableWithTextRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "ShortText")
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
	case "WGS84GeoPoint":
		const (
			MinLatitude  = 73531000
			MaxLatitude  = 164006000
			MinLongitude = 439451000
			MaxLongitude = 554351000
		)
		latitude := MinLatitude + rand.Intn(MaxLatitude-MinLatitude+1)
		longitude := MinLongitude + rand.Intn(MaxLongitude-MinLongitude+1)
		return GeoPoint{int32(latitude), int32(longitude)}
	case "ShortText":
		return []byte(strconv.Itoa(rand.Int()))
	default:
		return nil
	}
}

func testTableInsertRow(t *testing.T, keyType string) {
	options := NewTableOptions()
	if keyType != "" {
		options.TableType = PatTable
	}
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

func TestTableInsertRowWithGeoPointKey(t *testing.T) {
	testTableInsertRow(t, "WGS84GeoPoint")
}

func TestTableInsertRowWithTextKey(t *testing.T) {
	testTableInsertRow(t, "ShortText")
}

func testTableCreateScalarColumn(t *testing.T, valueType string) {
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

func testTableCreateVectorColumn(t *testing.T, valueType string) {
	options := NewColumnOptions()
	options.ColumnType = VectorColumn
	dirPath, _, db, table, _ :=
		createTempColumn(t, "Table", nil, "Value", valueType, options)
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

func testTableCreateScalarRefColumn(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.TableType = PatTable
	options.KeyType = keyType
	dirPath, _, db, table, _ :=
		createTempColumn(t, "Table", options, "Value", "Table", nil)
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

func testTableCreateVectorRefColumn(t *testing.T, keyType string) {
	tableOptions := NewTableOptions()
	tableOptions.TableType = PatTable
	tableOptions.KeyType = keyType
	columnOptions := NewColumnOptions()
	columnOptions.ColumnType = VectorColumn
	dirPath, _, db, table, _ :=
		createTempColumn(t, "Table", tableOptions, "Value", "Table", columnOptions)
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

func TestTableCreateColumnForBool(t *testing.T) {
	testTableCreateScalarColumn(t, "Bool")
}

func TestTableCreateColumnForInt(t *testing.T) {
	testTableCreateScalarColumn(t, "Int64")
}

func TestTableCreateColumnForFloat(t *testing.T) {
	testTableCreateScalarColumn(t, "Float")
}

func TestTableCreateColumnForGeoPoint(t *testing.T) {
	testTableCreateScalarColumn(t, "WGS84GeoPoint")
}

func TestTableCreateColumnForText(t *testing.T) {
	testTableCreateScalarColumn(t, "ShortText")
}

func TestTableCreateColumnForBoolVector(t *testing.T) {
	testTableCreateVectorColumn(t, "Bool")
}

func TestTableCreateColumnForIntVector(t *testing.T) {
	testTableCreateVectorColumn(t, "Int64")
}

func TestTableCreateColumnForFloatVector(t *testing.T) {
	testTableCreateVectorColumn(t, "Float")
}

func TestTableCreateColumnForGeoPointVector(t *testing.T) {
	testTableCreateVectorColumn(t, "WGS84GeoPoint")
}

func TestTableCreateColumnForTextVector(t *testing.T) {
	testTableCreateVectorColumn(t, "ShortText")
}

func TestTableCreateColumnForRefToBool(t *testing.T) {
	testTableCreateScalarRefColumn(t, "Bool")
}

func TestTableCreateColumnForRefToInt(t *testing.T) {
	testTableCreateScalarRefColumn(t, "Int64")
}

func TestTableCreateColumnForRefToFloat(t *testing.T) {
	testTableCreateScalarRefColumn(t, "Float")
}

func TestTableCreateColumnForRefToGeoPoint(t *testing.T) {
	testTableCreateScalarRefColumn(t, "WGS84GeoPoint")
}

func TestTableCreateColumnForRefToText(t *testing.T) {
	testTableCreateScalarRefColumn(t, "ShortText")
}

func TestTableCreateColumnForRefToBoolVector(t *testing.T) {
	testTableCreateVectorRefColumn(t, "Bool")
}

func TestTableCreateColumnForRefToIntVector(t *testing.T) {
	testTableCreateVectorRefColumn(t, "Int64")
}

func TestTableCreateColumnForRefToFloatVector(t *testing.T) {
	testTableCreateVectorRefColumn(t, "Float")
}

func TestTableCreateColumnForRefToGeoPointVector(t *testing.T) {
	testTableCreateVectorRefColumn(t, "WGS84GeoPoint")
}

func TestTableCreateColumnForRefToTextVector(t *testing.T) {
	testTableCreateVectorRefColumn(t, "ShortText")
}

func generateRandomValue(valueType string) interface{} {
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
	case "WGS84GeoPoint":
		const (
			MinLatitude  = 73531000
			MaxLatitude  = 164006000
			MinLongitude = 439451000
			MaxLongitude = 554351000
		)
		latitude := MinLatitude + rand.Intn(MaxLatitude-MinLatitude+1)
		longitude := MinLongitude + rand.Intn(MaxLongitude-MinLongitude+1)
		return GeoPoint{int32(latitude), int32(longitude)}
	case "ShortText":
		return []byte(strconv.Itoa(rand.Int()))
	default:
		return nil
	}
}

func generateRandomVectorValue(valueType string) interface{} {
	size := rand.Int() % 10
	switch valueType {
	case "Bool":
		value := make([]bool, size)
		for i := 0; i < size; i++ {
			value[i] = (rand.Int() & 1) == 1
		}
		return value
	case "Int64":
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
	case "WGS84GeoPoint":
		const (
			MinLatitude  = 73531000
			MaxLatitude  = 164006000
			MinLongitude = 439451000
			MaxLongitude = 554351000
		)
		value := make([]GeoPoint, size)
		for i := 0; i < size; i++ {
			latitude := MinLatitude + rand.Intn(MaxLatitude-MinLatitude+1)
			longitude := MinLongitude + rand.Intn(MaxLongitude-MinLongitude+1)
			value[i] = GeoPoint{int32(latitude), int32(longitude)}
		}
		return value
	case "ShortText":
		value := make([][]byte, size)
		for i := 0; i < size; i++ {
			value[i] = []byte(strconv.Itoa(rand.Int()))
		}
		return value
	default:
		return nil
	}
}

func testColumnSetValueForScalar(t *testing.T, valueType string) {
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

func testColumnSetValueForVector(t *testing.T, valueType string) {
	options := NewColumnOptions()
	options.ColumnType = VectorColumn
	dirPath, _, db, table, column :=
		createTempColumn(t, "Table", nil, "Value", valueType, options)
	defer removeTempDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
		if err := column.SetValue(id, generateRandomVectorValue(valueType)); err != nil {
			t.Fatalf("Column.SetValue() failed: %v", err)
		}
	}

	bytes, _ := db.Query("select Table --limit 3")
	t.Logf("valueType = <%s>, result = %s", valueType, string(bytes))
}

func TestColumnSetValueForBool(t *testing.T) {
	testColumnSetValueForScalar(t, "Bool")
}

func TestColumnSetValueForInt8(t *testing.T) {
	testColumnSetValueForScalar(t, "Int8")
}

func TestColumnSetValueForInt16(t *testing.T) {
	testColumnSetValueForScalar(t, "Int16")
}

func TestColumnSetValueForInt32(t *testing.T) {
	testColumnSetValueForScalar(t, "Int32")
}

func TestColumnSetValueForInt64(t *testing.T) {
	testColumnSetValueForScalar(t, "Int64")
}

func TestColumnSetValueForUInt8(t *testing.T) {
	testColumnSetValueForScalar(t, "UInt8")
}

func TestColumnSetValueForUInt16(t *testing.T) {
	testColumnSetValueForScalar(t, "UInt16")
}

func TestColumnSetValueForUInt32(t *testing.T) {
	testColumnSetValueForScalar(t, "UInt32")
}

func TestColumnSetValueForUInt64(t *testing.T) {
	testColumnSetValueForScalar(t, "UInt64")
}

func TestColumnSetValueForFloat(t *testing.T) {
	testColumnSetValueForScalar(t, "Float")
}

func TestColumnSetValueForGeoPoint(t *testing.T) {
	testColumnSetValueForScalar(t, "WGS84GeoPoint")
}

func TestColumnSetValueForText(t *testing.T) {
	testColumnSetValueForScalar(t, "ShortText")
}

func TestColumnSetValueForBoolVector(t *testing.T) {
	testColumnSetValueForVector(t, "Bool")
}

func TestColumnSetValueForIntVector(t *testing.T) {
	testColumnSetValueForVector(t, "Int64")
}

func TestColumnSetValueForFloatVector(t *testing.T) {
	testColumnSetValueForVector(t, "Float")
}

func TestColumnSetValueForGeoPointVector(t *testing.T) {
	testColumnSetValueForVector(t, "WGS84GeoPoint")
}

func TestColumnSetValueForTextVector(t *testing.T) {
	testColumnSetValueForVector(t, "ShortText")
}

func testColumnGetValueForScalar(t *testing.T, valueType string) {
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

func testColumnGetValueForVector(t *testing.T, valueType string) {
	options := NewColumnOptions()
	options.ColumnType = VectorColumn
	dirPath, _, db, table, column :=
		createTempColumn(t, "Table", nil, "Value", valueType, options)
	defer removeTempDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
		value := generateRandomVectorValue(valueType)
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

	bytes, _ := db.Query("select Table --limit 3")
	t.Logf("valueType = <%s>, result = %s", valueType, string(bytes))
}

func TestColumnGetValueForBool(t *testing.T) {
	testColumnGetValueForScalar(t, "Bool")
}

func TestColumnGetValueForInt(t *testing.T) {
	testColumnGetValueForScalar(t, "Int64")
}

func TestColumnGetValueForFloat(t *testing.T) {
	testColumnGetValueForScalar(t, "Float")
}

func TestColumnGetValueForGeoPoint(t *testing.T) {
	testColumnGetValueForScalar(t, "WGS84GeoPoint")
}

func TestColumnGetValueForText(t *testing.T) {
	testColumnGetValueForScalar(t, "ShortText")
}

func TestColumnGetValueForBoolVector(t *testing.T) {
	testColumnGetValueForVector(t, "Bool")
}

func TestColumnGetValueForIntVector(t *testing.T) {
	testColumnGetValueForVector(t, "Int64")
}

func TestColumnGetValueForFloatVector(t *testing.T) {
	testColumnGetValueForVector(t, "Float")
}

func TestColumnGetValueForGeoPointVector(t *testing.T) {
	testColumnGetValueForVector(t, "WGS84GeoPoint")
}

func TestColumnGetValueForTextVector(t *testing.T) {
	testColumnGetValueForVector(t, "ShortText")
}

var numTestRows = 100000

func benchmarkColumnSetValueForScalar(b *testing.B, valueType string) {
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

func benchmarkColumnSetValueForVector(b *testing.B, valueType string) {
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
		values[i] = generateRandomVectorValue(valueType)
	}

	for i := 0; i < b.N; i++ {
		options := NewColumnOptions()
		options.ColumnType = VectorColumn
		column, err := table.CreateColumn(fmt.Sprintf("V%d", i), valueType, options)
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
	benchmarkColumnSetValueForScalar(b, "Bool")
}

func BenchmarkColumnSetValueForInt(b *testing.B) {
	benchmarkColumnSetValueForScalar(b, "Int64")
}

func BenchmarkColumnSetValueForFloat(b *testing.B) {
	benchmarkColumnSetValueForScalar(b, "Float")
}

func BenchmarkColumnSetValueForGeoPoint(b *testing.B) {
	benchmarkColumnSetValueForScalar(b, "WGS84GeoPoint")
}

func BenchmarkColumnSetValueForText(b *testing.B) {
	benchmarkColumnSetValueForScalar(b, "ShortText")
}

func BenchmarkColumnSetValueForBoolVector(b *testing.B) {
	benchmarkColumnSetValueForVector(b, "Bool")
}

func BenchmarkColumnSetValueForIntVector(b *testing.B) {
	benchmarkColumnSetValueForVector(b, "Int64")
}

func BenchmarkColumnSetValueForFloatVector(b *testing.B) {
	benchmarkColumnSetValueForVector(b, "Float")
}

func BenchmarkColumnSetValueForGeoPointVector(b *testing.B) {
	benchmarkColumnSetValueForVector(b, "WGS84GeoPoint")
}

func BenchmarkColumnSetValueForTextVector(b *testing.B) {
	benchmarkColumnSetValueForVector(b, "ShortText")
}

func benchmarkColumnGetValueForScalar(b *testing.B, valueType string) {
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

func benchmarkColumnGetValueForVector(b *testing.B, valueType string) {
	options := NewColumnOptions()
	options.ColumnType = VectorColumn
	dirPath, _, db, table, column :=
		createTempColumn(b, "Table", nil, "Value", valueType, options)
	defer removeTempDB(b, dirPath, db)
	ids := make([]uint32, numTestRows)
	for i, _ := range ids {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			b.Fatalf("Table.InsertRow() failed: %s", err)
		}
		if err := column.SetValue(id, generateRandomVectorValue(valueType)); err != nil {
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
	benchmarkColumnGetValueForScalar(b, "Bool")
}

func BenchmarkColumnGetValueForInt(b *testing.B) {
	benchmarkColumnGetValueForScalar(b, "Int64")
}

func BenchmarkColumnGetValueForFloat(b *testing.B) {
	benchmarkColumnGetValueForScalar(b, "Float")
}

func BenchmarkColumnGetValueForGeoPoint(b *testing.B) {
	benchmarkColumnGetValueForScalar(b, "WGS84GeoPoint")
}

func BenchmarkColumnGetValueForText(b *testing.B) {
	benchmarkColumnGetValueForScalar(b, "ShortText")
}

func BenchmarkColumnGetValueForBoolVector(b *testing.B) {
	benchmarkColumnGetValueForVector(b, "Bool")
}

func BenchmarkColumnGetValueForIntVector(b *testing.B) {
	benchmarkColumnGetValueForVector(b, "Int64")
}

func BenchmarkColumnGetValueForFloatVector(b *testing.B) {
	benchmarkColumnGetValueForVector(b, "Float")
}

func BenchmarkColumnGetValueForGeoPointVector(b *testing.B) {
	benchmarkColumnGetValueForVector(b, "WGS84GeoPoint")
}

func BenchmarkColumnGetValueForTextVector(b *testing.B) {
	benchmarkColumnGetValueForVector(b, "ShortText")
}

func benchmarkDBSelectForScalar(b *testing.B, valueType string) {
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

func benchmarkDBSelectForVector(b *testing.B, valueType string) {
	options := NewColumnOptions()
	options.ColumnType = VectorColumn
	dirPath, _, db, table, column :=
		createTempColumn(b, "Table", nil, "Value", valueType, options)
	defer removeTempDB(b, dirPath, db)
	ids := make([]uint32, numTestRows)
	for i, _ := range ids {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			b.Fatalf("Table.InsertRow() failed: %s", err)
		}
		if err := column.SetValue(id, generateRandomVectorValue(valueType)); err != nil {
			b.Fatalf("Column.SetValue() failed: %s", err)
		}
		ids[i] = id
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes, err := db.Query("select Table --output_columns Value --limit -1 --cache no")
		if err != nil {
			b.Fatalf("DB.Query() failed: %s", err)
		}
		if len(bytes) < numTestRows*5 {
			b.Fatalf("DB.Query() failed: %s", err)
		}
	}
}

func BenchmarkDBSelectForBool(b *testing.B) {
	benchmarkDBSelectForScalar(b, "Bool")
}

func BenchmarkDBSelectForInt(b *testing.B) {
	benchmarkDBSelectForScalar(b, "Int64")
}

func BenchmarkDBSelectForFloat(b *testing.B) {
	benchmarkDBSelectForScalar(b, "Float")
}

func BenchmarkDBSelectForGeoPoint(b *testing.B) {
	benchmarkDBSelectForScalar(b, "WGS84GeoPoint")
}

func BenchmarkDBSelectForText(b *testing.B) {
	benchmarkDBSelectForScalar(b, "ShortText")
}

func BenchmarkDBSelectForBoolVector(b *testing.B) {
	benchmarkDBSelectForVector(b, "Bool")
}

func BenchmarkDBSelectForIntVector(b *testing.B) {
	benchmarkDBSelectForVector(b, "Int64")
}

func BenchmarkDBSelectForFloatVector(b *testing.B) {
	benchmarkDBSelectForVector(b, "Float")
}

func BenchmarkDBSelectForGeoPointVector(b *testing.B) {
	benchmarkDBSelectForVector(b, "WGS84GeoPoint")
}

func BenchmarkDBSelectForTextVector(b *testing.B) {
	benchmarkDBSelectForVector(b, "ShortText")
}
