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
	dirPath, err := ioutil.TempDir("", "grn_test")
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

// createTempGrnColumn() creates a database, a table, and a column for tests.
// createTempGrnColumn() uses createTempDB() to create a database, so the
// database must be removed with removeTempDB().
func createTempGrnColumn(tb testing.TB, tableName string,
	tableOptions *TableOptions, columnName string, valueType string,
	columnOptions *ColumnOptions) (
	string, string, *DB, *Table, *GrnColumn) {
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
	testDBCreateTableWithKey(t, "Int")
}

func TestDBCreateTableWithFloatKey(t *testing.T) {
	testDBCreateTableWithKey(t, "Float")
}

func TestDBCreateTableWithGeoPointKey(t *testing.T) {
	testDBCreateTableWithKey(t, "GeoPoint")
}

func TestDBCreateTableWithTextKey(t *testing.T) {
	testDBCreateTableWithKey(t, "Text")
}

func TestDBCreateTableWithBoolValue(t *testing.T) {
	testDBCreateTableWithValue(t, "Bool")
}

func TestDBCreateTableWithIntValue(t *testing.T) {
	testDBCreateTableWithValue(t, "Int")
}

func TestDBCreateTableWithFloatValue(t *testing.T) {
	testDBCreateTableWithValue(t, "Float")
}

func TestDBCreateTableWithGeoPointValue(t *testing.T) {
	testDBCreateTableWithValue(t, "GeoPoint")
}

func TestDBCreateTableWithBoolRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Bool")
}

func TestDBCreateTableWithIntRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Int")
}

func TestDBCreateTableWithFloatRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Float")
}

func TestDBCreateTableWithGeoPointRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "GeoPoint")
}

func TestDBCreateTableWithTextRefKey(t *testing.T) {
	testDBCreateTableWithRefKey(t, "Text")
}

func TestDBCreateTableWithBoolRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Bool")
}

func TestDBCreateTableWithIntRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Int")
}

func TestDBCreateTableWithFloatRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Float")
}

func TestDBCreateTableWithGeoPointRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "GeoPoint")
}

func TestDBCreateTableWithTextRefValue(t *testing.T) {
	testDBCreateTableWithRefValue(t, "Text")
}

func generateRandomKey(keyType string) interface{} {
	switch keyType {
	case "Bool":
		if (rand.Int() & 1) == 1 {
			return True
		} else {
			return False
		}
	case "Int":
		return Int(rand.Int63())
	case "Float":
		return Float(rand.Float64())
	case "GeoPoint":
		const (
			MinLatitude  = 73531000
			MaxLatitude  = 164006000
			MinLongitude = 439451000
			MaxLongitude = 554351000
		)
		latitude := MinLatitude + rand.Intn(MaxLatitude-MinLatitude+1)
		longitude := MinLongitude + rand.Intn(MaxLongitude-MinLongitude+1)
		return GeoPoint{int32(latitude), int32(longitude)}
	case "Text":
		return Text(strconv.Itoa(rand.Int()))
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

func TestTableInsertRowWithIntKey(t *testing.T) {
	testTableInsertRow(t, "Int")
}

func TestTableInsertRowWithFloatKey(t *testing.T) {
	testTableInsertRow(t, "Float")
}

func TestTableInsertRowWithGeoPointKey(t *testing.T) {
	testTableInsertRow(t, "GeoPoint")
}

func TestTableInsertRowWithTextKey(t *testing.T) {
	testTableInsertRow(t, "Text")
}

func testTableCreateScalarColumn(t *testing.T, valueType string) {
	dirPath, _, db, table, _ :=
		createTempGrnColumn(t, "Table", nil, "Value", valueType, nil)
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
		createTempGrnColumn(t, "Table", nil, "Value", valueType, options)
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
		createTempGrnColumn(t, "Table", options, "Value", "Table", nil)
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
		createTempGrnColumn(t, "Table", tableOptions, "Value", "Table", columnOptions)
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
	testTableCreateScalarColumn(t, "Int")
}

func TestTableCreateColumnForFloat(t *testing.T) {
	testTableCreateScalarColumn(t, "Float")
}

func TestTableCreateColumnForGeoPoint(t *testing.T) {
	testTableCreateScalarColumn(t, "GeoPoint")
}

func TestTableCreateColumnForText(t *testing.T) {
	testTableCreateScalarColumn(t, "Text")
}

func TestTableCreateColumnForBoolVector(t *testing.T) {
	testTableCreateVectorColumn(t, "Bool")
}

func TestTableCreateColumnForIntVector(t *testing.T) {
	testTableCreateVectorColumn(t, "Int")
}

func TestTableCreateColumnForFloatVector(t *testing.T) {
	testTableCreateVectorColumn(t, "Float")
}

func TestTableCreateColumnForGeoPointVector(t *testing.T) {
	testTableCreateVectorColumn(t, "GeoPoint")
}

func TestTableCreateColumnForTextVector(t *testing.T) {
	testTableCreateVectorColumn(t, "Text")
}

func TestTableCreateColumnForRefToBool(t *testing.T) {
	testTableCreateScalarRefColumn(t, "Bool")
}

func TestTableCreateColumnForRefToInt(t *testing.T) {
	testTableCreateScalarRefColumn(t, "Int")
}

func TestTableCreateColumnForRefToFloat(t *testing.T) {
	testTableCreateScalarRefColumn(t, "Float")
}

func TestTableCreateColumnForRefToGeoPoint(t *testing.T) {
	testTableCreateScalarRefColumn(t, "GeoPoint")
}

func TestTableCreateColumnForRefToText(t *testing.T) {
	testTableCreateScalarRefColumn(t, "Text")
}

func TestTableCreateColumnForRefToBoolVector(t *testing.T) {
	testTableCreateVectorRefColumn(t, "Bool")
}

func TestTableCreateColumnForRefToIntVector(t *testing.T) {
	testTableCreateVectorRefColumn(t, "Int")
}

func TestTableCreateColumnForRefToFloatVector(t *testing.T) {
	testTableCreateVectorRefColumn(t, "Float")
}

func TestTableCreateColumnForRefToGeoPointVector(t *testing.T) {
	testTableCreateVectorRefColumn(t, "GeoPoint")
}

func TestTableCreateColumnForRefToTextVector(t *testing.T) {
	testTableCreateVectorRefColumn(t, "Text")
}

func generateRandomValue(valueType string) interface{} {
	switch valueType {
	case "Bool":
		if (rand.Int() & 1) == 1 {
			return True
		} else {
			return False
		}
	case "Int":
		return Int(rand.Int63())
	case "Float":
		return Float(rand.Float64())
	case "GeoPoint":
		const (
			MinLatitude  = 73531000
			MaxLatitude  = 164006000
			MinLongitude = 439451000
			MaxLongitude = 554351000
		)
		latitude := MinLatitude + rand.Intn(MaxLatitude-MinLatitude+1)
		longitude := MinLongitude + rand.Intn(MaxLongitude-MinLongitude+1)
		return GeoPoint{int32(latitude), int32(longitude)}
	case "Text":
		return Text(strconv.Itoa(rand.Int()))
	default:
		return nil
	}
}

func generateRandomVectorValue(valueType string) interface{} {
	size := rand.Int() % 10
	switch valueType {
	case "Bool":
		value := make([]Bool, size)
		for i := 0; i < size; i++ {
			if (rand.Int() & 1) == 1 {
				value[i] = True
			}
		}
		return value
	case "Int":
		value := make([]Int, size)
		for i := 0; i < size; i++ {
			value[i] = Int(rand.Int63())
		}
		return value
	case "Float":
		value := make([]Float, size)
		for i := 0; i < size; i++ {
			value[i] = Float(rand.Float64())
		}
		return value
	case "GeoPoint":
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
	case "Text":
		value := make([]Text, size)
		for i := 0; i < size; i++ {
			value[i] = Text(strconv.Itoa(rand.Int()))
		}
		return value
	default:
		return nil
	}
}

func testGrnColumnSetValueForScalar(t *testing.T, valueType string) {
	dirPath, _, db, table, column :=
		createTempGrnColumn(t, "Table", nil, "Value", valueType, nil)
	defer removeTempDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
		if err := column.SetValue(id, generateRandomValue(valueType)); err != nil {
			t.Fatalf("GrnColumn.SetValue() failed: %v", err)
		}
	}

	bytes, _ := db.Query("select Table --limit 3")
	t.Logf("valueType = <%s>, result = %s", valueType, string(bytes))
}

func testGrnColumnSetValueForVector(t *testing.T, valueType string) {
	options := NewColumnOptions()
	options.ColumnType = VectorColumn
	dirPath, _, db, table, column :=
		createTempGrnColumn(t, "Table", nil, "Value", valueType, options)
	defer removeTempDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
		if err := column.SetValue(id, generateRandomVectorValue(valueType)); err != nil {
			t.Fatalf("GrnColumn.SetValue() failed: %v", err)
		}
	}

	bytes, _ := db.Query("select Table --limit 3")
	t.Logf("valueType = <%s>, result = %s", valueType, string(bytes))
}

func TestGrnColumnSetValueForBool(t *testing.T) {
	testGrnColumnSetValueForScalar(t, "Bool")
}

func TestGrnColumnSetValueForInt(t *testing.T) {
	testGrnColumnSetValueForScalar(t, "Int")
}

func TestGrnColumnSetValueForFloat(t *testing.T) {
	testGrnColumnSetValueForScalar(t, "Float")
}

func TestGrnColumnSetValueForGeoPoint(t *testing.T) {
	testGrnColumnSetValueForScalar(t, "GeoPoint")
}

func TestGrnColumnSetValueForText(t *testing.T) {
	testGrnColumnSetValueForScalar(t, "Text")
}

func TestGrnColumnSetValueForBoolVector(t *testing.T) {
	testGrnColumnSetValueForVector(t, "Bool")
}

func TestGrnColumnSetValueForIntVector(t *testing.T) {
	testGrnColumnSetValueForVector(t, "Int")
}

func TestGrnColumnSetValueForFloatVector(t *testing.T) {
	testGrnColumnSetValueForVector(t, "Float")
}

func TestGrnColumnSetValueForGeoPointVector(t *testing.T) {
	testGrnColumnSetValueForVector(t, "GeoPoint")
}

func TestGrnColumnSetValueForTextVector(t *testing.T) {
	testGrnColumnSetValueForVector(t, "Text")
}

func testGrnColumnGetValueForScalar(t *testing.T, valueType string) {
	dirPath, _, db, table, column :=
		createTempGrnColumn(t, "Table", nil, "Value", valueType, nil)
	defer removeTempDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
		value := generateRandomValue(valueType)
		if err := column.SetValue(id, value); err != nil {
			t.Fatalf("GrnColumn.SetValue() failed: %v", err)
		}
		if storedValue, err := column.GetValue(id); err != nil {
			t.Fatalf("GrnColumn.GetValue() failed: %v", err)
		} else if !reflect.DeepEqual(value, storedValue) {
			t.Fatalf("GrnColumn.GetValue() failed: value = %v, storedValue = %v",
				value, storedValue)
		}
	}
}

func testGrnColumnGetValueForVector(t *testing.T, valueType string) {
	options := NewColumnOptions()
	options.ColumnType = VectorColumn
	dirPath, _, db, table, column :=
		createTempGrnColumn(t, "Table", nil, "Value", valueType, options)
	defer removeTempDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("Table.InsertRow() failed: %v", err)
		}
		value := generateRandomVectorValue(valueType)
		if err := column.SetValue(id, value); err != nil {
			t.Fatalf("GrnColumn.SetValue() failed: %v", err)
		}
		if storedValue, err := column.GetValue(id); err != nil {
			t.Fatalf("GrnColumn.GetValue() failed: %v", err)
		} else if !reflect.DeepEqual(value, storedValue) {
			t.Fatalf("GrnColumn.GetValue() failed: value = %v, storedValue = %v",
				value, storedValue)
		}
	}

	bytes, _ := db.Query("select Table --limit 3")
	t.Logf("valueType = <%s>, result = %s", valueType, string(bytes))
}

func TestGrnColumnGetValueForBool(t *testing.T) {
	testGrnColumnGetValueForScalar(t, "Bool")
}

func TestGrnColumnGetValueForInt(t *testing.T) {
	testGrnColumnGetValueForScalar(t, "Int")
}

func TestGrnColumnGetValueForFloat(t *testing.T) {
	testGrnColumnGetValueForScalar(t, "Float")
}

func TestGrnColumnGetValueForGeoPoint(t *testing.T) {
	testGrnColumnGetValueForScalar(t, "GeoPoint")
}

func TestGrnColumnGetValueForText(t *testing.T) {
	testGrnColumnGetValueForScalar(t, "Text")
}

func TestGrnColumnGetValueForBoolVector(t *testing.T) {
	testGrnColumnGetValueForVector(t, "Bool")
}

func TestGrnColumnGetValueForIntVector(t *testing.T) {
	testGrnColumnGetValueForVector(t, "Int")
}

func TestGrnColumnGetValueForFloatVector(t *testing.T) {
	testGrnColumnGetValueForVector(t, "Float")
}

func TestGrnColumnGetValueForGeoPointVector(t *testing.T) {
	testGrnColumnGetValueForVector(t, "GeoPoint")
}

func TestGrnColumnGetValueForTextVector(t *testing.T) {
	testGrnColumnGetValueForVector(t, "Text")
}

var numTestRows = 100000

func benchmarkGrnColumnSetValueForScalar(b *testing.B, valueType string) {
	b.StopTimer()
	dirPath, _, db, table :=
		createTempTable(b, "Table", nil)
	defer removeTempDB(b, dirPath, db)
	ids := make([]Int, numTestRows)
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

func benchmarkGrnColumnSetValueForVector(b *testing.B, valueType string) {
	b.StopTimer()
	dirPath, _, db, table :=
		createTempTable(b, "Table", nil)
	defer removeTempDB(b, dirPath, db)
	ids := make([]Int, numTestRows)
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

func BenchmarkGrnColumnSetValueForBool(b *testing.B) {
	benchmarkGrnColumnSetValueForScalar(b, "Bool")
}

func BenchmarkGrnColumnSetValueForInt(b *testing.B) {
	benchmarkGrnColumnSetValueForScalar(b, "Int")
}

func BenchmarkGrnColumnSetValueForFloat(b *testing.B) {
	benchmarkGrnColumnSetValueForScalar(b, "Float")
}

func BenchmarkGrnColumnSetValueForGeoPoint(b *testing.B) {
	benchmarkGrnColumnSetValueForScalar(b, "GeoPoint")
}

func BenchmarkGrnColumnSetValueForText(b *testing.B) {
	benchmarkGrnColumnSetValueForScalar(b, "Text")
}

func BenchmarkGrnColumnSetValueForBoolVector(b *testing.B) {
	benchmarkGrnColumnSetValueForVector(b, "Bool")
}

func BenchmarkGrnColumnSetValueForIntVector(b *testing.B) {
	benchmarkGrnColumnSetValueForVector(b, "Int")
}

func BenchmarkGrnColumnSetValueForFloatVector(b *testing.B) {
	benchmarkGrnColumnSetValueForVector(b, "Float")
}

func BenchmarkGrnColumnSetValueForGeoPointVector(b *testing.B) {
	benchmarkGrnColumnSetValueForVector(b, "GeoPoint")
}

func BenchmarkGrnColumnSetValueForTextVector(b *testing.B) {
	benchmarkGrnColumnSetValueForVector(b, "Text")
}

func benchmarkGrnColumnGetValueForScalar(b *testing.B, valueType string) {
	dirPath, _, db, table, column :=
		createTempGrnColumn(b, "Table", nil, "Value", valueType, nil)
	defer removeTempDB(b, dirPath, db)
	ids := make([]Int, numTestRows)
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

func benchmarkGrnColumnGetValueForVector(b *testing.B, valueType string) {
	options := NewColumnOptions()
	options.ColumnType = VectorColumn
	dirPath, _, db, table, column :=
		createTempGrnColumn(b, "Table", nil, "Value", valueType, options)
	defer removeTempDB(b, dirPath, db)
	ids := make([]Int, numTestRows)
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

func BenchmarkGrnColumnGetValueForBool(b *testing.B) {
	benchmarkGrnColumnGetValueForScalar(b, "Bool")
}

func BenchmarkGrnColumnGetValueForInt(b *testing.B) {
	benchmarkGrnColumnGetValueForScalar(b, "Int")
}

func BenchmarkGrnColumnGetValueForFloat(b *testing.B) {
	benchmarkGrnColumnGetValueForScalar(b, "Float")
}

func BenchmarkGrnColumnGetValueForGeoPoint(b *testing.B) {
	benchmarkGrnColumnGetValueForScalar(b, "GeoPoint")
}

func BenchmarkGrnColumnGetValueForText(b *testing.B) {
	benchmarkGrnColumnGetValueForScalar(b, "Text")
}

func BenchmarkGrnColumnGetValueForBoolVector(b *testing.B) {
	benchmarkGrnColumnGetValueForVector(b, "Bool")
}

func BenchmarkGrnColumnGetValueForIntVector(b *testing.B) {
	benchmarkGrnColumnGetValueForVector(b, "Int")
}

func BenchmarkGrnColumnGetValueForFloatVector(b *testing.B) {
	benchmarkGrnColumnGetValueForVector(b, "Float")
}

func BenchmarkGrnColumnGetValueForGeoPointVector(b *testing.B) {
	benchmarkGrnColumnGetValueForVector(b, "GeoPoint")
}

func BenchmarkGrnColumnGetValueForTextVector(b *testing.B) {
	benchmarkGrnColumnGetValueForVector(b, "Text")
}

func benchmarkDBSelectForScalar(b *testing.B, valueType string) {
	dirPath, _, db, table, column :=
		createTempGrnColumn(b, "Table", nil, "Value", valueType, nil)
	defer removeTempDB(b, dirPath, db)
	ids := make([]Int, numTestRows)
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
		createTempGrnColumn(b, "Table", nil, "Value", valueType, options)
	defer removeTempDB(b, dirPath, db)
	ids := make([]Int, numTestRows)
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
	benchmarkDBSelectForScalar(b, "Int")
}

func BenchmarkDBSelectForFloat(b *testing.B) {
	benchmarkDBSelectForScalar(b, "Float")
}

func BenchmarkDBSelectForGeoPoint(b *testing.B) {
	benchmarkDBSelectForScalar(b, "GeoPoint")
}

func BenchmarkDBSelectForText(b *testing.B) {
	benchmarkDBSelectForScalar(b, "Text")
}

func BenchmarkDBSelectForBoolVector(b *testing.B) {
	benchmarkDBSelectForVector(b, "Bool")
}

func BenchmarkDBSelectForIntVector(b *testing.B) {
	benchmarkDBSelectForVector(b, "Int")
}

func BenchmarkDBSelectForFloatVector(b *testing.B) {
	benchmarkDBSelectForVector(b, "Float")
}

func BenchmarkDBSelectForGeoPointVector(b *testing.B) {
	benchmarkDBSelectForVector(b, "GeoPoint")
}

func BenchmarkDBSelectForTextVector(b *testing.B) {
	benchmarkDBSelectForVector(b, "Text")
}
