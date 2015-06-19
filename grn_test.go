package gnx

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
)

// createTempGrnDB() creates a database for tests.
// The database must be removed with removeTempGrnDB().
func createTempGrnDB(tb testing.TB) (string, string, *GrnDB) {
	dirPath, err := ioutil.TempDir("", "grn_test")
	if err != nil {
		tb.Fatalf("ioutil.TempDir() failed: %v", err)
	}
	dbPath := dirPath + "/db"
	db, err := CreateGrnDB(dbPath)
	if err != nil {
		os.RemoveAll(dirPath)
		tb.Fatalf("CreateGrnDB() failed: %v", err)
	}
	return dirPath, dbPath, db
}

// removeTempGrnDB() removes a database created with createTempGrnDB().
func removeTempGrnDB(tb testing.TB, dirPath string, db *GrnDB) {
	if err := db.Close(); err != nil {
		os.RemoveAll(dirPath)
		tb.Fatalf("GrnDB.Close() failed: %v", err)
	}
	if err := os.RemoveAll(dirPath); err != nil {
		tb.Fatalf("os.RemoveAll() failed: %v", err)
	}
}

// createTempGrnTable() creates a database and a table for tests.
// createTempGrnTable() uses createTempGrnDB() to create a database, so the
// database must be removed with removeTempGrnDB().
func createTempGrnTable(tb testing.TB, name string, options *TableOptions) (
	string, string, *GrnDB, *GrnTable) {
	dirPath, dbPath, db := createTempGrnDB(tb)
	table, err := db.CreateTable(name, options)
	if err != nil {
		removeTempGrnDB(tb, dirPath, db)
		tb.Fatalf("GrnDB.CreateTable() failed: %v", err)
	}
	return dirPath, dbPath, db, table
}

// createTempGrnColumn() creates a database, a table, and a column for tests.
// createTempGrnColumn() uses createTempGrnDB() to create a database, so the
// database must be removed with removeTempGrnDB().
func createTempGrnColumn(tb testing.TB, tableName string,
	tableOptions *TableOptions, columnName string, valueType string,
	columnOptions *ColumnOptions) (
	string, string, *GrnDB, *GrnTable, *GrnColumn) {
	dirPath, dbPath, db, table := createTempGrnTable(tb, tableName, tableOptions)
	column, err := table.CreateColumn(columnName, valueType, columnOptions)
	if err != nil {
		removeTempGrnDB(tb, dirPath, db)
		tb.Fatalf("GrnDB.CreateTable() failed: %v", err)
	}
	return dirPath, dbPath, db, table, column
}

func TestCreateGrnDB(t *testing.T) {
	dirPath, _, db := createTempGrnDB(t)
	removeTempGrnDB(t, dirPath, db)
}

func TestOpenGrnDB(t *testing.T) {
	dirPath, dbPath, db := createTempGrnDB(t)
	db2, err := OpenGrnDB(dbPath)
	if err != nil {
		t.Fatalf("OpenGrnDB() failed: %v", err)
	}
	db2.Close()
	removeTempGrnDB(t, dirPath, db)
}

func testGrnDBCreateTableWithKey(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.TableType = PatTable
	options.KeyType = keyType
	dirPath, _, db, _ := createTempGrnTable(t, "Table", options)
	removeTempGrnDB(t, dirPath, db)
}

func testGrnDBCreateTableWithValue(t *testing.T, valueType string) {
	options := NewTableOptions()
	options.TableType = PatTable
	options.ValueType = valueType
	dirPath, _, db, _ := createTempGrnTable(t, "Table", options)
	removeTempGrnDB(t, dirPath, db)
}

func testGrnDBCreateTableWithRefKey(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.TableType = PatTable
	options.KeyType = keyType
	dirPath, _, db, _ := createTempGrnTable(t, "To", options)
	defer removeTempGrnDB(t, dirPath, db)

	options = NewTableOptions()
	options.TableType = PatTable
	options.KeyType = "To"
	_, err := db.CreateTable("From", options)
	if err != nil {
		t.Fatalf("GrnDB.CreateTable() failed: %v", err)
	}
}

func testGrnDBCreateTableWithRefValue(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.TableType = PatTable
	options.KeyType = keyType
	dirPath, _, db, _ := createTempGrnTable(t, "To", options)
	defer removeTempGrnDB(t, dirPath, db)

	options = NewTableOptions()
	options.ValueType = ""
	_, err := db.CreateTable("From", options)
	if err != nil {
		t.Fatalf("GrnDB.CreateTable() failed: %v", err)
	}
}

func TestGrnDBCreateTableWithoutKeyValue(t *testing.T) {
	dirPath, _, db, _ := createTempGrnTable(t, "Table", nil)
	removeTempGrnDB(t, dirPath, db)
}

func TestGrnDBCreateTableWithBoolKey(t *testing.T) {
	testGrnDBCreateTableWithKey(t, "Bool")
}

func TestGrnDBCreateTableWithIntKey(t *testing.T) {
	testGrnDBCreateTableWithKey(t, "Int")
}

func TestGrnDBCreateTableWithFloatKey(t *testing.T) {
	testGrnDBCreateTableWithKey(t, "Float")
}

func TestGrnDBCreateTableWithGeoPointKey(t *testing.T) {
	testGrnDBCreateTableWithKey(t, "GeoPoint")
}

func TestGrnDBCreateTableWithTextKey(t *testing.T) {
	testGrnDBCreateTableWithKey(t, "Text")
}

func TestGrnDBCreateTableWithBoolValue(t *testing.T) {
	testGrnDBCreateTableWithValue(t, "Bool")
}

func TestGrnDBCreateTableWithIntValue(t *testing.T) {
	testGrnDBCreateTableWithValue(t, "Int")
}

func TestGrnDBCreateTableWithFloatValue(t *testing.T) {
	testGrnDBCreateTableWithValue(t, "Float")
}

func TestGrnDBCreateTableWithGeoPointValue(t *testing.T) {
	testGrnDBCreateTableWithValue(t, "GeoPoint")
}

func TestGrnDBCreateTableWithBoolRefKey(t *testing.T) {
	testGrnDBCreateTableWithRefKey(t, "Bool")
}

func TestGrnDBCreateTableWithIntRefKey(t *testing.T) {
	testGrnDBCreateTableWithRefKey(t, "Int")
}

func TestGrnDBCreateTableWithFloatRefKey(t *testing.T) {
	testGrnDBCreateTableWithRefKey(t, "Float")
}

func TestGrnDBCreateTableWithGeoPointRefKey(t *testing.T) {
	testGrnDBCreateTableWithRefKey(t, "GeoPoint")
}

func TestGrnDBCreateTableWithTextRefKey(t *testing.T) {
	testGrnDBCreateTableWithRefKey(t, "Text")
}

func TestGrnDBCreateTableWithBoolRefValue(t *testing.T) {
	testGrnDBCreateTableWithRefValue(t, "Bool")
}

func TestGrnDBCreateTableWithIntRefValue(t *testing.T) {
	testGrnDBCreateTableWithRefValue(t, "Int")
}

func TestGrnDBCreateTableWithFloatRefValue(t *testing.T) {
	testGrnDBCreateTableWithRefValue(t, "Float")
}

func TestGrnDBCreateTableWithGeoPointRefValue(t *testing.T) {
	testGrnDBCreateTableWithRefValue(t, "GeoPoint")
}

func TestGrnDBCreateTableWithTextRefValue(t *testing.T) {
	testGrnDBCreateTableWithRefValue(t, "Text")
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

func testGrnTableInsertRow(t *testing.T, keyType string) {
	options := NewTableOptions()
	if keyType != "" {
		options.TableType = PatTable
	}
	options.KeyType = keyType
	dirPath, _, db, table := createTempGrnTable(t, "Table", options)
	defer removeTempGrnDB(t, dirPath, db)

	count := 0
	for i := 0; i < 100; i++ {
		inserted, _, err := table.InsertRow(generateRandomKey(keyType))
		if err != nil {
			t.Fatalf("GrnTable.InsertRow() failed: %v", err)
		}
		if inserted {
			count++
		}
	}
	t.Logf("keyType = <%s>, count = %d", keyType, count)
}

func TestGrnTableInsertRowWithoutKey(t *testing.T) {
	testGrnTableInsertRow(t, "")
}

func TestGrnTableInsertRowWithBoolKey(t *testing.T) {
	testGrnTableInsertRow(t, "Bool")
}

func TestGrnTableInsertRowWithIntKey(t *testing.T) {
	testGrnTableInsertRow(t, "Int")
}

func TestGrnTableInsertRowWithFloatKey(t *testing.T) {
	testGrnTableInsertRow(t, "Float")
}

func TestGrnTableInsertRowWithGeoPointKey(t *testing.T) {
	testGrnTableInsertRow(t, "GeoPoint")
}

func TestGrnTableInsertRowWithTextKey(t *testing.T) {
	testGrnTableInsertRow(t, "Text")
}

func testGrnTableCreateScalarColumn(t *testing.T, valueType string) {
	dirPath, _, db, table, _ :=
		createTempGrnColumn(t, "Table", nil, "Value", valueType, nil)
	defer removeTempGrnDB(t, dirPath, db)

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

func testGrnTableCreateVectorColumn(t *testing.T, valueType string) {
	options := NewColumnOptions()
	options.ColumnType = VectorColumn
	dirPath, _, db, table, _ :=
		createTempGrnColumn(t, "Table", nil, "Value", valueType, options)
	defer removeTempGrnDB(t, dirPath, db)

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

func testGrnTableCreateScalarRefColumn(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.TableType = PatTable
	options.KeyType = keyType
	dirPath, _, db, table, _ :=
		createTempGrnColumn(t, "Table", options, "Value", "Table", nil)
	defer removeTempGrnDB(t, dirPath, db)

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

func testGrnTableCreateVectorRefColumn(t *testing.T, keyType string) {
	tableOptions := NewTableOptions()
	tableOptions.TableType = PatTable
	tableOptions.KeyType = keyType
	columnOptions := NewColumnOptions()
	columnOptions.ColumnType = VectorColumn
	dirPath, _, db, table, _ :=
		createTempGrnColumn(t, "Table", tableOptions, "Value", "Table", columnOptions)
	defer removeTempGrnDB(t, dirPath, db)

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

func TestGrnTableCreateColumnForBool(t *testing.T) {
	testGrnTableCreateScalarColumn(t, "Bool")
}

func TestGrnTableCreateColumnForInt(t *testing.T) {
	testGrnTableCreateScalarColumn(t, "Int")
}

func TestGrnTableCreateColumnForFloat(t *testing.T) {
	testGrnTableCreateScalarColumn(t, "Float")
}

func TestGrnTableCreateColumnForGeoPoint(t *testing.T) {
	testGrnTableCreateScalarColumn(t, "GeoPoint")
}

func TestGrnTableCreateColumnForText(t *testing.T) {
	testGrnTableCreateScalarColumn(t, "Text")
}

func TestGrnTableCreateColumnForBoolVector(t *testing.T) {
	testGrnTableCreateVectorColumn(t, "Bool")
}

func TestGrnTableCreateColumnForIntVector(t *testing.T) {
	testGrnTableCreateVectorColumn(t, "Int")
}

func TestGrnTableCreateColumnForFloatVector(t *testing.T) {
	testGrnTableCreateVectorColumn(t, "Float")
}

func TestGrnTableCreateColumnForGeoPointVector(t *testing.T) {
	testGrnTableCreateVectorColumn(t, "GeoPoint")
}

func TestGrnTableCreateColumnForTextVector(t *testing.T) {
	testGrnTableCreateVectorColumn(t, "Text")
}

func TestGrnTableCreateColumnForRefToBool(t *testing.T) {
	testGrnTableCreateScalarRefColumn(t, "Bool")
}

func TestGrnTableCreateColumnForRefToInt(t *testing.T) {
	testGrnTableCreateScalarRefColumn(t, "Int")
}

func TestGrnTableCreateColumnForRefToFloat(t *testing.T) {
	testGrnTableCreateScalarRefColumn(t, "Float")
}

func TestGrnTableCreateColumnForRefToGeoPoint(t *testing.T) {
	testGrnTableCreateScalarRefColumn(t, "GeoPoint")
}

func TestGrnTableCreateColumnForRefToText(t *testing.T) {
	testGrnTableCreateScalarRefColumn(t, "Text")
}

func TestGrnTableCreateColumnForRefToBoolVector(t *testing.T) {
	testGrnTableCreateVectorRefColumn(t, "Bool")
}

func TestGrnTableCreateColumnForRefToIntVector(t *testing.T) {
	testGrnTableCreateVectorRefColumn(t, "Int")
}

func TestGrnTableCreateColumnForRefToFloatVector(t *testing.T) {
	testGrnTableCreateVectorRefColumn(t, "Float")
}

func TestGrnTableCreateColumnForRefToGeoPointVector(t *testing.T) {
	testGrnTableCreateVectorRefColumn(t, "GeoPoint")
}

func TestGrnTableCreateColumnForRefToTextVector(t *testing.T) {
	testGrnTableCreateVectorRefColumn(t, "Text")
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
	defer removeTempGrnDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("GrnTable.InsertRow() failed: %v", err)
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
	defer removeTempGrnDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("GrnTable.InsertRow() failed: %v", err)
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
	defer removeTempGrnDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("GrnTable.InsertRow() failed: %v", err)
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
	defer removeTempGrnDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("GrnTable.InsertRow() failed: %v", err)
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
		createTempGrnTable(b, "Table", nil)
	defer removeTempGrnDB(b, dirPath, db)
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
		createTempGrnTable(b, "Table", nil)
	defer removeTempGrnDB(b, dirPath, db)
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
	defer removeTempGrnDB(b, dirPath, db)
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
	defer removeTempGrnDB(b, dirPath, db)
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

func benchmarkGrnDBSelectForScalar(b *testing.B, valueType string) {
	dirPath, _, db, table, column :=
		createTempGrnColumn(b, "Table", nil, "Value", valueType, nil)
	defer removeTempGrnDB(b, dirPath, db)
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

func benchmarkGrnDBSelectForVector(b *testing.B, valueType string) {
	options := NewColumnOptions()
	options.ColumnType = VectorColumn
	dirPath, _, db, table, column :=
		createTempGrnColumn(b, "Table", nil, "Value", valueType, options)
	defer removeTempGrnDB(b, dirPath, db)
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

func BenchmarkGrnDBSelectForBool(b *testing.B) {
	benchmarkGrnDBSelectForScalar(b, "Bool")
}

func BenchmarkGrnDBSelectForInt(b *testing.B) {
	benchmarkGrnDBSelectForScalar(b, "Int")
}

func BenchmarkGrnDBSelectForFloat(b *testing.B) {
	benchmarkGrnDBSelectForScalar(b, "Float")
}

func BenchmarkGrnDBSelectForGeoPoint(b *testing.B) {
	benchmarkGrnDBSelectForScalar(b, "GeoPoint")
}

func BenchmarkGrnDBSelectForText(b *testing.B) {
	benchmarkGrnDBSelectForScalar(b, "Text")
}

func BenchmarkGrnDBSelectForBoolVector(b *testing.B) {
	benchmarkGrnDBSelectForVector(b, "Bool")
}

func BenchmarkGrnDBSelectForIntVector(b *testing.B) {
	benchmarkGrnDBSelectForVector(b, "Int")
}

func BenchmarkGrnDBSelectForFloatVector(b *testing.B) {
	benchmarkGrnDBSelectForVector(b, "Float")
}

func BenchmarkGrnDBSelectForGeoPointVector(b *testing.B) {
	benchmarkGrnDBSelectForVector(b, "GeoPoint")
}

func BenchmarkGrnDBSelectForTextVector(b *testing.B) {
	benchmarkGrnDBSelectForVector(b, "Text")
}
