package gnx

/*
#cgo pkg-config: groonga
*/
import "C"

import (
	"fmt"
	"math"
)

// -- Data types --

type Bool uint8
type Int int64
type Float float64
type GeoPoint struct{ Latitude, Longitude int32 }
type Text []byte

//type BoolVector []Bool
//type IntVector []Int
//type FloatVector []Float
//type GeoPointVector []GeoPoint
//type TextVector []Text

const (
	True  = Bool(3)
	False = Bool(0)
)

func NullBool() Bool         { return Bool(1) }
func NullInt() Int           { return Int(math.MinInt64) }
func NullFloat() Float       { return Float(math.NaN()) }
func NullGeoPoint() GeoPoint { return GeoPoint{math.MinInt32, math.MinInt32} }
func NullText() Text         { return nil }

//func NullBoolVector() BoolVector         { return nil }
//func NullIntVector() IntVector           { return nil }
//func NullFloatVector() FloatVector       { return nil }
//func NullGeoPointVector() GeoPointVector { return nil }
//func NullTextVector() TextVector         { return nil }

type TypeID int

const (
	VoidID = TypeID(iota)
	BoolID
	IntID
	FloatID
	GeoPointID
	TextID
//	BoolVectorID
//	IntVectorID
//	FloatVectorID
//	GeoPointVectorID
//	TextVectorID
)

func (id TypeID) String() string {
	switch id {
	case VoidID:
		return "Void"
	case BoolID:
		return "Bool"
	case IntID:
		return "Int"
	case FloatID:
		return "Float"
	case GeoPointID:
		return "GeoPoint"
	case TextID:
		return "Text"
//	case BoolVectorID:
//		return "BoolVector"
//	case IntVectorID:
//		return "IntVector"
//	case FloatVectorID:
//		return "FloatVector"
//	case GeoPointVectorID:
//		return "GeoPointVector"
//	case TextVectorID:
//		return "TextVector"
	default:
		return fmt.Sprintf("TypeID(%d)", id)
	}
}
