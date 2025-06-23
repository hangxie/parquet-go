package common

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v2/parquet"
)

const (
	PAR_GO_PATH_DELIMITER = "\x01"
	DefaultPageSize       = 8 * 1024
	DefaultRowGroupSize   = 128 * 1024 * 1024
)

type ByteArray string

func (d ByteArray) MarshalJSON() ([]byte, error) {
	encoded := base64.StdEncoding.EncodeToString([]byte(d))
	return json.Marshal(encoded)
}

func (d *ByteArray) UnmarshalJSON(data []byte) error {
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return err
	}
	*d = ByteArray(decoded)
	return nil
}

type fieldAttr struct {
	Type           string
	Length         int32
	Scale          int32
	Precision      int32
	Encoding       parquet.Encoding
	OmitStats      bool
	RepetitionType parquet.FieldRepetitionType

	convertedType     string
	isAdjustedToUTC   bool
	fieldID           int32
	logicalTypeFields map[string]string
}

func (mp *fieldAttr) update(key, val string) error {
	var err error
	switch key {
	case "type":
		mp.Type = val
	case "convertedtype":
		mp.convertedType = val
	case "length":
		if mp.Length, err = str2Int32(val); err != nil {
			return fmt.Errorf("failed to parse length: %s", err.Error())
		}
	case "scale":
		if mp.Scale, err = str2Int32(val); err != nil {
			return fmt.Errorf("failed to parse scale: %s", err.Error())
		}
	case "precision":
		if mp.Precision, err = str2Int32(val); err != nil {
			return fmt.Errorf("failed to parse precision: %s", err.Error())
		}
	case "fieldid":
		if mp.fieldID, err = str2Int32(val); err != nil {
			return fmt.Errorf("failed to parse fieldid: %s", err.Error())
		}
	case "isadjustedtoutc":
		if mp.isAdjustedToUTC, err = str2Bool(val); err != nil {
			return fmt.Errorf("failed to parse isadjustedtoutc: %s", err.Error())
		}
	case "omitstats":
		if mp.OmitStats, err = str2Bool(val); err != nil {
			return fmt.Errorf("failed to parse omitstats: %s", err.Error())
		}
	case "repetitiontype":
		mp.RepetitionType, err = parquet.FieldRepetitionTypeFromString(strings.ToUpper(val))
		if err != nil {
			return fmt.Errorf("failed to parse repetitiontype: %w", err)
		}
	case "encoding":
		mp.Encoding, err = parquet.EncodingFromString(strings.ToUpper(val))
		if err != nil {
			return fmt.Errorf("failed to parse encoding: %w", err)
		}
	default:
		if strings.HasPrefix(key, "logicaltype") {
			if mp.logicalTypeFields == nil {
				mp.logicalTypeFields = make(map[string]string)
			}
			mp.logicalTypeFields[key] = val
		} else {
			return fmt.Errorf("unrecognized tag '%v'", key)
		}
	}
	return nil
}

type Tag struct {
	InName string
	ExName string
	fieldAttr
	Key   fieldAttr
	Value fieldAttr
}

func NewTag() *Tag {
	return &Tag{}
}

func StringToTag(tag string) (*Tag, error) {
	mp := NewTag()
	tagStr := strings.Replace(tag, "\t", "", -1)

	for tag := range strings.SplitSeq(tagStr, ",") {
		kv := strings.SplitN(tag, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("expect 'key=value' but got '%s'", tag)
		}
		key, val := kv[0], kv[1]
		key = strings.ToLower(key)
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)

		if key == "name" {
			if mp.InName == "" {
				mp.InName = StringToVariableName(val)
			}
			mp.ExName = val
			continue
		}

		if key == "inname" {
			mp.InName = val
			continue
		}

		var err error
		if strings.HasPrefix(key, "key") {
			err = mp.Key.update(strings.TrimPrefix(key, "key"), val)
		} else if strings.HasPrefix(key, "value") {
			err = mp.Value.update(strings.TrimPrefix(key, "value"), val)
		} else {
			err = mp.fieldAttr.update(key, val)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse tag '%s': %w", tag, err)
		}
	}
	return mp, nil
}

func NewSchemaElementFromTagMap(info *Tag) (*parquet.SchemaElement, error) {
	schema := parquet.NewSchemaElement()
	schema.Name = info.InName
	schema.TypeLength = &info.Length
	schema.Scale = &info.Scale
	schema.Precision = &info.Precision
	schema.FieldID = &info.fieldID
	schema.RepetitionType = &info.RepetitionType
	schema.NumChildren = nil

	if t, err := parquet.TypeFromString(info.Type); err == nil {
		schema.Type = &t
	} else {
		return nil, fmt.Errorf("field [%s] with type [%s]: %s", info.InName, info.Type, err.Error())
	}

	if ct, err := parquet.ConvertedTypeFromString(info.convertedType); err == nil {
		schema.ConvertedType = &ct
	}

	var logicalType *parquet.LogicalType
	var err error
	if len(info.logicalTypeFields) > 0 {
		logicalType, err = newLogicalTypeFromFieldsMap(info.logicalTypeFields)
		if err != nil {
			return nil, fmt.Errorf("failed to create logicaltype from field map: %s", err.Error())
		}
	} else {
		logicalType = newLogicalTypeFromConvertedType(schema, info)
	}

	schema.LogicalType = logicalType

	return schema, nil
}

func newTimeUnitFromString(unitStr string) (*parquet.TimeUnit, error) {
	unit := parquet.NewTimeUnit()
	switch unitStr {
	case "MILLIS":
		unit.MILLIS = parquet.NewMilliSeconds()
	case "MICROS":
		unit.MICROS = parquet.NewMicroSeconds()
	case "NANOS":
		unit.NANOS = parquet.NewNanoSeconds()
	default:
		return nil, fmt.Errorf("logicaltype time error, unknown unit: %s", unitStr)
	}
	return unit, nil
}

func newLogicalTypeFromFieldsMap(mp map[string]string) (*parquet.LogicalType, error) {
	val, ok := mp["logicaltype"]
	if !ok {
		return nil, errors.New("does not have logicaltype")
	}

	var err error
	logicalType := parquet.NewLogicalType()
	switch val {
	case "STRING":
		logicalType.STRING = parquet.NewStringType()
	case "MAP":
		logicalType.MAP = parquet.NewMapType()
	case "LIST":
		logicalType.LIST = parquet.NewListType()
	case "ENUM":
		logicalType.ENUM = parquet.NewEnumType()
	case "DECIMAL":
		logicalType.DECIMAL = parquet.NewDecimalType()
		if logicalType.DECIMAL.Precision, err = str2Int32(mp["logicaltype.precision"]); err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.precision as int32: %s", err.Error())
		}
		if logicalType.DECIMAL.Scale, err = str2Int32(mp["logicaltype.scale"]); err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.scale as int32: %s", err.Error())
		}
	case "DATE":
		logicalType.DATE = parquet.NewDateType()
	case "TIME":
		logicalType.TIME = parquet.NewTimeType()
		if logicalType.TIME.IsAdjustedToUTC, err = str2Bool(mp["logicaltype.isadjustedtoutc"]); err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.isadjustedtoutc as boolean: %s", err.Error())
		}
		if logicalType.TIME.Unit, err = newTimeUnitFromString(mp["logicaltype.unit"]); err != nil {
			return nil, err
		}
	case "TIMESTAMP":
		logicalType.TIMESTAMP = parquet.NewTimestampType()
		if logicalType.TIMESTAMP.IsAdjustedToUTC, err = str2Bool(mp["logicaltype.isadjustedtoutc"]); err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.isadjustedtoutc as boolean: %s", err.Error())
		}
		if logicalType.TIMESTAMP.Unit, err = newTimeUnitFromString(mp["logicaltype.unit"]); err != nil {
			return nil, err
		}
	case "INTEGER":
		logicalType.INTEGER = parquet.NewIntType()
		bitWidth, err := str2Int32(mp["logicaltype.bitwidth"])
		if err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.bitwidth as int32: %s", err.Error())
		}
		logicalType.INTEGER.BitWidth = int8(bitWidth)
		if logicalType.INTEGER.IsSigned, err = str2Bool(mp["logicaltype.issigned"]); err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.issigned as boolean: %s", err.Error())
		}
	case "JSON":
		logicalType.JSON = parquet.NewJsonType()
	case "BSON":
		logicalType.BSON = parquet.NewBsonType()
	case "UUID":
		logicalType.UUID = parquet.NewUUIDType()
	default:
		return nil, fmt.Errorf("unknown logicaltype: %s", val)
	}

	return logicalType, nil
}

var intAttrMap = map[parquet.ConvertedType]struct {
	bitWidth int8
	isSigned bool
}{
	parquet.ConvertedType_INT_8:   {8, true},
	parquet.ConvertedType_INT_16:  {16, true},
	parquet.ConvertedType_INT_32:  {32, true},
	parquet.ConvertedType_INT_64:  {64, true},
	parquet.ConvertedType_UINT_8:  {8, false},
	parquet.ConvertedType_UINT_16: {16, false},
	parquet.ConvertedType_UINT_32: {32, false},
	parquet.ConvertedType_UINT_64: {64, false},
}

func newLogicalTypeFromConvertedType(schemaElement *parquet.SchemaElement, info *Tag) *parquet.LogicalType {
	if schemaElement.ConvertedType == nil {
		return nil
	}

	logicalType := parquet.NewLogicalType()
	if attr, ok := intAttrMap[*schemaElement.ConvertedType]; ok {
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = attr.bitWidth
		logicalType.INTEGER.IsSigned = attr.isSigned
		return logicalType
	}

	switch *schemaElement.ConvertedType {
	case parquet.ConvertedType_DECIMAL:
		logicalType.DECIMAL = parquet.NewDecimalType()
		logicalType.DECIMAL.Precision = info.Precision
		logicalType.DECIMAL.Scale = info.Scale
	case parquet.ConvertedType_DATE:
		logicalType.DATE = parquet.NewDateType()
	case parquet.ConvertedType_TIME_MICROS, parquet.ConvertedType_TIME_MILLIS:
		logicalType.TIME = parquet.NewTimeType()
		logicalType.TIME.IsAdjustedToUTC = info.isAdjustedToUTC
		logicalType.TIME.Unit, _ = newTimeUnitFromString(schemaElement.ConvertedType.String()[5:])
	case parquet.ConvertedType_TIMESTAMP_MICROS, parquet.ConvertedType_TIMESTAMP_MILLIS:
		logicalType.TIMESTAMP = parquet.NewTimestampType()
		logicalType.TIMESTAMP.IsAdjustedToUTC = info.isAdjustedToUTC
		logicalType.TIMESTAMP.Unit, _ = newTimeUnitFromString(schemaElement.ConvertedType.String()[10:])
	case parquet.ConvertedType_BSON:
		logicalType.BSON = parquet.NewBsonType()
	case parquet.ConvertedType_ENUM:
		logicalType.ENUM = parquet.NewEnumType()
	case parquet.ConvertedType_JSON:
		logicalType.JSON = parquet.NewJsonType()
	case parquet.ConvertedType_LIST:
		logicalType.LIST = parquet.NewListType()
	case parquet.ConvertedType_MAP:
		logicalType.MAP = parquet.NewMapType()
	case parquet.ConvertedType_UTF8:
		logicalType.STRING = parquet.NewStringType()
	default:
		return nil
	}

	return logicalType
}

func DeepCopy(src, dst *Tag) {
	*dst = *src
	dst.logicalTypeFields = nil
	dst.Key.logicalTypeFields = nil
	dst.Value.logicalTypeFields = nil
}

// Get key tag map for map
func GetKeyTagMap(src *Tag) *Tag {
	res := NewTag()
	res.InName = "Key"
	res.ExName = "key"
	res.fieldAttr = src.Key
	res.logicalTypeFields = nil
	return res
}

// Get value tag map for map
func GetValueTagMap(src *Tag) *Tag {
	res := NewTag()
	res.InName = "Value"
	res.ExName = "value"
	res.fieldAttr = src.Value
	res.logicalTypeFields = nil
	return res
}

// Convert string to a golang variable name
func StringToVariableName(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	name := ""
	for i := range ln {
		c := str[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			name += string(c)
		} else {
			name += strconv.Itoa(int(c))
		}
	}

	name = headToUpper(name)
	return name
}

// Convert the first letter of a string to uppercase
func headToUpper(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	c := str[0]
	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
		return strings.ToUpper(str[0:1]) + str[1:]
	}
	// handle non-alpha prefix such as "_"
	return "PARGO_PREFIX_" + str
}

func str2Int32(val string) (int32, error) {
	valInt, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}
	return int32(valInt), nil
}

func str2Bool(val string) (bool, error) {
	valBoolean, err := strconv.ParseBool(val)
	if err != nil {
		return false, err
	}
	return valBoolean, nil
}

// . -> \x01
func ReformPathStr(pathStr string) string {
	return strings.ReplaceAll(pathStr, ".", PAR_GO_PATH_DELIMITER)
}

// Convert path slice to string
func PathToStr(path []string) string {
	return strings.Join(path, PAR_GO_PATH_DELIMITER)
}

// Convert string to path slice
func StrToPath(str string) []string {
	return strings.Split(str, PAR_GO_PATH_DELIMITER)
}

// Get the pathStr index in a path
func PathStrIndex(str string) int {
	return len(strings.Split(str, PAR_GO_PATH_DELIMITER))
}

func IsChildPath(parent, child string) bool {
	ln := len(parent)
	return strings.HasPrefix(child, parent) && (len(child) == ln || child[ln] == PAR_GO_PATH_DELIMITER[0])
}

func ToPtr[T any](value T) *T {
	return &value
}
