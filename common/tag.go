package common

import (
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
)

type fieldAttr struct {
	Type            string
	Length          int32
	Scale           int32
	Precision       int32
	Encoding        parquet.Encoding
	OmitStats       bool
	RepetitionType  parquet.FieldRepetitionType
	CompressionType *parquet.CompressionCodec // nil means use file-level compression
	BloomFilter     bool                      // enable bloom filter for this column
	BloomFilterSize int32                     // bloom filter size in bytes (0 = default)

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
		valInt, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return fmt.Errorf("parse length value '%s': %w", val, err)
		}
		mp.Length = int32(valInt)
	case "scale":
		valInt, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return fmt.Errorf("parse scale value '%s': %w", val, err)
		}
		mp.Scale = int32(valInt)
	case "precision":
		valInt, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return fmt.Errorf("parse precision value '%s': %w", val, err)
		}
		mp.Precision = int32(valInt)
	case "fieldid":
		valInt, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return fmt.Errorf("parse fieldid value '%s': %w", val, err)
		}
		mp.fieldID = int32(valInt)
	case "isadjustedtoutc":
		if mp.isAdjustedToUTC, err = strconv.ParseBool(val); err != nil {
			return fmt.Errorf("parse isadjustedtoutc value '%s': %w", val, err)
		}
	case "omitstats":
		if mp.OmitStats, err = strconv.ParseBool(val); err != nil {
			return fmt.Errorf("parse omitstats value '%s': %w", val, err)
		}
	case "bloomfilter":
		if mp.BloomFilter, err = strconv.ParseBool(val); err != nil {
			return fmt.Errorf("parse bloomfilter value '%s': %w", val, err)
		}
	case "bloomfiltersize":
		valInt, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return fmt.Errorf("parse bloomfiltersize value '%s': %w", val, err)
		}
		mp.BloomFilterSize = int32(valInt)
	case "repetitiontype":
		mp.RepetitionType, err = parquet.FieldRepetitionTypeFromString(strings.ToUpper(val))
		if err != nil {
			return fmt.Errorf("parse repetitiontype: %w", err)
		}
	case "encoding":
		mp.Encoding, err = parquet.EncodingFromString(strings.ToUpper(val))
		if err != nil {
			return fmt.Errorf("parse encoding: %w", err)
		}
	case "compression":
		codec, err := parquet.CompressionCodecFromString(strings.ToUpper(val))
		if err != nil {
			return fmt.Errorf("parse compression: %w", err)
		}
		mp.CompressionType = &codec
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

func StringToTag(tag string) (*Tag, error) {
	mp := &Tag{}
	tagStr := strings.ReplaceAll(tag, "\t", "")

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
			err = mp.update(key, val)
		}
		if err != nil {
			return nil, fmt.Errorf("parse tag '%s': %w", tag, err)
		}
	}
	return mp, nil
}

func DeepCopy(src, dst *Tag) {
	*dst = *src
	if src.logicalTypeFields != nil {
		dst.logicalTypeFields = make(map[string]string)
		maps.Copy(dst.logicalTypeFields, src.logicalTypeFields)
	}
	if src.Key.logicalTypeFields != nil {
		dst.Key.logicalTypeFields = make(map[string]string)
		maps.Copy(dst.Key.logicalTypeFields, src.Key.logicalTypeFields)
	}
	if src.Value.logicalTypeFields != nil {
		dst.Value.logicalTypeFields = make(map[string]string)
		maps.Copy(dst.Value.logicalTypeFields, src.Value.logicalTypeFields)
	}
}

// Get key tag map for map
func GetKeyTagMap(src *Tag) *Tag {
	res := &Tag{}
	res.InName = "Key"
	res.ExName = "key"
	res.fieldAttr = src.Key
	if src.Key.logicalTypeFields != nil {
		res.logicalTypeFields = make(map[string]string)
		maps.Copy(res.logicalTypeFields, src.Key.logicalTypeFields)
	}
	return res
}

// Get value tag map for map
func GetValueTagMap(src *Tag) *Tag {
	res := &Tag{}
	res.InName = "Value"
	res.ExName = "value"
	res.fieldAttr = src.Value
	if src.Value.logicalTypeFields != nil {
		res.logicalTypeFields = make(map[string]string)
		maps.Copy(res.logicalTypeFields, src.Value.logicalTypeFields)
	}
	return res
}

// StringToVariableName converts a string to a valid Go variable name.
func StringToVariableName(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	var b strings.Builder
	b.Grow(ln)
	for i := range ln {
		c := str[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			b.WriteByte(c)
		} else {
			b.WriteString(strconv.Itoa(int(c)))
		}
	}

	return headToUpper(b.String())
}

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
