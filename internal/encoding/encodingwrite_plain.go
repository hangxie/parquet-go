package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"reflect"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func ToInt64(nums []any) []int64 { // convert bool/int values to int64 values
	ln := len(nums)
	res := make([]int64, ln)
	if ln <= 0 {
		return res
	}
	tk := reflect.TypeOf(nums[0]).Kind()
	for i := range ln {
		if tk == reflect.Bool {
			if nums[i].(bool) {
				res[i] = 1
			} else {
				res[i] = 0
			}
		} else {
			res[i] = int64(reflect.ValueOf(nums[i]).Int())
		}
	}
	return res
}

func WritePlain(src []any, pt parquet.Type) ([]byte, error) {
	switch pt {
	case parquet.Type_BOOLEAN:
		return WritePlainBOOLEAN(src)
	case parquet.Type_INT32:
		return WritePlainINT32(src)
	case parquet.Type_INT64:
		return WritePlainINT64(src)
	case parquet.Type_INT96:
		return WritePlainINT96(src), nil
	case parquet.Type_FLOAT:
		return WritePlainFLOAT(src)
	case parquet.Type_DOUBLE:
		return WritePlainDOUBLE(src)
	case parquet.Type_BYTE_ARRAY:
		return WritePlainBYTE_ARRAY(src)
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return WritePlainFIXED_LEN_BYTE_ARRAY(src)
	default:
		return nil, fmt.Errorf("unsupported parquet type: %v", pt)
	}
}

func WritePlainBOOLEAN(nums []any) ([]byte, error) {
	ln := len(nums)
	byteNum := (ln + 7) / 8
	res := make([]byte, byteNum)
	for i := range ln {
		tmp, ok := nums[i].(bool)
		if !ok {
			return nil, fmt.Errorf("[%v] is not a bool", nums[i])
		}
		if tmp {
			res[i/8] = res[i/8] | (1 << uint32(i%8))
		}
	}
	return res, nil
}

func WritePlainINT32(nums []any) ([]byte, error) {
	bufWriter := new(bytes.Buffer)
	if err := BinaryWriteINT32(bufWriter, nums); err != nil {
		return nil, err
	}
	return bufWriter.Bytes(), nil
}

func WritePlainINT64(nums []any) ([]byte, error) {
	bufWriter := new(bytes.Buffer)
	if err := BinaryWriteINT64(bufWriter, nums); err != nil {
		return nil, err
	}
	return bufWriter.Bytes(), nil
}

func WritePlainINT96(nums []any) []byte {
	bufWriter := new(bytes.Buffer)
	for i := range nums {
		bufWriter.WriteString(nums[i].(string))
	}
	return bufWriter.Bytes()
}

func WritePlainFLOAT(nums []any) ([]byte, error) {
	bufWriter := new(bytes.Buffer)
	if err := BinaryWriteFLOAT32(bufWriter, nums); err != nil {
		return nil, err
	}
	return bufWriter.Bytes(), nil
}

func WritePlainDOUBLE(nums []any) ([]byte, error) {
	bufWriter := new(bytes.Buffer)
	if err := BinaryWriteFLOAT64(bufWriter, nums); err != nil {
		return nil, err
	}
	return bufWriter.Bytes(), nil
}

func WritePlainBYTE_ARRAY(arrays []any) ([]byte, error) {
	bufLen := 0
	for i := range arrays {
		bufLen += 4 + len(arrays[i].(string))
	}

	buf := make([]byte, bufLen)
	pos := 0
	for i := range arrays {
		value, ok := arrays[i].(string)
		if !ok {
			return nil, fmt.Errorf("[%v] is not a string", arrays[i])
		}
		binary.LittleEndian.PutUint32(buf[pos:], uint32(len(value)))
		pos += 4
		copy(buf[pos:pos+len(value)], value)
		pos += len(value)
	}
	return buf, nil
}

func WritePlainFIXED_LEN_BYTE_ARRAY(arrays []any) ([]byte, error) {
	bufWriter := new(bytes.Buffer)
	cnt := len(arrays)
	for i := range cnt {
		tmp, ok := arrays[i].(string)
		if !ok {
			return nil, fmt.Errorf("[%v] is not a string", arrays[i])
		}
		bufWriter.WriteString(tmp)
	}
	return bufWriter.Bytes(), nil
}

func WriteUnsignedVarInt(num uint64) []byte {
	byteNum := (bits.Len64(uint64(num)) + 6) / 7
	if byteNum == 0 {
		return make([]byte, 1)
	}
	res := make([]byte, byteNum)

	numTmp := num
	for i := range byteNum {
		res[i] = byte(numTmp & uint64(0x7F))
		res[i] = res[i] | byte(0x80)
		numTmp = numTmp >> 7
	}
	res[byteNum-1] &= byte(0x7F)
	return res
}
