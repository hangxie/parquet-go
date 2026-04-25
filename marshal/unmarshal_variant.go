package marshal

import (
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

func processVariantReconstruction(variantReconstructors map[string]*ShreddedVariantReconstructor, root reflect.Value, prefixPath string, schemaHandler *schema.SchemaHandler, tableBgn, tableEnd map[string]int, sliceRecords map[reflect.Value]*SliceRecord) error {
	for variantPath, reconstructor := range variantReconstructors {
		numRows := countVariantRows(reconstructor, tableBgn, tableEnd)
		expandRootForVariants(root, numRows, sliceRecords)

		for rowIdx := range numRows {
			variant, err := reconstructor.Reconstruct(rowIdx, tableBgn, tableEnd)
			if err != nil {
				return fmt.Errorf("reconstruct variant at %s row %d: %w", variantPath, rowIdx, err)
			}
			if err := setVariantValue(root, variantPath, prefixPath, schemaHandler, variant, rowIdx, sliceRecords); err != nil {
				return fmt.Errorf("set variant at %s row %d: %w", variantPath, rowIdx, err)
			}
		}
	}
	return nil
}

func countVariantRows(reconstructor *ShreddedVariantReconstructor, tableBgn, tableEnd map[string]int) int {
	if reconstructor.MetadataTable == nil {
		return 0
	}
	metaPath := common.PathToStr(reconstructor.MetadataTable.Path)
	metaBgn, metaEnd := tableBgn[metaPath], tableEnd[metaPath]
	if metaBgn < 0 || metaEnd <= metaBgn {
		return 0
	}
	numRows := 0
	for i := metaBgn; i < metaEnd; i++ {
		if reconstructor.MetadataTable.RepetitionLevels[i] == 0 {
			numRows++
		}
	}
	return numRows
}

func expandRootForVariants(root reflect.Value, numRows int, sliceRecords map[reflect.Value]*SliceRecord) {
	if sliceRec, ok := sliceRecords[root]; ok && len(sliceRec.Values) >= numRows {
		return
	}
	if root.Kind() == reflect.Slice && root.Len() < numRows {
		newSlice := reflect.MakeSlice(root.Type(), numRows, numRows)
		reflect.Copy(newSlice, root)
		root.Set(newSlice)
	}
}

func isVariantNull(variant types.Variant) bool {
	return len(variant.Value) == 0 || (len(variant.Value) == 1 && variant.Value[0] == 0 && len(variant.Metadata) == 0)
}

func setDecodedOrFallback(target reflect.Value, variant types.Variant) {
	decoded, err := types.ConvertVariantValue(variant)
	if err != nil {
		target.Set(reflect.ValueOf(variant))
	} else if decoded != nil {
		target.Set(reflect.ValueOf(decoded))
	} else {
		target.Set(reflect.Zero(target.Type()))
	}
}

var variantType = reflect.TypeOf(types.Variant{})

func assignVariantDirect(po reflect.Value, variant types.Variant) error {
	if isVariantNull(variant) && po.Kind() == reflect.Interface {
		po.Set(reflect.Zero(po.Type()))
		return nil
	}
	if po.Kind() == reflect.Interface && po.Type() != variantType {
		setDecodedOrFallback(po, variant)
		return nil
	}
	po.Set(reflect.ValueOf(variant))
	return nil
}

func assignVariantPtr(po reflect.Value, variant types.Variant) error {
	if isVariantNull(variant) {
		po.Set(reflect.Zero(po.Type()))
		return nil
	}
	if po.IsNil() {
		po.Set(reflect.New(po.Type().Elem()))
	}
	if po.Type().Elem().Kind() == reflect.Interface && po.Type().Elem() != variantType {
		setDecodedOrFallback(po.Elem(), variant)
		return nil
	}
	po.Elem().Set(reflect.ValueOf(variant))
	return nil
}

func assignVariantToTarget(po reflect.Value, variant types.Variant) error {
	if po.Type() == variantType || po.Kind() == reflect.Interface {
		return assignVariantDirect(po, variant)
	}
	if po.Kind() == reflect.Ptr && (po.Type().Elem() == variantType || po.Type().Elem().Kind() == reflect.Interface) {
		return assignVariantPtr(po, variant)
	}
	return fmt.Errorf("could not set variant value at path end")
}

func navigateToVariantTarget(root reflect.Value, path []string, prefixIndex int, variant types.Variant, rowIdx int, sliceRecords map[reflect.Value]*SliceRecord) (reflect.Value, error) {
	po := root
	for i := prefixIndex; i < len(path); i++ {
		if !po.IsValid() {
			return po, fmt.Errorf("invalid reflect value at path index %d", i)
		}

		switch po.Kind() {
		case reflect.Ptr:
			if po.IsNil() {
				po.Set(reflect.New(po.Type().Elem()))
			}
			po = po.Elem()
			i--
			continue

		case reflect.Slice:
			if po.Type().Elem().Kind() == reflect.Uint8 {
				return po, fmt.Errorf("unexpected []byte at path index %d", i)
			}
			sliceRec, ok := sliceRecords[po]
			if ok && rowIdx < len(sliceRec.Values) {
				po = sliceRec.Values[rowIdx]
			} else if rowIdx < po.Len() {
				po = po.Index(rowIdx)
			} else {
				return po, fmt.Errorf("row index %d out of bounds for slice at path index %d", rowIdx, i)
			}
			i--
			continue

		case reflect.Struct:
			field := po.FieldByName(path[i])
			if !field.IsValid() {
				return po, fmt.Errorf("field %q not found at path index %d", path[i], i)
			}
			po = field

		case reflect.Interface:
			if po.IsNil() {
				if i == len(path)-1 {
					po.Set(reflect.ValueOf(variant))
					return po, nil
				}
				return po, fmt.Errorf("cannot navigate through nil interface at path index %d", i)
			}
			po = po.Elem()
			i--
			continue

		default:
			return po, fmt.Errorf("unexpected kind %v at path index %d", po.Kind(), i)
		}
	}
	return po, nil
}

// setVariantValue navigates the struct path and sets a variant value at the specified location.
func setVariantValue(root reflect.Value, variantPath, prefixPath string, _ *schema.SchemaHandler, variant types.Variant, rowIdx int, sliceRecords map[reflect.Value]*SliceRecord) error {
	path := common.StrToPath(variantPath)
	prefixIndex := common.PathStrIndex(prefixPath)

	po, err := navigateToVariantTarget(root, path, prefixIndex, variant, rowIdx, sliceRecords)
	if err != nil {
		return err
	}

	return assignVariantToTarget(po, variant)
}
