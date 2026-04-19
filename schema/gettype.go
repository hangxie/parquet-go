package schema

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/types"
)

// VariantSchemaInfo contains information about a VARIANT schema, including
// whether it's shredded and the indices of its component columns.
type VariantSchemaInfo struct {
	MetadataIdx    int32   // Index of the metadata column (always present, required)
	ValueIdx       int32   // Index of the value column (-1 if absent in shredded variant)
	TypedValueIdxs []int32 // Indices of typed_value columns (empty if not shredded)
	IsShredded     bool    // True if the variant has typed_value columns
}

// isValidVariantSchema checks if a schema element at idx represents a valid VARIANT type
// per the Parquet spec. Supports both unshredded (2 children: metadata + value) and
// shredded variants (2+ children: metadata + optional value + typed_value columns).
//
// Unshredded format:
//
//	GROUP Variant (VARIANT)
//	├── metadata (BYTE_ARRAY, REQUIRED)
//	└── value (BYTE_ARRAY, REQUIRED)
//
// Shredded format:
//
//	GROUP Variant (VARIANT)
//	├── metadata (BYTE_ARRAY, REQUIRED)
//	├── value (BYTE_ARRAY, OPTIONAL)        ← OPTIONAL when shredded
//	└── typed_value (various types, OPTIONAL)
func isVariantMetadataChild(child *parquet.SchemaElement) bool {
	return child.Type != nil && *child.Type == parquet.Type_BYTE_ARRAY &&
		child.RepetitionType != nil && *child.RepetitionType == parquet.FieldRepetitionType_REQUIRED
}

func isVariantValueChild(child *parquet.SchemaElement) bool {
	if child.Type == nil || *child.Type != parquet.Type_BYTE_ARRAY || child.RepetitionType == nil {
		return false
	}
	rt := *child.RepetitionType
	return rt == parquet.FieldRepetitionType_REQUIRED || rt == parquet.FieldRepetitionType_OPTIONAL
}

func isOptionalChild(child *parquet.SchemaElement) bool {
	return child.RepetitionType != nil && *child.RepetitionType == parquet.FieldRepetitionType_OPTIONAL
}

func (sh *SchemaHandler) isValidVariantSchema(idx int32, children []int32) bool {
	elem := sh.SchemaElements[idx]

	if elem.LogicalType == nil || elem.LogicalType.VARIANT == nil {
		return false
	}
	if len(children) < 2 {
		return false
	}

	hasMetadata := false
	hasValueOrTyped := false

	for _, childIdx := range children {
		if int(childIdx) >= len(sh.SchemaElements) {
			continue
		}
		child := sh.SchemaElements[childIdx]
		childName := sh.GetInName(int(childIdx))

		switch {
		case strings.EqualFold(childName, "Metadata"):
			hasMetadata = isVariantMetadataChild(child)
		case strings.EqualFold(childName, "Value"):
			hasValueOrTyped = isVariantValueChild(child)
		default:
			if isOptionalChild(child) {
				hasValueOrTyped = true
			}
		}
	}

	return hasMetadata && hasValueOrTyped
}

// getVariantSchemaInfo extracts detailed information about a variant schema.
// Returns nil if the schema at idx is not a valid variant schema.
func (sh *SchemaHandler) getVariantSchemaInfo(idx int32, children []int32) *VariantSchemaInfo {
	if !sh.isValidVariantSchema(idx, children) {
		return nil
	}

	info := &VariantSchemaInfo{
		MetadataIdx:    -1,
		ValueIdx:       -1,
		TypedValueIdxs: []int32{},
		IsShredded:     false,
	}

	for _, childIdx := range children {
		child := sh.SchemaElements[childIdx]
		childName := sh.GetInName(int(childIdx))

		if strings.EqualFold(childName, "Metadata") {
			info.MetadataIdx = childIdx
		} else if strings.EqualFold(childName, "Value") {
			info.ValueIdx = childIdx
			if child.RepetitionType != nil && *child.RepetitionType == parquet.FieldRepetitionType_OPTIONAL {
				info.IsShredded = true
			}
		} else if strings.EqualFold(childName, "Typed_value") {
			if child.RepetitionType != nil && *child.RepetitionType == parquet.FieldRepetitionType_OPTIONAL {
				info.TypedValueIdxs = append(info.TypedValueIdxs, childIdx)
				info.IsShredded = true
			}
		} else {
			// Any other optional field is a shredded field
			if child.RepetitionType != nil && *child.RepetitionType == parquet.FieldRepetitionType_OPTIONAL {
				info.TypedValueIdxs = append(info.TypedValueIdxs, childIdx)
				info.IsShredded = true
			}
		}
	}

	return info
}

// buildChildrenMap builds the parent-child relationship map for all schema elements
// This is cached in SchemaHandler to avoid rebuilding on every GetType() call
func (sh *SchemaHandler) buildChildrenMap() [][]int32 {
	ln := int32(len(sh.SchemaElements))
	children := make([][]int32, ln)
	for i := range int(ln) {
		children[i] = []int32{}
	}

	var pos int32 = 0
	stack := make([][2]int32, 0)
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
				p := stack[len(stack)-1][0]
				children[p] = append(children[p], pos)
			}
			item := [2]int32{pos, sh.SchemaElements[pos].GetNumChildren()}
			stack = append(stack, item)
			pos++
		} else {
			stack = stack[:len(stack)-1]
		}
	}
	return children
}

var anyType = reflect.TypeFor[any]()

func typeOrAny(t reflect.Type) reflect.Type {
	if t == nil {
		return anyType
	}
	return t
}

func applyRepetition(t reflect.Type, rT *parquet.FieldRepetitionType) reflect.Type {
	if rT != nil && *rT == parquet.FieldRepetitionType_OPTIONAL {
		return reflect.PointerTo(t)
	}
	return t
}

func resolveLeafType(pT *parquet.Type, rT *parquet.FieldRepetitionType) reflect.Type {
	if rT == nil {
		return typeOrAny(types.ParquetTypeToGoReflectType(pT, nil))
	}
	switch *rT {
	case parquet.FieldRepetitionType_REPEATED:
		return reflect.SliceOf(typeOrAny(types.ParquetTypeToGoReflectType(pT, nil)))
	case parquet.FieldRepetitionType_OPTIONAL:
		et := types.ParquetTypeToGoReflectType(pT, rT)
		if et == nil {
			return reflect.PointerTo(anyType)
		}
		return et
	default:
		return typeOrAny(types.ParquetTypeToGoReflectType(pT, rT))
	}
}

func (sh *SchemaHandler) isListSchema(idx int32, elements [][]int32, cT *parquet.ConvertedType) bool {
	return cT != nil && *cT == parquet.ConvertedType_LIST &&
		len(elements[idx]) == 1 &&
		sh.GetInName(int(elements[idx][0])) == "List" &&
		len(elements[elements[idx][0]]) == 1 &&
		sh.GetInName(int(elements[elements[idx][0]][0])) == "Element"
}

func (sh *SchemaHandler) isMapSchema(idx int32, elements [][]int32, cT *parquet.ConvertedType) bool {
	return cT != nil && *cT == parquet.ConvertedType_MAP &&
		len(elements[idx]) == 1 &&
		sh.GetInName(int(elements[idx][0])) == "Key_value" &&
		len(elements[elements[idx][0]]) == 2 &&
		sh.GetInName(int(elements[elements[idx][0]][0])) == "Key" &&
		sh.GetInName(int(elements[elements[idx][0]][1])) == "Value"
}

func (sh *SchemaHandler) resolveStructType(idx int32, elements [][]int32, elementTypes []reflect.Type, rT *parquet.FieldRepetitionType) reflect.Type {
	fields := make([]reflect.StructField, 0, len(elements[idx]))
	for _, ci := range elements[idx] {
		fields = append(fields, reflect.StructField{
			Name: sh.Infos[ci].InName,
			Type: typeOrAny(elementTypes[ci]),
			Tag:  reflect.StructTag(`json:"` + sh.Infos[ci].ExName + `"`),
		})
	}
	structType := reflect.StructOf(fields)
	if rT == nil || *rT == parquet.FieldRepetitionType_REQUIRED {
		return structType
	} else if *rT == parquet.FieldRepetitionType_OPTIONAL {
		return reflect.New(structType).Type()
	}
	return reflect.SliceOf(structType)
}

func (sh *SchemaHandler) resolveGroupType(idx int32, elements [][]int32, elementTypes []reflect.Type, cT *parquet.ConvertedType, rT *parquet.FieldRepetitionType) reflect.Type {
	switch {
	case sh.isListSchema(idx, elements, cT):
		cidx := elements[elements[idx][0]][0]
		return applyRepetition(reflect.SliceOf(elementTypes[cidx]), rT)

	case sh.isMapSchema(idx, elements, cT):
		kIdx, vIdx := elements[elements[idx][0]][0], elements[elements[idx][0]][1]
		return applyRepetition(reflect.MapOf(typeOrAny(elementTypes[kIdx]), typeOrAny(elementTypes[vIdx])), rT)

	case sh.isValidVariantSchema(idx, elements[idx]):
		return applyRepetition(anyType, rT)

	default:
		return sh.resolveStructType(idx, elements, elementTypes, rT)
	}
}

// Get object type from schema by reflect
func (sh *SchemaHandler) GetTypes() []reflect.Type {
	if sh.elementTypes != nil {
		return sh.elementTypes
	}

	ln := int32(len(sh.SchemaElements))

	var elements [][]int32
	if sh.childrenMap != nil {
		elements = sh.childrenMap
	} else {
		elements = sh.buildChildrenMap()
		sh.childrenMap = elements
	}

	elementTypes := make([]reflect.Type, ln)

	var pos int32 = 0
	stack := make([][2]int32, 0)
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
			}
			item := [2]int32{pos, sh.SchemaElements[pos].GetNumChildren()}
			stack = append(stack, item)
			pos++
		} else {
			curlen := len(stack) - 1
			idx := stack[curlen][0]
			elem := sh.SchemaElements[idx]
			pT, cT, rT := elem.Type, elem.ConvertedType, elem.RepetitionType

			if elem.GetNumChildren() == 0 {
				elementTypes[idx] = resolveLeafType(pT, rT)
			} else {
				elementTypes[idx] = sh.resolveGroupType(idx, elements, elementTypes, cT, rT)
			}

			stack = stack[:curlen]
		}
	}

	sh.elementTypes = elementTypes
	return elementTypes
}

func (sh *SchemaHandler) GetType(prefixPath string) (reflect.Type, error) {
	prefixPath, err := sh.ConvertToInPathStr(prefixPath)
	if err != nil {
		return nil, err
	}

	ts := sh.GetTypes()
	if idx, ok := sh.MapIndex[prefixPath]; !ok {
		return nil, fmt.Errorf("GetType: path not found: %v", prefixPath)
	} else {
		// Use cached children map (built by GetTypes or buildChildrenMap)
		children := sh.childrenMap

		// Traverse subtree to find any leaf with interface{} type
		toVisit := []int32{idx}
		for len(toVisit) > 0 {
			cur := toVisit[len(toVisit)-1]
			toVisit = toVisit[:len(toVisit)-1]

			if sh.SchemaElements[cur].GetNumChildren() == 0 {
				t := ts[cur]
				if t == nil || t.Kind() == reflect.Interface {
					path := sh.IndexMap[cur]
					return nil, fmt.Errorf("corrupt or unsupported schema at %s: unknown physical type", path)
				}
				continue
			}
			toVisit = append(toVisit, children[cur]...)
		}

		return ts[idx], nil
	}
}
