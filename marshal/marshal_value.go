package marshal

import (
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

// Convert the objects to table map. srcInterface is a slice of objects
func marshalPrimitiveValue(node *Node, res map[string]*layout.Table, schemaHandler *schema.SchemaHandler) error {
	table := res[node.PathMap.Path]
	schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
	se := schemaHandler.SchemaElements[schemaIndex]
	var v any
	if node.Val.IsValid() {
		v = node.Val.Interface()
	}
	val, err := types.InterfaceToParquetType(v, se.Type)
	if err != nil {
		return fmt.Errorf("convert value for %s: %w", node.PathMap.Path, err)
	}
	table.Values = append(table.Values, val)
	table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
	table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
	return nil
}

func appendNilToChildren(node *Node, res map[string]*layout.Table, schemaHandler *schema.SchemaHandler) {
	path := node.PathMap.Path
	index := schemaHandler.MapIndex[path]
	numChildren := schemaHandler.SchemaElements[index].GetNumChildren()
	if numChildren > 0 {
		for key, table := range res {
			if common.IsChildPath(path, key) {
				table.Values = append(table.Values, nil)
				table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
				table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
			}
		}
	} else {
		table := res[path]
		table.Values = append(table.Values, nil)
		table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
		table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
	}
}

func selectMarshaler(node *Node, schemaHandler *schema.SchemaHandler) Marshaler {
	tk := reflect.Interface
	if node.Val.IsValid() {
		tk = node.Val.Type().Kind()
	}

	switch tk {
	case reflect.Pointer:
		return &ParquetPtr{}
	case reflect.Struct:
		return &ParquetStruct{}
	case reflect.Slice:
		return &ParquetSlice{schemaHandler: schemaHandler}
	case reflect.Map:
		schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
		sele := schemaHandler.SchemaElements[schemaIndex]
		if !sele.IsSetConvertedType() {
			return &ParquetMapStruct{schemaHandler: schemaHandler}
		}
		return &ParquetMap{schemaHandler: schemaHandler}
	default:
		return nil
	}
}

func isByteSlice(node *Node) bool {
	return node.Val.IsValid() && node.Val.Type().Kind() == reflect.Slice && node.Val.Type().Elem().Kind() == reflect.Uint8
}

// HandleUnknown enforces that UNKNOWN logical type columns always receive nil values.
func HandleUnknown(node *Node, se *parquet.SchemaElement, res map[string]*layout.Table, stack []*Node) ([]*Node, bool, error) {
	if se.LogicalType == nil || se.LogicalType.UNKNOWN == nil {
		return stack, false, nil
	}
	isNil := !node.Val.IsValid()
	if !isNil {
		switch node.Val.Kind() {
		case reflect.Pointer, reflect.Interface:
			isNil = node.Val.IsNil()
		}
	}
	if !isNil {
		return nil, false, fmt.Errorf("UNKNOWN column %s must have nil value, got non-nil", node.PathMap.Path)
	}
	table := res[node.PathMap.Path]
	table.Values = append(table.Values, nil)
	table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
	table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
	return stack, true, nil
}

func HandleVariant(
	node *Node,
	schema *parquet.SchemaElement,
	res map[string]*layout.Table,
	schemaHandler *schema.SchemaHandler,
	nodeBuf *NodeBufType,
	stack []*Node,
) ([]*Node, bool, error) {
	if schema.LogicalType == nil || schema.LogicalType.VARIANT == nil {
		return stack, false, nil
	}

	isNil := !node.Val.IsValid()
	if !isNil {
		switch node.Val.Kind() {
		case reflect.Interface, reflect.Pointer, reflect.Slice, reflect.Map:
			isNil = node.Val.IsNil()
		}
	}

	if isNil {
		for key := range node.PathMap.Children {
			newPathStr := node.PathMap.Children[key].Path
			for path, table := range res {
				if common.IsChildPath(newPathStr, path) {
					table.Values = append(table.Values, nil)
					table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
					table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
				}
			}
		}
		return stack, true, nil
	}

	v, err := types.AnyToVariant(node.Val.Interface())
	if err != nil {
		return nil, true, fmt.Errorf("convert to variant: %w", err)
	}

	// Validate that PathMap has the expected VARIANT children
	valuePathMap := node.PathMap.Children["Value"]
	metadataPathMap := node.PathMap.Children["Metadata"]
	if valuePathMap == nil || metadataPathMap == nil {
		return nil, true, fmt.Errorf("VARIANT schema missing required children (Value and/or Metadata) in PathMap")
	}

	// If the variant group is present, its definition level should be at its max
	childDL, err := schemaHandler.MaxDefinitionLevel(common.StrToPath(node.PathMap.Path))
	if err != nil {
		return nil, true, fmt.Errorf("max definition level for variant %s: %w", node.PathMap.Path, err)
	}

	// Both children already hold the encoded bytes. Routing them back through the value
	// conversion would read them as a BYTE_ARRAY column's text, which is base64.
	appendChild := func(path string, value []byte) error {
		table := res[path]
		if table == nil {
			return fmt.Errorf("VARIANT child %s has no table", path)
		}
		table.Values = append(table.Values, string(value))
		table.DefinitionLevels = append(table.DefinitionLevels, childDL)
		table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
		return nil
	}
	if err := appendChild(metadataPathMap.Path, v.Metadata); err != nil {
		return nil, true, err
	}
	if err := appendChild(valuePathMap.Path, v.Value); err != nil {
		return nil, true, err
	}

	return stack, true, nil
}
