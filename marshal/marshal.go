package marshal

import (
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/schema"
)

type Node struct {
	Val     reflect.Value
	PathMap *schema.PathMapType
	RL      int32
	DL      int32
}

// NodeBuf
type NodeBufType struct {
	Index int
	Buf   []*Node
}

func NewNodeBuf(ln int) *NodeBufType {
	nodeBuf := new(NodeBufType)
	nodeBuf.Index = 0
	nodeBuf.Buf = make([]*Node, ln)
	for i := range ln {
		nodeBuf.Buf[i] = new(Node)
	}
	return nodeBuf
}

func (nbt *NodeBufType) GetNode() *Node {
	if nbt.Index >= len(nbt.Buf) {
		nbt.Buf = append(nbt.Buf, new(Node))
	}
	nbt.Index++
	return nbt.Buf[nbt.Index-1]
}

func (nbt *NodeBufType) Reset() {
	nbt.Index = 0
}

func processNode(node *Node, res map[string]*layout.Table, schemaHandler *schema.SchemaHandler, nodeBuf *NodeBufType, stack []*Node) ([]*Node, error) {
	schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
	se := schemaHandler.SchemaElements[schemaIndex]

	if newStack, handled, handleErr := HandleVariant(node, se, res, schemaHandler, nodeBuf, stack); handleErr != nil {
		return nil, handleErr
	} else if handled {
		return newStack, nil
	}

	if newStack, handled, handleErr := HandleUnknown(node, se, res, stack); handleErr != nil {
		return nil, handleErr
	} else if handled {
		return newStack, nil
	}

	// Validate group nodes before primitive dispatch: an absent value back-fills
	// the subtree as a missing group; a present non-group value is a mismatch.
	if se.GetNumChildren() > 0 {
		if !node.Val.IsValid() {
			appendNilToChildren(node, res, schemaHandler)
			return stack, nil
		}
		switch node.Val.Kind() {
		case reflect.Map, reflect.Struct, reflect.Pointer, reflect.Interface:
			// valid group containers — fall through to marshaler dispatch
		case reflect.Slice:
			if isByteSlice(node) { // a byte slice is a scalar, never a group
				return nil, fmt.Errorf("expected a group for %s, got []byte", node.PathMap.Path)
			}
		default:
			return nil, fmt.Errorf("expected a group for %s, got %s", node.PathMap.Path, node.Val.Kind())
		}
	}

	// []byte should be treated as primitive BYTE_ARRAY, not as a LIST
	if isByteSlice(node) {
		if err := marshalPrimitiveValue(node, res, schemaHandler); err != nil {
			return nil, fmt.Errorf("marshal byte slice for %s: %w", node.PathMap.Path, err)
		}
		return stack, nil
	}

	m := selectMarshaler(node, schemaHandler)
	if m == nil {
		if err := marshalPrimitiveValue(node, res, schemaHandler); err != nil {
			return nil, fmt.Errorf("marshal primitive for %s: %w", node.PathMap.Path, err)
		}
		return stack, nil
	}

	oldLen := len(stack)
	var err error
	if stack, err = m.Marshal(node, nodeBuf, stack); err != nil {
		return nil, fmt.Errorf("marshal %s: %w", node.PathMap.Path, err)
	}
	if len(stack) == oldLen {
		appendNilToChildren(node, res, schemaHandler)
	}
	return stack, nil
}

func Marshal(srcInterface []any, schemaHandler *schema.SchemaHandler) (tb *map[string]*layout.Table, err error) {
	src := reflect.ValueOf(srcInterface)
	res, err := setupTableMap(schemaHandler, len(srcInterface))
	if err != nil {
		return nil, fmt.Errorf("setup table map: %w", err)
	}
	pathMap := schemaHandler.PathMap
	nodeBuf := NewNodeBuf(1)

	stack := make([]*Node, 0, 100)
	for i := range srcInterface {
		stack = stack[:0]
		nodeBuf.Reset()

		node := nodeBuf.GetNode()
		node.Val = src.Index(i)
		if src.Index(i).Type().Kind() == reflect.Interface {
			node.Val = src.Index(i).Elem()
		}
		node.PathMap = pathMap
		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node = stack[ln-1]
			stack = stack[:ln-1]

			if node.PathMap == nil {
				return nil, fmt.Errorf("internal error: node has nil PathMap")
			}

			if stack, err = processNode(node, res, schemaHandler, nodeBuf, stack); err != nil {
				return nil, fmt.Errorf("process node %s: %w", node.PathMap.Path, err)
			}
		}
	}

	return &res, nil
}

func setupTableMap(schemaHandler *schema.SchemaHandler, numElements int) (map[string]*layout.Table, error) {
	tableMap := make(map[string]*layout.Table)
	for i := range len(schemaHandler.SchemaElements) {
		schema := schemaHandler.SchemaElements[i]
		pathStr := schemaHandler.IndexMap[int32(i)]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			table := layout.NewEmptyTable()
			table.Path = common.StrToPath(pathStr)
			var err error
			if table.MaxDefinitionLevel, err = schemaHandler.MaxDefinitionLevel(table.Path); err != nil {
				return nil, fmt.Errorf("max definition level for %v: %w", table.Path, err)
			}
			if table.MaxRepetitionLevel, err = schemaHandler.MaxRepetitionLevel(table.Path); err != nil {
				return nil, fmt.Errorf("max repetition level for %v: %w", table.Path, err)
			}
			table.RepetitionType = schema.GetRepetitionType()
			table.Schema = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
			table.Info = schemaHandler.Infos[i]
			// Pre-size tables under the assumption that they'll be filled.
			table.Values = make([]any, 0, numElements)
			table.DefinitionLevels = make([]int32, 0, numElements)
			table.RepetitionLevels = make([]int32, 0, numElements)
			tableMap[pathStr] = table
		}
	}
	return tableMap, nil
}
