package marshal

import (
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

type Marshaler interface {
	Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) (newStack []*Node, err error)
}

type ParquetPtr struct{}

func (p *ParquetPtr) Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) ([]*Node, error) {
	if node.Val.IsNil() {
		return stack, nil
	}
	node.Val = node.Val.Elem()
	node.DL++
	stack = append(stack, node)
	return stack, nil
}

type ParquetStruct struct{}

func (p *ParquetStruct) Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) ([]*Node, error) {
	var ok bool

	numField := node.Val.Type().NumField()
	for j := range numField {
		tf := node.Val.Type().Field(j)
		name := tf.Name
		newNode := nodeBuf.GetNode()

		// some ignored item
		if newNode.PathMap, ok = node.PathMap.Children[name]; !ok {
			continue
		}

		newNode.Val = node.Val.Field(j)
		newNode.RL = node.RL
		newNode.DL = node.DL
		stack = append(stack, newNode)
	}
	return stack, nil
}

type ParquetMapStruct struct {
	schemaHandler *schema.SchemaHandler
}

func (p *ParquetMapStruct) Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) ([]*Node, error) {
	var ok bool

	keys := node.Val.MapKeys()
	if len(keys) <= 0 {
		return stack, nil
	}

	// Track every child, leaf and group alike, so absent keys still get nils.
	missingKeys := make(map[string]bool)
	for k := range node.PathMap.Children {
		missingKeys[k] = true
	}
	for j := len(keys) - 1; j >= 0; j-- {
		key := keys[j]
		newNode := nodeBuf.GetNode()

		// some ignored item
		k := key.String()
		if newNode.PathMap, ok = node.PathMap.Children[k]; !ok {
			continue
		}
		missingKeys[k] = false
		v := node.Val.MapIndex(key)
		newNode.RL = node.RL
		newNode.DL = node.DL
		if v.Type().Kind() == reflect.Interface {
			newNode.Val = v.Elem()
			if newNode.Val.IsValid() && p.schemaHandler != nil &&
				p.schemaHandler.SchemaElements != nil && p.schemaHandler.MapIndex != nil {
				if index, exists := p.schemaHandler.MapIndex[newNode.PathMap.Path]; exists &&
					int(index) < len(p.schemaHandler.SchemaElements) {
					if elem := p.schemaHandler.SchemaElements[index]; elem != nil &&
						elem.RepetitionType != nil && *elem.RepetitionType != parquet.FieldRepetitionType_REQUIRED {
						newNode.DL++
					}
				}
			}
		} else {
			newNode.Val = v
		}
		stack = append(stack, newNode)
	}

	var null any
	for k, isMissing := range missingKeys {
		if isMissing {
			newNode := nodeBuf.GetNode()
			newNode.PathMap = node.PathMap.Children[k]
			newNode.Val = reflect.ValueOf(null)
			newNode.RL = node.RL
			newNode.DL = node.DL
			stack = append(stack, newNode)
		}
	}
	return stack, nil
}

type ParquetSlice struct {
	schemaHandler *schema.SchemaHandler
}

func (p *ParquetSlice) Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) ([]*Node, error) {
	ln := node.Val.Len()
	pathMap := node.PathMap
	path := node.PathMap.Path
	if p.schemaHandler != nil && p.schemaHandler.SchemaElements != nil && p.schemaHandler.MapIndex != nil {
		if index, exists := p.schemaHandler.MapIndex[node.PathMap.Path]; exists && int(index) < len(p.schemaHandler.SchemaElements) {
			if elem := p.schemaHandler.SchemaElements[index]; elem != nil && elem.RepetitionType != nil && *elem.RepetitionType != parquet.FieldRepetitionType_REPEATED {
				if pmList, ok := pathMap.Children["List"]; ok {
					if pmElement, ok := pmList.Children["Element"]; ok {
						pathMap = pmElement
						path = path + common.ParGoPathDelimiter + "List" + common.ParGoPathDelimiter + "Element"
					} else {
						return stack, nil
					}
				} else {
					return stack, nil
				}
			}
		}
	}
	if ln <= 0 {
		return stack, nil
	}

	rlNow, err := p.schemaHandler.MaxRepetitionLevel(common.StrToPath(path))
	if err != nil {
		return nil, fmt.Errorf("max repetition level for %s: %w", path, err)
	}
	for j := ln - 1; j >= 0; j-- {
		newNode := nodeBuf.GetNode()
		newNode.PathMap = pathMap
		v := node.Val.Index(j)
		if v.Type().Kind() == reflect.Interface {
			newNode.Val = v.Elem()
		} else {
			newNode.Val = v
		}
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		newNode.DL = node.DL + 1
		stack = append(stack, newNode)
	}
	return stack, nil
}

type ParquetMap struct {
	schemaHandler *schema.SchemaHandler
}

func (p *ParquetMap) Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) ([]*Node, error) {
	path := node.PathMap.Path + common.ParGoPathDelimiter + "Key_value"
	keys := node.Val.MapKeys()
	if len(keys) <= 0 {
		return stack, nil
	}

	rlNow, err := p.schemaHandler.MaxRepetitionLevel(common.StrToPath(path))
	if err != nil {
		return nil, fmt.Errorf("max repetition level for %s: %w", path, err)
	}
	for j := len(keys) - 1; j >= 0; j-- {
		key := keys[j]
		value := node.Val.MapIndex(key)
		newNode := nodeBuf.GetNode()
		newNode.PathMap = node.PathMap.Children["Key_value"].Children["Key"]
		newNode.Val = key
		newNode.DL = node.DL + 1
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		stack = append(stack, newNode)

		newNode = nodeBuf.GetNode()
		newNode.PathMap = node.PathMap.Children["Key_value"].Children["Value"]
		newNode.Val = value
		newNode.DL = node.DL + 1
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		stack = append(stack, newNode)
	}
	return stack, nil
}
