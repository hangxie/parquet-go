package schema

import (
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/types"
)

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

// Get object type from schema by reflect
func (sh *SchemaHandler) GetTypes() []reflect.Type {
	// Return cached types if available
	if sh.elementTypes != nil {
		return sh.elementTypes
	}

	ln := int32(len(sh.SchemaElements))

	// Build or use cached children map
	var elements [][]int32
	if sh.childrenMap != nil {
		elements = sh.childrenMap
	} else {
		elements = sh.buildChildrenMap()
		sh.childrenMap = elements
	}

	elementTypes := make([]reflect.Type, ln)

	var pos int32 = 0
	stack := make([][2]int32, 0) // stack item[0]: index of schemas; item[1]: numChildren
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
			nc := sh.SchemaElements[idx].GetNumChildren()
			pT, cT := sh.SchemaElements[idx].Type, sh.SchemaElements[idx].ConvertedType
			rT := sh.SchemaElements[idx].RepetitionType

			if nc == 0 {
				// Leaf node handling
				if rT == nil {
					// Default to REQUIRED if RepetitionType is nil
					elementTypes[idx] = types.ParquetTypeToGoReflectType(pT, nil)
					if elementTypes[idx] == nil {
						// Fallback to interface{} to avoid panics on malformed schemas
						elementTypes[idx] = reflect.TypeOf((*any)(nil)).Elem()
					}
				} else {
					switch *rT {
					case parquet.FieldRepetitionType_REPEATED:
						et := types.ParquetTypeToGoReflectType(pT, nil)
						if et == nil {
							et = reflect.TypeOf((*any)(nil)).Elem()
						}
						elementTypes[idx] = reflect.SliceOf(et)
					case parquet.FieldRepetitionType_OPTIONAL:
						et := types.ParquetTypeToGoReflectType(pT, rT)
						if et == nil {
							// Represent optional unknown as *interface{}
							elementTypes[idx] = reflect.PointerTo(reflect.TypeOf((*any)(nil)).Elem())
						} else {
							elementTypes[idx] = et
						}
					default:
						elementTypes[idx] = types.ParquetTypeToGoReflectType(pT, rT)
						if elementTypes[idx] == nil {
							elementTypes[idx] = reflect.TypeOf((*any)(nil)).Elem()
						}
					}
				}
			} else {
				if cT != nil && *cT == parquet.ConvertedType_LIST &&
					len(elements[idx]) == 1 &&
					sh.GetInName(int(elements[idx][0])) == "List" &&
					len(elements[elements[idx][0]]) == 1 &&
					sh.GetInName(int(elements[elements[idx][0]][0])) == "Element" {
					cidx := elements[elements[idx][0]][0]
					if rT != nil && *rT == parquet.FieldRepetitionType_OPTIONAL {
						elementTypes[idx] = reflect.PointerTo(reflect.SliceOf(elementTypes[cidx]))
					} else {
						elementTypes[idx] = reflect.SliceOf(elementTypes[cidx])
					}
				} else if cT != nil && *cT == parquet.ConvertedType_MAP &&
					len(elements[idx]) == 1 &&
					sh.GetInName(int(elements[idx][0])) == "Key_value" &&
					len(elements[elements[idx][0]]) == 2 &&
					sh.GetInName(int(elements[elements[idx][0]][0])) == "Key" &&
					sh.GetInName(int(elements[elements[idx][0]][1])) == "Value" {
					kIdx, vIdx := elements[elements[idx][0]][0], elements[elements[idx][0]][1]
					kT, vT := elementTypes[kIdx], elementTypes[vIdx]
					if kT == nil {
						kT = reflect.TypeOf((*any)(nil)).Elem()
					}
					if vT == nil {
						vT = reflect.TypeOf((*any)(nil)).Elem()
					}
					if rT != nil && *rT == parquet.FieldRepetitionType_OPTIONAL {
						elementTypes[idx] = reflect.PointerTo(reflect.MapOf(kT, vT))
					} else {
						elementTypes[idx] = reflect.MapOf(kT, vT)
					}
				} else {
					fields := []reflect.StructField{}
					for _, ci := range elements[idx] {
						ft := elementTypes[ci]
						if ft == nil {
							ft = reflect.TypeOf((*any)(nil)).Elem()
						}
						fields = append(fields, reflect.StructField{
							Name: sh.Infos[ci].InName,
							Type: ft,
							Tag:  reflect.StructTag(`json:"` + sh.Infos[ci].ExName + `"`),
						})
					}

					structType := reflect.StructOf(fields)

					if rT == nil || *rT == parquet.FieldRepetitionType_REQUIRED {
						elementTypes[idx] = structType
					} else if *rT == parquet.FieldRepetitionType_OPTIONAL {
						elementTypes[idx] = reflect.New(structType).Type()
					} else if *rT == parquet.FieldRepetitionType_REPEATED {
						elementTypes[idx] = reflect.SliceOf(structType)
					}
				}
			}

			stack = stack[:curlen]
		}
	}

	// Cache the computed types for future calls
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
		return nil, fmt.Errorf("[GetType] Can't find %v", prefixPath)
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
