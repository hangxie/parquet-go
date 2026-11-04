package schema

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// ValidateUniqueNames rejects sibling fields sharing an external or internal name, which leaves one unaddressable.
func (sh *SchemaHandler) ValidateUniqueNames() error {
	type group struct {
		remaining int32
		path      string
		exNames   map[string]bool
		inNames   map[string]bool
	}
	var stack []*group
	for i, se := range sh.SchemaElements {
		if se == nil {
			return fmt.Errorf("schema element %d is nil", i)
		}
		exName, inName := se.Name, se.Name
		if i < len(sh.Infos) && sh.Infos[i] != nil {
			exName, inName = sh.Infos[i].ExName, sh.Infos[i].InName
		}
		for len(stack) > 0 && stack[len(stack)-1].remaining == 0 {
			stack = stack[:len(stack)-1]
		}
		path := exName
		if len(stack) > 0 {
			parent := stack[len(stack)-1]
			parent.remaining--
			if parent.exNames[exName] || parent.inNames[inName] {
				return fmt.Errorf("duplicate column name %q under %s", exName, parent.path)
			}
			parent.exNames[exName], parent.inNames[inName] = true, true
			path = strings.Join([]string{parent.path, exName}, ".")
		}
		if n := se.GetNumChildren(); n > 0 {
			stack = append(stack, &group{remaining: n, path: path, exNames: map[string]bool{}, inNames: map[string]bool{}})
		}
	}
	return nil
}

// validateSchemaList checks that the elements describe exactly one complete root subtree.
func validateSchemaList(elements []*parquet.SchemaElement) error {
	if len(elements) == 0 {
		return fmt.Errorf("malformed schema: empty schema")
	}
	// Each element fills one pending slot and opens a slot for each child.
	// Use int64 because several int32 child counts can exceed the int32 range.
	pending := int64(1)
	for i, element := range elements {
		if element == nil {
			return fmt.Errorf("malformed schema: element %d is nil", i)
		}
		if pending == 0 {
			return fmt.Errorf("malformed schema: element %d (%q) is outside the root subtree", i, element.Name)
		}
		children := element.GetNumChildren()
		if children < 0 {
			return fmt.Errorf("malformed schema: element %d (%q) has negative num_children %d", i, element.Name, children)
		}
		pending += int64(children) - 1
	}
	if pending != 0 {
		return fmt.Errorf("malformed schema: %d unfulfilled child slots at end of schema", pending)
	}
	return nil
}
