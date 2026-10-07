package schema

import (
	"fmt"
	"strings"
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
