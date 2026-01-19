package marshal

import (
	"testing"

	"github.com/hangxie/parquet-go/v2/schema"
)

type TestListStructIssue struct {
	List []int32 `parquet:"name=list, type=LIST, valuetype=INT32"`
}

// TestMarshalPanicListMissingElement verifies that Marshal does not panic
// when the schema for a LIST is missing the "Element" child (e.g. non-standard or malformed schema).
func TestMarshalPanicListMissingElement(t *testing.T) {
	sh, err := schema.NewSchemaHandlerFromStruct(new(TestListStructIssue))
	if err != nil {
		t.Fatal(err)
	}

	root := sh.PathMap
	if root == nil {
		t.Fatal("root PathMap is nil")
	}

	listField := root.Children["list"]
	if listField == nil {
		// Fallback for case sensitivity or tagging issues
		listField = root.Children["List"]
	}
	if listField == nil {
		t.Fatal("list field not found in PathMap")
	}

	// Simulate the issue: Rename "Element" to "Item" so "Element" is missing
	if l := listField.Children["List"]; l != nil {
		if e := l.Children["Element"]; e != nil {
			l.Children["Item"] = e
			delete(l.Children, "Element")
		} else {
			t.Log("Element child not found under List, cannot simulate issue")
		}
	} else {
		t.Log("List child not found under list, cannot simulate issue")
	}

	data := []TestListStructIssue{
		{List: []int32{1, 2, 3}},
	}

	iface := make([]any, len(data))
	for i := range data {
		iface[i] = data[i]
	}

	// This should not panic
	_, err = Marshal(iface, sh)
	if err != nil {
		// Error is acceptable, panic is not.
		t.Logf("Marshal error (expected behavior for mismatched schema): %v", err)
	}
}
