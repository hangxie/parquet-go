package marshal

import (
	"fmt"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/layout"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
)

// Marshal function for CSV like data
func MarshalCSV(records []any, schemaHandler *schema.SchemaHandler) (*map[string]*layout.Table, error) {
	res := make(map[string]*layout.Table)
	if ln := len(records); ln <= 0 {
		return &res, nil
	}

	// Infos contains root node
	fieldCount := len(schemaHandler.Infos) - 1
	for i := range fieldCount {
		pathStr := schemaHandler.GetRootInName() + common.PAR_GO_PATH_DELIMITER + schemaHandler.Infos[i+1].InName
		table := layout.NewEmptyTable()
		res[pathStr] = table
		table.Path = common.StrToPath(pathStr)

		schema := schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
		var err error
		if table.MaxDefinitionLevel, err = schemaHandler.MaxDefinitionLevel(table.Path); err != nil {
			return nil, err
		}
		if table.MaxRepetitionLevel, err = schemaHandler.MaxRepetitionLevel(table.Path); err != nil {
			return nil, err
		}

		table.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
		table.Schema = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
		table.Info = schemaHandler.Infos[i+1]
		// Pre-allocate these arrays for efficiency
		table.Values = make([]any, 0, len(records))
		table.RepetitionLevels = make([]int32, 0, len(records))
		table.DefinitionLevels = make([]int32, 0, len(records))

		isOptional := *schema.RepetitionType == parquet.FieldRepetitionType_OPTIONAL
		for j := range records {
			row := records[j].([]any)
			if len(row) < fieldCount {
				return nil, fmt.Errorf("row %d has less than %d fields", j, fieldCount)
			}
			rec := row[i]
			table.Values = append(table.Values, rec)

			table.RepetitionLevels = append(table.RepetitionLevels, 0)
			if rec == nil && isOptional {
				table.DefinitionLevels = append(table.DefinitionLevels, 0)
			} else {
				table.DefinitionLevels = append(table.DefinitionLevels, table.MaxDefinitionLevel)
			}
		}
	}
	return &res, nil
}
