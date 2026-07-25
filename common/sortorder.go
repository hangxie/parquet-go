package common

import "github.com/hangxie/parquet-go/v3/parquet"

// IsSignedSortOrder reports whether a column's sort order is SIGNED, gating the
// deprecated signed Statistics.Min/Max fields (PARQUET-251). It follows the
// parquet-mr/Arrow classification and FindFuncTable's type precedence.
func IsSignedSortOrder(pT *parquet.Type, cT *parquet.ConvertedType, logT *parquet.LogicalType) bool {
	if cT != nil {
		switch *cT {
		case parquet.ConvertedType_UTF8, parquet.ConvertedType_ENUM,
			parquet.ConvertedType_JSON, parquet.ConvertedType_BSON,
			parquet.ConvertedType_INTERVAL,
			parquet.ConvertedType_UINT_8, parquet.ConvertedType_UINT_16,
			parquet.ConvertedType_UINT_32, parquet.ConvertedType_UINT_64:
			return false // unsigned
		case parquet.ConvertedType_INT_8, parquet.ConvertedType_INT_16,
			parquet.ConvertedType_INT_32, parquet.ConvertedType_INT_64,
			parquet.ConvertedType_DECIMAL, parquet.ConvertedType_DATE,
			parquet.ConvertedType_TIME_MILLIS, parquet.ConvertedType_TIME_MICROS,
			parquet.ConvertedType_TIMESTAMP_MILLIS, parquet.ConvertedType_TIMESTAMP_MICROS:
			return true // signed
		}
	}

	if logT != nil {
		switch {
		case logT.STRING != nil, logT.ENUM != nil, logT.JSON != nil,
			logT.BSON != nil, logT.UUID != nil:
			return false // unsigned
		case logT.INTEGER != nil:
			return logT.INTEGER.IsSigned
		case logT.DECIMAL != nil, logT.DATE != nil, logT.TIME != nil,
			logT.TIMESTAMP != nil, logT.FLOAT16 != nil:
			// FLOAT16 has a numeric total order like FLOAT/DOUBLE.
			return true // signed
		case logT.UNKNOWN != nil, logT.VARIANT != nil,
			logT.GEOMETRY != nil, logT.GEOGRAPHY != nil:
			return false // unknown order
		}
	}

	if pT != nil {
		switch *pT {
		case parquet.Type_BOOLEAN, parquet.Type_INT32, parquet.Type_INT64,
			parquet.Type_FLOAT, parquet.Type_DOUBLE:
			// BOOLEAN follows parquet-mr's defaultSortOrder (SIGNED); harmless
			// as its single 0x00/0x01 byte orders identically either way.
			return true // signed
		case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY,
			parquet.Type_INT96:
			return false // unsigned (BYTE_ARRAY/FIXED) or unknown (INT96)
		}
	}

	return false
}
