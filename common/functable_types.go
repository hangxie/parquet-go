package common

type boolFuncTable struct{}

func (boolFuncTable) LessThan(a, b any) bool {
	return !a.(bool) && b.(bool)
}

func (table boolFuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 1
}

type int32FuncTable struct{}

func (int32FuncTable) LessThan(a, b any) bool {
	return a.(int32) < b.(int32)
}

func (table int32FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 4
}

type uint32FuncTable struct{}

func (uint32FuncTable) LessThan(a, b any) bool {
	return uint32(a.(int32)) < uint32(b.(int32))
}

func (table uint32FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 4
}

type int64FuncTable struct{}

func (int64FuncTable) LessThan(a, b any) bool {
	return a.(int64) < b.(int64)
}

func (table int64FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 8
}

type uint64FuncTable struct{}

func (uint64FuncTable) LessThan(a, b any) bool {
	return uint64(a.(int64)) < uint64(b.(int64))
}

func (table uint64FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 8
}

type int96FuncTable struct{}

func (int96FuncTable) LessThan(ai, bi any) bool {
	aStr, aOk := ai.(string)
	bStr, bOk := bi.(string)
	if !aOk || !bOk {
		return false
	}

	a, b := []byte(aStr), []byte(bStr)
	if len(a) < 12 || len(b) < 12 {
		return false
	}

	fa, fb := a[11]>>7, b[11]>>7
	if fa > fb {
		return true
	} else if fa < fb {
		return false
	}
	for i := 11; i >= 0; i-- {
		if a[i] < b[i] {
			return true
		} else if a[i] > b[i] {
			return false
		}
	}
	return false
}

func (table int96FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

type float32FuncTable struct{}

func (float32FuncTable) LessThan(a, b any) bool {
	return a.(float32) < b.(float32)
}

func (table float32FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 4
}

type float64FuncTable struct{}

func (float64FuncTable) LessThan(a, b any) bool {
	return a.(float64) < b.(float64)
}

func (table float64FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 8
}

type stringFuncTable struct{}

func (stringFuncTable) LessThan(a, b any) bool {
	return a.(string) < b.(string)
}

func (table stringFuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

type intervalFuncTable struct{}

func (intervalFuncTable) LessThan(ai, bi any) bool {
	aStr, aOk := ai.(string)
	bStr, bOk := bi.(string)
	if !aOk || !bOk {
		return false
	}

	a, b := []byte(aStr), []byte(bStr)
	if len(a) < 12 || len(b) < 12 {
		return false
	}

	for i := 11; i >= 0; i-- {
		if a[i] > b[i] {
			return false
		} else if a[i] < b[i] {
			return true
		}
	}
	return false
}

func (table intervalFuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

type decimalStringFuncTable struct{}

func (decimalStringFuncTable) LessThan(a, b any) bool {
	return cmpIntBinary(a.(string), b.(string), "BigEndian", true)
}

func (table decimalStringFuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}
