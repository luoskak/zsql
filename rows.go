package zsql

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

var (
	StandardTimeLayout = "2006-01-02 15:04:05"
)

type DataRow struct {
	Fields map[string]interface{}
}

func (db *DB) Rows() []*DataRow {
	tx := db.getInstance()
	var rows []map[string]interface{}
	tx.Statement.Dest = &rows
	executeQuery(tx)
	var dataRows []*DataRow
	for _, row := range rows {
		dataRows = append(dataRows, &DataRow{Fields: row})
	}
	return dataRows
}

func (d *DataRow) GetInt32(fieldName string) int32 {
	vi := d.Fields[fieldName]
	if vi == nil {
		panic("no column for name " + fieldName)
	}
	switch v := vi.(type) {
	case int32:
		return v
	case int:
		if v < math.MinInt32 || v > math.MaxInt32 {
			panic("value of " + fieldName + " overflow for int32")
		}
		return int32(v)
	case int64:
		if v < math.MinInt32 || v > math.MaxInt32 {
			panic("value of " + fieldName + " overflow for int32")
		}
		return int32(v)
	case float32, float64:
		r := floatRoundToInt(v)
		if r < math.MinInt32 || r > math.MaxInt32 {
			panic("value of " + fieldName + " overflow for int32")
		}
		return int32(r)
	case []uint8:
		r, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			panic(err)
		}
		if r < math.MinInt32 || r > math.MaxInt32 {
			panic("value of " + fieldName + " overflow for int32")
		}
		return int32(r)
	case string:
		if v == "" {
			return 0
		}
		r, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			panic(err)
		}
		if r < math.MinInt32 || r > math.MaxInt32 {
			panic("value of " + fieldName + " overflow for int32")
		}
		return int32(r)
	default:
		panic(fmt.Sprintf("can not get type %T to int32", vi))
	}
}

func (d *DataRow) GetInt(fieldName string) int {
	vi := d.Fields[fieldName]
	if vi == nil {
		panic("no column for name " + fieldName)
	}
	switch v := vi.(type) {
	case int32:
		return int(v)
	case int:
		return v
	case int64:
		if strconv.IntSize == 64 {
			return int(v)
		}
		if v < math.MinInt32 || v > math.MaxInt32 {
			panic("value of " + fieldName + " overflow for int")
		}
		return int(v)
	case float32, float64:
		r := floatRoundToInt(v)
		if strconv.IntSize == 64 {
			return int(r)
		}
		if r < math.MinInt32 || r > math.MaxInt32 {
			panic("value of " + fieldName + " overflow for int32")
		}
		return int(r)
	case []uint8:
		r, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			panic(err)
		}
		if strconv.IntSize == 64 {
			return int(r)
		}
		if r < math.MinInt32 || r > math.MaxInt32 {
			panic("value of " + fieldName + " overflow for int32")
		}
		return int(r)
	case string:
		if v == "" {
			return 0
		}
		r, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			panic(err)
		}
		if strconv.IntSize == 64 {
			return int(r)
		}
		if r < math.MinInt32 || r > math.MaxInt32 {
			panic("value of " + fieldName + " overflow for int32")
		}
		return int(r)
	default:
		panic(fmt.Sprintf("can not get type %T to int32", vi))
	}
}

func (d *DataRow) GetInt64(fieldName string) int64 {
	vi := d.Fields[fieldName]
	if vi == nil {
		panic("no column for name " + fieldName)
	}
	switch v := vi.(type) {
	case int32:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	case float32, float64:
		return int64(floatRoundToInt(v))
	case []uint8:
		r, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			panic(err)
		}
		return r
	case string:
		if v == "" {
			return 0
		}
		r, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			panic(err)
		}
		return r
	default:
		panic(fmt.Sprintf("can not get type %T to int64", vi))
	}
}

func floatRoundToInt(vi interface{}) int64 {
	value := 0.0
	switch v := vi.(type) {
	case float32:
		value = float64(v)
	case float64:
		value = v
	default:
		panic("no float")
	}
	// round
	return int64(math.Round(value))
}

func (d *DataRow) GetFloat64(fieldName string) float64 {
	vi := d.Fields[fieldName]
	if vi == nil {
		panic("no column for name " + fieldName)
	}
	switch v := vi.(type) {
	case float32:
		return float64(v)
	case float64:
		return v
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		r, err := strconv.ParseFloat(v, 64)
		if err != nil {
			panic(err)
		}
		return r
	default:
		panic(fmt.Sprintf("can not get type %T to float64", vi))
	}
}

func (d *DataRow) GetTime(fieldName string) time.Time {
	vi := d.Fields[fieldName]
	if vi == nil {
		panic("no column for name " + fieldName)
	}
	defaultTime := time.Date(0, 0, 0, 0, 0, 0, 0, time.Local)
	switch v := vi.(type) {
	case time.Time:
		return v
	case int:
		return defaultTime.Add(time.Second * time.Duration(v))
	case string:
		if len(v) == 0 {
			return defaultTime
		}
		layout := StandardTimeLayout
		layout = layout[:len(v)]
		r, err := time.ParseInLocation(layout, v, time.Local)
		if err != nil {
			panic(err)
		}
		return r
	default:
		panic(fmt.Sprintf("can not get type %T to time.Time", vi))
	}
}

func (d *DataRow) GetTimeStr(fieldName string) string {
	return d.GetTime(fieldName).Format(StandardTimeLayout)
}

func (d *DataRow) GetString(fieldName string) string {
	vi := d.Fields[fieldName]
	if vi == nil {
		panic("no column for name " + fieldName)
	}
	switch v := vi.(type) {
	case int32, int, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case []uint8:
		return string(v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case string:
		return v
	case time.Time:
		return v.Format(StandardTimeLayout)
	default:
		panic(fmt.Sprintf("can not get type %T to string", vi))
	}
}
