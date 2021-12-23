package zsql

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"time"

	"github.com/luoskak/zsql/pkg/schema"
)

func Scan(rows *sql.Rows, db *DB) {
	columns, _ := rows.Columns()
	values := make([]interface{}, len(columns))
	db.RowsAffected = 0

	initialized := false
	switch dest := db.Statement.Dest.(type) {
	case map[string]interface{}, *map[string]interface{}:
		if initialized || rows.Next() {
			columnTypes, _ := rows.ColumnTypes()
			prepareValue(values, db, columnTypes, columns)

			db.RowsAffected++
			db.AddError(rows.Scan(values...))

			mapValue, ok := dest.(map[string]interface{})
			if !ok {
				if v, ok := dest.(*map[string]interface{}); ok {
					mapValue = *v
				}
			}
			scanIntoMap(mapValue, values, columns)
		}
	case *[]map[string]interface{}:
		columnTypes, _ := rows.ColumnTypes()
		for initialized || rows.Next() {
			prepareValue(values, db, columnTypes, columns)

			initialized = false
			db.RowsAffected++
			db.AddError(rows.Scan(values...))

			mapValue := map[string]interface{}{}
			scanIntoMap(mapValue, values, columns)
			*dest = append(*dest, mapValue)
		}
	case *int, *int8, *int16, *int32, *int64,
		*uint, *uint8, *uint16, *uint32, *uint64, *uintptr,
		*float32, *float64,
		*bool, *string, *time.Time,
		*sql.NullInt32, *sql.NullInt64, *sql.NullFloat64,
		*sql.NullBool, *sql.NullString, *sql.NullTime:
		for initialized || rows.Next() {
			initialized = false
			db.RowsAffected++
			db.AddError(rows.Scan(dest))
		}
	default:
		sc := db.Statement.Schema
		reflectValue := db.Statement.ReflectValue
		if reflectValue.Kind() == reflect.Interface {
			reflectValue = reflectValue.Elem()
		}

		switch reflectValue.Kind() {
		case reflect.Slice, reflect.Array:
			var (
				reflectValueType = reflectValue.Type().Elem()
				isPtr            = reflectValueType.Kind() == reflect.Ptr
				fields           = make([]*schema.Field, len(columns))
			)

			if isPtr {
				reflectValueType = reflectValueType.Elem()
			}

			db.Statement.ReflectValue.Set(reflect.MakeSlice(reflectValue.Type(), 0, 20))

			if sc != nil {
				if reflectValueType != sc.ModelType && reflectValueType.Kind() == reflect.Struct {
					sc, _ = schema.Parse(db.Statement.Dest, db.opts.cacheStore, db.opts.namingStrategy)
				}
				for idx, column := range columns {
					if field := sc.LookUpField(column); field != nil {
						fields[idx] = field
					} else {
						values[idx] = &sql.RawBytes{}
					}
				}
			}

			isPluck := false
			if len(fields) == 1 {
				if _, ok := reflect.New(reflectValueType).Interface().(sql.Scanner); ok ||
					reflectValueType.Kind() != reflect.Struct ||
					sc.ModelType.ConvertibleTo(schema.TimeReflectType) {
					isPluck = true
				}
			}

			for initialized || rows.Next() {
				initialized = false
				db.RowsAffected++

				elem := reflect.New(reflectValueType)
				if isPluck {
					db.AddError(rows.Scan(elem.Interface()))
				} else {
					for idx, field := range fields {
						if field != nil {
							values[idx] = reflect.New(reflect.PtrTo(field.IndirectFieldType)).Interface()
						}
					}

					db.AddError(rows.Scan(values...))

					for idx, field := range fields {
						if field != nil {
							field.Set(elem, values[idx])
						}
					}
				}
				if isPtr {
					reflectValue = reflect.Append(reflectValue, elem)
				} else {
					reflectValue = reflect.Append(reflectValue, elem.Elem())
				}

			}

			db.Statement.ReflectValue.Set(reflectValue)
		case reflect.Struct, reflect.Ptr:
			if reflectValue.Type() != sc.ModelType {
				sc, _ = schema.Parse(db.Statement.Dest, db.opts.cacheStore, db.opts.namingStrategy)
			}

			if initialized || rows.Next() {
				for idx, column := range columns {
					if field := sc.LookUpField(column); field != nil {
						values[idx] = reflect.New(reflect.PtrTo(field.IndirectFieldType)).Interface()
					} else if len(columns) == 1 {
						values[idx] = dest
					} else {
						values[idx] = &sql.RawBytes{}
					}
				}

				db.RowsAffected++
				db.AddError(rows.Scan(values...))

				for idx, column := range columns {
					if field := sc.LookUpField(column); field != nil {
						field.Set(reflectValue, values[idx])
					}
				}
			}

		default:
			if rows.Next() {
				db.AddError(rows.Scan(dest))
			}

		}

	}

	if err := rows.Err(); err != nil && err != db.Error {
		db.AddError(err)
	}

}

func prepareValue(values []interface{}, db *DB, columnTypes []*sql.ColumnType, columns []string) {
	if db.Statement.Schema != nil {
		for idx, name := range columns {
			if field := db.Statement.Schema.LookUpField(name); field != nil {
				values[idx] = reflect.New(reflect.PtrTo(field.FieldType)).Interface()
				continue
			}
			values[idx] = new(interface{})
		}
	} else if len(columnTypes) > 0 {
		for idx, columnType := range columnTypes {
			if columnType.ScanType() != nil {
				values[idx] = reflect.New(reflect.PtrTo(columnType.ScanType())).Interface()
			} else {
				values[idx] = new(interface{})
			}
		}
	} else {
		for idx := range columns {
			values[idx] = new(interface{})
		}
	}
}

func scanIntoMap(mapValue map[string]interface{}, values []interface{}, columns []string) {
	for idx, column := range columns {
		if reflectValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(values[idx]))); reflectValue.IsValid() {
			mapValue[column] = reflectValue.Interface()
			if valuer, ok := mapValue[column].(driver.Valuer); ok {
				mapValue[column], _ = valuer.Value()
			} else if b, ok := mapValue[column].(sql.RawBytes); ok {
				mapValue[column] = string(b)
			}
		} else {
			mapValue[column] = nil
		}
	}
}
