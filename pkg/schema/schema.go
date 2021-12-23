package schema

import (
	"errors"
	"fmt"
	"go/ast"
	"reflect"
	"sync"

	"github.com/luoskak/logger"
)

// ErrUnsupportedDataType unsupported data type
var ErrUnsupportedDataType = errors.New("unsupported data type")

type Schema struct {
	Name           string
	Table          string
	ModelType      reflect.Type
	FieldsByName   map[string]*Field
	FieldsByDBName map[string]*Field
	Fields         []*Field
	DBNames        []string
	err            error
	initialized    chan struct{}
	cacheStore     *sync.Map
}

func Parse(dest interface{}, cacheStore *sync.Map, namer Namer) (*Schema, error) {
	if dest == nil {
		return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
	}

	modelType := reflect.Indirect(reflect.ValueOf(dest)).Type()
	if modelType.Kind() == reflect.Interface {
		modelType = reflect.Indirect(reflect.ValueOf(dest)).Elem().Type()
	}

	for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	if modelType.Kind() != reflect.Struct {
		if modelType.PkgPath() == "" {
			return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
		}
		return nil, fmt.Errorf("%w: %s.%s", ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name())
	}

	if v, ok := cacheStore.Load(modelType); ok {
		s := v.(*Schema)
		<-s.initialized
		return s, s.err
	}

	// modelValue := reflect.New(modelType)
	tableName := modelType.Name()

	schema := &Schema{
		Name:           modelType.Name(),
		Table:          tableName,
		ModelType:      modelType,
		FieldsByName:   map[string]*Field{},
		FieldsByDBName: map[string]*Field{},
		cacheStore:     cacheStore,
		initialized:    make(chan struct{}),
	}

	defer close(schema.initialized)

	if v, loaded := cacheStore.Load(modelType); loaded {
		s := v.(*Schema)
		<-s.initialized
		return s, s.err
	}

	for i := 0; i < modelType.NumField(); i++ {
		if fieldStruct := modelType.Field(i); ast.IsExported(fieldStruct.Name) {
			field := schema.ParseField(fieldStruct)
			schema.Fields = append(schema.Fields, field)
		}
	}

	for _, field := range schema.Fields {
		if field.DBName == "" && field.DataType != "" {
			field.DBName = namer.ColumnName(schema.Name, field.Name)
		}

		if field.DBName != "" {
			if _, ok := schema.FieldsByDBName[field.DBName]; !ok {
				schema.DBNames = append(schema.DBNames, field.DBName)
				schema.FieldsByDBName[field.DBName] = field
				schema.FieldsByName[field.Name] = field
			}
		}

		if of, ok := schema.FieldsByName[field.Name]; !ok || of.TagSettings["-"] == "-" {
			schema.FieldsByName[field.Name] = field
		}

		field.setupValuerAndSetter()

	}

	if v, loaded := cacheStore.LoadOrStore(modelType, schema); loaded {
		s := v.(*Schema)
		<-s.initialized
		return s, s.err
	}

	defer func() {
		if schema.err != nil {
			logger.Error(schema.err.Error())
			cacheStore.Delete(modelType)
		}
	}()

	return schema, schema.err

}

func (schema Schema) LookUpField(name string) *Field {
	if field, ok := schema.FieldsByDBName[name]; ok {
		return field
	}
	if field, ok := schema.FieldsByName[name]; ok {
		return field
	}
	return nil
}
