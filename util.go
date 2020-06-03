package simplets

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"reflect"
	"strings"

	. "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

func addHashPrefix(str string) string {
	sum := md5.Sum([]byte(str))
	prefix := base64.StdEncoding.EncodeToString(sum[0:3])
	return fmt.Sprintf("%s...%s", prefix, str)
}

func trimHashPrefix(str string) string {
	return str[4+len("..."):]
}

func IsObjectNotExist(err error) bool {
	return strings.Contains(err.Error(), "OTSObjectNotExist")
}

func IsConditionCheckFail(err error) bool {
	return strings.Contains(err.Error(), "OTSConditionCheckFail")
}

func substantiateError(err error) error {
	if IsObjectNotExist(err) {
		return fmt.Errorf("err is %w: %v", ErrObjectNotExist, err)
	}
	if IsConditionCheckFail(err) {
		return fmt.Errorf("err is %w: %v", ErrConditionCheckFail, err)
	}
	return err
}

var typeOfBytes = reflect.TypeOf([]byte(nil))

type fieldInfo struct {
	structFieldInfo
	kind   reflect.Kind
	value  interface{}
	isZero bool
	values map[string]interface{} // for prefix field
}

func fillStructFromFields(t reflect.Type, v reflect.Value, fields map[string]*fieldInfo) {
	for i := 0; i < t.NumField(); i++ {
		value := v.Field(i)
		field := t.Field(i)
		si := getStructFieldInfo(field)
		fieldInfo, ok := fields[si.fieldName]
		if !ok {
			continue
		}
		if fieldInfo.isPrefixCol {
			if value.Kind() != reflect.Map { //todo failfast in Check function if kind is not map
				continue
			}
			valueType := value.Type().Elem()
			if value.IsNil() {
				value.Set(reflect.MakeMap(value.Type()))
			}
			for k, v := range fieldInfo.values {
				k1 := reflect.ValueOf(k)
				v1 := reflect.ValueOf(v)
				// we must ensure the field type in table is same as type defined in struct, otherwise, it will panic
				if valueType.Kind() == v1.Type().Kind() || valueType.Kind() == reflect.Interface {
					if v1.Interface() != nil {
						value.SetMapIndex(k1, v1)
					}
				}
			}
		} else {
			// we can't simply value.Set(reflect.ValueOf(structFieldInfo.value)) because if the field type in table is not match our type
			// defined in struct, it will panic
			if value.Kind() == reflect.ValueOf(fieldInfo.value).Kind() || value.Kind() == reflect.Interface {
				if fieldInfo.value != nil {
					value.Set(reflect.ValueOf(fieldInfo.value))
				}
			}
		}
	}
}

func getTableInfoFromStruct(r interface{}) (table string, pkFields, colFields []*fieldInfo) {
	v := reflect.ValueOf(r).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		value := v.Field(i)
		kind := value.Kind()
		si := getStructFieldInfo(t.Field(i))
		if si.tableName != "" {
			table = si.tableName
		}
		if si.fieldName == "" {
			continue
		}
		f := &fieldInfo{
			structFieldInfo: si,
			kind:            kind,
			value:           value.Interface(),
			isZero:          value.IsZero(),
		}
		if f.isPk {
			pkFields = append(pkFields, f)
		} else {
			colFields = append(colFields, f)
		}
	}
	if table == "" {
		panic("no ts_table tag found")
	}
	return
}

func generateInfo(v reflect.Value, t reflect.Type) (*PrimaryKey, map[string]*fieldInfo, string) {
	table := ""
	pk := new(PrimaryKey)
	fields := make(map[string]*fieldInfo)
	for i := 0; i < t.NumField(); i++ {
		value := v.Field(i)
		kind := value.Kind()
		si := getStructFieldInfo(t.Field(i))
		if si.tableName != "" {
			table = si.tableName
		}
		if si.fieldName == "" {
			// this field is not relate to ts, just ignore
			continue
		}
		//if !value.IsZero() && si.isAutoIncPk {
		//	panic("autoInc pk do not accept any value")
		//}
		//todo: make sure the struct field type is bool int64 string float []byte, otherwise fail fast
		v := value.Interface()
		f := &fieldInfo{
			structFieldInfo: si,
			kind:            kind,
			value:           v,
			isZero:          value.IsZero(),
		}
		if f.isPrefixCol {
			if kind != reflect.Map {
				panic("ts_col_prefix field must be a map")
			}
			values := make(map[string]interface{})
			iter := value.MapRange()
			for iter.Next() {
				k := iter.Key()
				v := iter.Value()
				values[k.String()] = v.Interface()
			}
			f.values = values
		}
		fields[si.fieldName] = f
		if !si.isPk {
			continue
		}
		if si.isHashPk {
			pk.AddPrimaryKeyColumn(si.fieldName, addHashPrefix(v.(string)))
		} else if si.isAutoIncPk && value.IsZero() {
			pk.AddPrimaryKeyColumnWithAutoIncrement(si.fieldName)
		} else {
			pk.AddPrimaryKeyColumn(si.fieldName, v)
		}
	}
	return pk, fields, table
}

type structFieldInfo struct {
	tableName      string
	fieldName      string
	isPk           bool
	isHashPk       bool
	isAutoIncPk    bool
	isPrefixCol    bool
	isAtomicIncCol bool
	columnPrefix   string
}

//Pk1  string           `ts_pk:"pk1,hash" ts_table:"test_auto_inc"`
//Pk2  int64            `ts_pk:"pk2,auto_inc"`
//ColAny int64            `ts_col:"col1,atomic"`
//ColsAtomic map[string]int64 `ts_col_prefix:"c_,atomic"`
func getStructFieldInfo(field reflect.StructField) structFieldInfo {
	pkStr := field.Tag.Get("ts_pk")
	colStr := field.Tag.Get("ts_col")
	colPrefixStr := field.Tag.Get("ts_col_prefix")
	info := structFieldInfo{}
	if pkStr != "" {
		splits := strings.Split(pkStr, ",")
		pkName := splits[0]
		isHashPk := strings.Contains(pkStr, "hash")
		isAutoIncPk := strings.Contains(pkStr, "auto_inc")
		tabName := field.Tag.Get("ts_table")
		info = structFieldInfo{
			tableName:   tabName,
			fieldName:   pkName,
			isPk:        true,
			isHashPk:    isHashPk,
			isAutoIncPk: isAutoIncPk,
		}
	} else if colStr != "" {
		splits := strings.Split(colStr, ",")
		colName := splits[0]
		isAtomicIncCol := strings.Contains(colStr, "atomic")
		info = structFieldInfo{
			fieldName:      colName,
			isAtomicIncCol: isAtomicIncCol,
		}
	} else if colPrefixStr != "" {
		splits := strings.Split(colPrefixStr, ",")
		colName := splits[0]
		isAtomicIncCol := strings.Contains(colPrefixStr, "atomic")
		info = structFieldInfo{
			fieldName:      colName,
			isAtomicIncCol: isAtomicIncCol,
			isPrefixCol:    true,
			columnPrefix:   colName,
		}

	} else {
		info = structFieldInfo{}
	}

	//todo: 1. hash must be first field, 2. cannot isHashPk and isAutoIncPk simultaneously
	//todo: ensure field type is int64/bytes/string/float64
	//todo 3. panic("table tag must only be defined in first field")

	if info.isAtomicIncCol {
		kind := field.Type.Kind()
		if kind != reflect.Int64 && kind != reflect.Map {
			panic("atomic increment column type must be int64")
		}
		if kind == reflect.Map {
			elemKind := field.Type.Elem().Kind()
			if elemKind != reflect.Int64 {
				panic("atomic increment column type must be int64")
			}
		}
	}

	return info
}

func getKnownPrefixFieldOfColumn(fields map[string]*fieldInfo, col string) (*fieldInfo, string) {
	for _, f := range fields {
		if !f.isPrefixCol {
			continue
		}
		if strings.HasPrefix(col, f.columnPrefix) {
			return f, strings.TrimPrefix(col, f.columnPrefix)
		}
	}
	return nil, ""
}

func fillColsToFieldInfos(columns []*AttributeColumn, fields map[string]*fieldInfo) {
	for _, column := range columns {
		if field, ok := fields[column.ColumnName]; ok {
			field.value = column.Value
			continue
		}
		field, realKey := getKnownPrefixFieldOfColumn(fields, column.ColumnName)
		if field == nil {
			continue
		}
		field.values[realKey] = column.Value
	}
}

func fillPKsToFieldInfos(primaryKey PrimaryKey, fields map[string]*fieldInfo) {
	for _, key := range primaryKey.PrimaryKeys {
		field := fields[key.ColumnName]
		if field.isHashPk {
			field.value = trimHashPrefix(key.Value.(string))
		} else {
			field.value = key.Value
		}
	}
}
