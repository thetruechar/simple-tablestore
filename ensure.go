package simplets

import (
	"fmt"
	"reflect"

	. "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

func EnsureTable(client *TableStoreClient, r interface{}, opts ...EnsureTableOption) {
	var opt EnsureTableOption
	if len(opts) == 1 {
		opt = opts[0]
	}
	table, pkFields, _ := getTableInfoFromStruct(r)
	resp, err := client.DescribeTable(&DescribeTableRequest{TableName: table})
	if err != nil {
		tableNotExistErr := IsObjectNotExist(err)
		if !tableNotExistErr {
			panic(err)
		}
		if tableNotExistErr && opt.PanicIfTableNotExist {
			panic("table is not exist")
		}
		// create the table on demanded
		meta := &TableMeta{
			TableName: table,
		}
		for _, field := range pkFields {
			var pkType PrimaryKeyType
			switch field.kind {
			case reflect.Int64:
				pkType = PrimaryKeyType_INTEGER
			case reflect.String:
				pkType = PrimaryKeyType_STRING
			case reflect.Slice:
				if reflect.ValueOf(field.value).Type() == typeOfBytes {
					pkType = PrimaryKeyType_BINARY
				} else {
					panic("primary key type can only be string, int64, binary")
				}
			default:
				panic("primary key type can only be string, int64, binary")
			}
			if field.isAutoIncPk {
				meta.AddPrimaryKeyColumnOption(field.fieldName, pkType, AUTO_INCREMENT)
			} else {
				meta.AddPrimaryKeyColumn(field.fieldName, pkType)
			}
		}
		// use the most simple option if user not provide
		option := &TableOption{
			TimeToAlive: -1,
			MaxVersion:  1,
		}
		if opt.TableOption != nil {
			option = opt.TableOption
		}
		throughput := new(ReservedThroughput)
		if opt.ReservedThroughput != nil {
			throughput = opt.ReservedThroughput
		}
		req := &CreateTableRequest{
			TableMeta:          meta,
			TableOption:        option,
			ReservedThroughput: throughput,
			StreamSpec:         opt.StreamSpec,
			IndexMetas:         opt.IndexMetas,
		}
		_, err = client.CreateTable(req)
		if err != nil {
			panic(err)
		}
		return
	}
	mustMatchPrimaryKeys(pkFields, resp.TableMeta.SchemaEntry)
	//todo: check DefinedColumns
}

func mustMatchPrimaryKeys(fields []*fieldInfo, pkSchemas []*PrimaryKeySchema) {
	if len(fields) != len(pkSchemas) {
		panic("primary key count is not match")
	}
	for i, schema := range pkSchemas {
		f := fields[i]
		if *schema.Name != f.fieldName {
			panic(fmt.Sprintf("primary key name is not match, %s != %s", *schema.Name, f.fieldName))
		}
		if schema.Option == nil {
			continue
		}
		if *schema.Option == AUTO_INCREMENT && !f.isAutoIncPk {
			panic(fmt.Sprintf("primary key is AUTO_INCREMENT, %s, but tag is not define by ts_pk_+", f.fieldName))
		}
	}
}
