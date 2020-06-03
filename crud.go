package simplets

import (
	"fmt"
	"reflect"

	. "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

var ErrConditionCheckFail = fmt.Errorf("OTSConditionCheckFail")
var ErrObjectNotExist = fmt.Errorf("OTSObjectNotExist")

func GetRow(client *TableStoreClient, r interface{}) (bool, error) {
	getRowRequest := new(GetRowRequest)
	criteria := new(SingleRowQueryCriteria)

	v := reflect.ValueOf(r).Elem()
	t := v.Type()
	pk, fields, table := generateInfo(v, t)

	criteria.PrimaryKey = pk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = table
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1
	getResp, err := client.GetRow(getRowRequest)
	if err != nil {
		return false, err
	}
	if getResp.PrimaryKey.PrimaryKeys == nil {
		return false, nil
	}

	columns := getResp.Columns
	fillColsToFieldInfos(columns, fields)
	fillStructFromFields(t, v, fields)
	return true, nil
}

func getSupportedValue(i interface{}) interface{} {
	value := reflect.ValueOf(i)
	kind := value.Kind()
	var v interface{}
	switch kind {
	case reflect.Bool:
		v = value.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v = value.Int()
	case reflect.String:
		v = value.String()
	case reflect.Float32, reflect.Float64:
		v = value.Float()
	case reflect.Slice:
		if value.Type() == typeOfBytes {
			v = value.Bytes()
		}
	default:
	}
	return v
}

func PutRow(client *TableStoreClient, r interface{}, setters ...Option) error {
	opts := &Options{}
	for _, s := range setters {
		s(opts)
	}
	v := reflect.ValueOf(r).Elem()
	t := v.Type()

	rowRequest := new(PutRowRequest)
	rowChange := new(PutRowChange)
	pk, fields, table := generateInfo(v, t)
	rowChange.TableName = table
	rowChange.PrimaryKey = pk
	rowChange.ReturnType = ReturnType_RT_PK

	for col, field := range fields {
		if field.isPk || (field.isZero && !opts.storeZeroValue) {
			continue
		}
		if field.isPrefixCol {
			for col, value := range field.values {
				rowChange.AddColumn(field.columnPrefix+col, getSupportedValue(value))
			}
		} else {
			rowChange.AddColumn(col, getSupportedValue(field.value))
		}
	}
	rowChange.SetCondition(opts.rowExistence)
	if opts.columnFilter != nil {
		rowChange.SetColumnCondition(opts.columnFilter)
	}
	rowRequest.PutRowChange = rowChange
	resp, err := client.PutRow(rowRequest)
	if err != nil {
		return err
	}
	fillPKsToFieldInfos(resp.PrimaryKey, fields)
	fillStructFromFields(t, v, fields)
	return err
}

func UpdateRow(client *TableStoreClient, r interface{}, setters ...Option) error {
	opts := &Options{}
	for _, s := range setters {
		s(opts)
	}
	v := reflect.ValueOf(r).Elem()
	t := v.Type()

	rowRequest := new(UpdateRowRequest)
	rowChange := new(UpdateRowChange)
	pk, fields, table := generateInfo(v, t)
	rowChange.TableName = table
	rowChange.PrimaryKey = pk
	var incColumnsToReturn []string
	for col, field := range fields {
		if field.isPk || (field.isZero && !opts.storeZeroValue) {
			continue
		}
		if field.isPrefixCol {
			for col, value := range field.values {
				if field.isAtomicIncCol {
					rowChange.IncrementColumn(field.columnPrefix+col, getSupportedValue(value).(int64))
					incColumnsToReturn = append(incColumnsToReturn, field.columnPrefix+col)
				} else {
					rowChange.PutColumn(field.columnPrefix+col, getSupportedValue(value))
				}
			}
		} else {
			if field.isAtomicIncCol {
				rowChange.IncrementColumn(col, field.value.(int64))
				incColumnsToReturn = append(incColumnsToReturn, col)
			} else {
				rowChange.PutColumn(col, getSupportedValue(field.value))
			}
		}
	}
	if len(incColumnsToReturn) > 0 {
		rowChange.ReturnType = ReturnType_RT_AFTER_MODIFY
		rowChange.ColumnNamesToReturn = incColumnsToReturn
	}
	rowChange.SetCondition(opts.rowExistence)
	if opts.columnFilter != nil {
		rowChange.SetColumnCondition(opts.columnFilter)
	}
	rowRequest.UpdateRowChange = rowChange
	resp, err := client.UpdateRow(rowRequest)
	if err != nil {
		return substantiateError(err)
	}
	columns := resp.Columns
	fillColsToFieldInfos(columns, fields)
	fillStructFromFields(t, v, fields)
	return nil
}

func DeleteRow(client *TableStoreClient, r interface{}, setters ...Option) error {
	opts := &Options{}
	for _, s := range setters {
		s(opts)
	}
	v := reflect.ValueOf(r).Elem()
	t := v.Type()
	pk, _, table := generateInfo(v, t)

	rowRequest := new(DeleteRowRequest)
	rowChange := new(DeleteRowChange)
	rowChange.TableName = table
	rowChange.PrimaryKey = pk
	rowChange.SetCondition(opts.rowExistence)
	if opts.columnFilter != nil {
		rowChange.SetColumnCondition(opts.columnFilter)
	}
	rowRequest.DeleteRowChange = rowChange
	_, err := client.DeleteRow(rowRequest)
	return substantiateError(err)
}
