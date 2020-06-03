package simplets

import (
	"errors"
	"reflect"

	. "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

//var ErrNoRows = errors.New("simple-tablestore: no rows in result set")
var ErrRangeEnd = errors.New("simple-tablestore: range query end")

func constructRangeRequest(r interface{}, froms, tos []interface{}, direction Direction, limit int32) *GetRangeRequest {
	v := reflect.ValueOf(r)
	t := v.Type()
	pk, _, table := generateInfo(v, t)
	getRangeRequest := &GetRangeRequest{}
	rangeRowQueryCriteria := &RangeRowQueryCriteria{}
	rangeRowQueryCriteria.TableName = table

	startPK := new(PrimaryKey)
	endPK := new(PrimaryKey)
	for i, p := range pk.PrimaryKeys {
		from := froms[i]
		to := tos[i]
		pkName := p.ColumnName
		if from == MIN {
			startPK.AddPrimaryKeyColumnWithMinValue(pkName)
		} else if from == MAX {
			startPK.AddPrimaryKeyColumnWithMaxValue(pkName)
		} else {
			startPK.AddPrimaryKeyColumn(pkName, getSupportedValue(from))
		}
		if to == MIN {
			endPK.AddPrimaryKeyColumnWithMinValue(pkName)
		} else if to == MAX {
			endPK.AddPrimaryKeyColumnWithMaxValue(pkName)
		} else {
			endPK.AddPrimaryKeyColumn(pkName, getSupportedValue(to))
		}
	}
	rangeRowQueryCriteria.StartPrimaryKey = startPK
	rangeRowQueryCriteria.EndPrimaryKey = endPK
	rangeRowQueryCriteria.Direction = direction
	rangeRowQueryCriteria.MaxVersion = 1
	rangeRowQueryCriteria.Limit = limit
	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria
	return getRangeRequest
}

type Rows struct {
	client              *TableStoreClient
	req                 *GetRangeRequest
	nextStartPrimaryKey *PrimaryKey
	noNextBatch         bool
	count               int32
	total               int32
	infinite            bool
	end                 bool

	cursor int
	rows   []*Row
}

func (i *Rows) isEnd() bool {
	if i.end {
		return true
	}
	if i.cursor == len(i.rows) && i.noNextBatch {
		return true
	}
	if i.infinite {
		return false
	}

	return int32(i.cursor) == i.total
}

func (i *Rows) Scan(r interface{}) error {
	if i.isEnd() {
		return ErrRangeEnd
	}
	if i.cursor == len(i.rows) {
		req := i.req
		if i.nextStartPrimaryKey != nil {
			req.RangeRowQueryCriteria.StartPrimaryKey = i.nextStartPrimaryKey
		}
		getRangeResp, err := i.client.GetRange(i.req)
		if err != nil {
			return err
		}
		if len(getRangeResp.Rows) == 0 {
			return ErrRangeEnd
		}
		i.cursor = 0
		i.rows = getRangeResp.Rows
		i.nextStartPrimaryKey = getRangeResp.NextStartPrimaryKey
		if i.nextStartPrimaryKey == nil {
			i.noNextBatch = true
		}
	}
	v := reflect.ValueOf(r).Elem()
	t := v.Type()
	_, fields, _ := generateInfo(v, t)
	row := i.rows[i.cursor]
	fillPKsToFieldInfos(*row.PrimaryKey, fields)
	fillColsToFieldInfos(row.Columns, fields)
	fillStructFromFields(t, v, fields)
	i.cursor++
	i.count++
	if !i.infinite && i.count == i.total {
		i.end = true
	}
	return nil
}

func Range(client *TableStoreClient, r interface{}, froms, tos []interface{}, direction Direction, total int32) *Rows {
	return &Rows{
		client:   client,
		req:      constructRangeRequest(r, froms, tos, direction, getSuitableLimit(total)),
		total:    total,
		infinite: total <= 0,
	}
}

func getSuitableLimit(total int32) int32 {
	if total <= 0 || total > 50 {
		return 50
	}
	return total
}
