package simplets

import (
	"errors"
	"fmt"
	"testing"

	. "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/stretchr/testify/require"
)

const (
	endpoint = ""
	instance = ""
	akid     = ""
	aksr     = ""
)

var (
	cli = NewClient(endpoint, instance, akid, aksr)
)

type SimpleRecord struct {
	Pk1      string            `ts_pk:"p1,hash" ts_table:"test_simple_record"`
	Pk2      int64             `ts_pk:"p2"`
	ColStr   string            `ts_col:"col_str"`
	ColInt64 int64             `ts_col:"col_int64"`
	ColBytes []byte            `ts_col:"col_bytes"`
	ColsStr  map[string]string `ts_col_prefix:"c_"`
}

func TestSimpleExample(t *testing.T) {
	r := &SimpleRecord{
		Pk1:      "oss://a/b/c",
		Pk2:      1000,
		ColStr:   "abc",
		ColInt64: 2,
		ColsStr: map[string]string{
			"foo": "a", // this will create a column with key c_foo with value "a"
			"bar": "b", // this will create a column with key c_bar with value "b"
		},
	}
	// first, we ensure the table is exist, if not, ensure will create the table on the fly
	EnsureTable(cli, r)
	// PutRow to insert a record
	err := PutRow(cli, r)
	require.NoError(t, err)

	// we change some column value
	r.ColInt64 = 3
	r.ColsStr["foo"] = "aa" // change column c_foo value to "aa"
	err = UpdateRow(cli, r)
	require.NoError(t, err)

	// we examine it by get this row
	r = &SimpleRecord{
		Pk1: "oss://a/b/c",
		Pk2: 1000,
	}
	exist, err := GetRow(cli, r)
	require.NoError(t, err)
	require.Equal(t, true, exist)
	require.Equal(t, "abc", r.ColStr)
	require.Equal(t, int64(3), r.ColInt64)
	require.Equal(t, "aa", r.ColsStr["foo"])
	require.Equal(t, "b", r.ColsStr["bar"])

	// DeleteRow to delete record
	err = DeleteRow(cli, r)
	require.NoError(t, err)
	exist, err = GetRow(cli, r)
	require.NoError(t, err)
	require.Equal(t, false, exist)
}

type AutoIncrementRecord struct {
	Pk1  string `ts_pk:"p1" ts_table:"test_auto_inc"`
	Pk2  int64  `ts_pk:"p2,auto_inc"` // tablestore primary key auto_increment function
	Col1 string `ts_col:"col1"`
}

func TestAutoIncrementExample(t *testing.T) {
	r := &AutoIncrementRecord{
		Pk1:  "oss://a/b",
		Col1: "a",
	}
	EnsureTable(cli, r)

	// we can get the auto_increment pk2 result after PutRow
	err := PutRow(cli, r)
	require.NoError(t, err)
	// auto_increment is always a very big number
	require.Less(t, int64(1000000), r.Pk2)
	t.Logf("pk2 is %d", r.Pk2)
	autoPk1 := r.Pk2

	// insert a record again, caution: if the Pk2 field is not a zero value, it will put exactly the value to the row
	r.Pk2 = 0
	err = PutRow(cli, r)
	require.NoError(t, err)
	t.Logf("pk2 is %d", r.Pk2)
	autoPk2 := r.Pk2
	require.Less(t, autoPk1, autoPk2)
}

type ColumnAnyValueRecord struct {
	Pk1     string                 `ts_pk:"pk1" ts_table:"test_column_any_value"`
	ColAny  interface{}            `ts_col:"col1"`
	ColsAny map[string]interface{} `ts_col_prefix:"c_"`
}

type ColumnStringRecord struct {
	Pk1    string `ts_pk:"pk1" ts_table:"test_column_any_value"`
	ColStr string `ts_col:"col1"`
}

func TestColumnAnyValue(t *testing.T) {
	var err error
	r1 := &ColumnAnyValueRecord{Pk1: "a1", ColAny: 100}   // ColAny field can accept any Int value, it will convert to int64
	r2 := &ColumnAnyValueRecord{Pk1: "a2", ColAny: "abc"} // ColAny can also be String

	EnsureTable(cli, &ColumnAnyValueRecord{})
	err = PutRow(cli, r1)
	require.NoError(t, err)
	err = PutRow(cli, r2)
	require.NoError(t, err)
	r := &ColumnAnyValueRecord{Pk1: "a1"}
	_, err = GetRow(cli, r)
	require.NoError(t, err)
	require.EqualValues(t, 100, r.ColAny)

	r3 := &ColumnAnyValueRecord{Pk1: "a3", ColsAny: map[string]interface{}{"foo": 100, "bar": "abc"}}
	err = PutRow(cli, r3)
	require.NoError(t, err)
	r3 = &ColumnAnyValueRecord{Pk1: "a3"}
	_, err = GetRow(cli, r3)
	require.NoError(t, err)
	require.EqualValues(t, 100, r3.ColsAny["foo"])
	require.EqualValues(t, "abc", r3.ColsAny["bar"])
	r3 = &ColumnAnyValueRecord{Pk1: "a3", ColsAny: map[string]interface{}{"foo": 200, "bar": "bcd"}}
	err = UpdateRow(cli, r3)
	require.NoError(t, err)
	_, err = GetRow(cli, r3)
	require.NoError(t, err)
	require.EqualValues(t, 200, r3.ColsAny["foo"])
	require.EqualValues(t, "bcd", r3.ColsAny["bar"])

	// if query by specific type, it will ignore any column that has incompatible type
	r4 := &ColumnStringRecord{Pk1: "a1"}
	_, err = GetRow(cli, r4)
	require.EqualValues(t, "", r4.ColStr)
}

type AtomicIncRecord struct {
	Pk1        string           `ts_pk:"pk1" ts_table:"test_atomic_inc"`
	ColAtomic  int64            `ts_col:"col1,atomic"`
	ColsAtomic map[string]int64 `ts_col_prefix:"c_,atomic"`
}

func TestAutoIncPkStruct(t *testing.T) {
	r := &AtomicIncRecord{Pk1: "a1"}
	EnsureTable(cli, &AtomicIncRecord{})
	err := PutRow(cli, r)
	require.NoError(t, err)
	r.ColAtomic = 100
	err = UpdateRow(cli, r)
	require.NoError(t, err)
	require.EqualValues(t, 100, r.ColAtomic)
	r.ColAtomic = 100
	err = UpdateRow(cli, r)
	require.NoError(t, err)
	require.EqualValues(t, 200, r.ColAtomic)
	r = &AtomicIncRecord{Pk1: r.Pk1, ColsAtomic: map[string]int64{"a": 100}}
	err = UpdateRow(cli, r)
	require.NoError(t, err)
	require.EqualValues(t, 100, r.ColsAtomic["a"])
	r = &AtomicIncRecord{Pk1: r.Pk1, ColsAtomic: map[string]int64{"a": 100}}
	err = UpdateRow(cli, r)
	require.NoError(t, err)
	require.EqualValues(t, 200, r.ColsAtomic["a"])
}

type RangeRecord struct {
	Pk      int64  `ts_pk:"pk" ts_table:"test_range_table"`
	Seq     int64  `ts_pk:"seq,auto_inc"`
	Content string `ts_col:"content"`
}

func TestScan(t *testing.T) {
	_, _ = cli.DeleteTable(&DeleteTableRequest{TableName: "test_range_table"})
	EnsureTable(cli, &RangeRecord{})
	// we only want 5, it will get ErrRangeEnd, because it has no rows
	rows := Range(cli, RangeRecord{}, []interface{}{MIN, MIN}, []interface{}{MAX, MAX}, FORWARD, 5)
	require.True(t, errors.Is(rows.Scan(&RangeRecord{}), ErrRangeEnd))

	for i := 0; i < 123; i++ {
		err := PutRow(cli, &RangeRecord{Pk: 1, Content: fmt.Sprintf("%d", i+1)})
		require.NoError(t, err)
	}

	// we only want 5, it will get 5
	count := 0
	rows = Range(cli, RangeRecord{}, []interface{}{1, MIN}, []interface{}{1, MAX}, FORWARD, 5)
	for {
		r := &RangeRecord{}
		err := rows.Scan(r)
		if err == ErrRangeEnd {
			t.Log("end range")
			break
		}
		require.NoError(t, err)
		count++
	}
	require.EqualValues(t, 5, count)

	// we want 100, it will fetch 2 times
	count = 0
	rows = Range(cli, RangeRecord{}, []interface{}{1, MIN}, []interface{}{1, MAX}, FORWARD, 100)
	for {
		r := &RangeRecord{}
		err := rows.Scan(r)
		if err == ErrRangeEnd {
			t.Log("end range")
			break
		}
		require.NoError(t, err)
		count++
	}
	require.EqualValues(t, 100, count)

	// we want all, it will fetch 3 times to exhaust the result
	count = 0
	rows = Range(cli, RangeRecord{}, []interface{}{1, MIN}, []interface{}{1, MAX}, FORWARD, -1)
	for {
		r := &RangeRecord{}
		err := rows.Scan(r)
		if err == ErrRangeEnd {
			t.Log("end range")
			break
		}
		require.NoError(t, err)
		count++
		//t.Log(r)
		//t.Logf("count: %d", count)
	}
	require.EqualValues(t, 123, count)

	// we want 300, it will exhaust the result to get all 123
	count = 0
	rows = Range(cli, RangeRecord{}, []interface{}{1, MIN}, []interface{}{1, MAX}, FORWARD, 300)
	for {
		r := &RangeRecord{}
		err := rows.Scan(r)
		if err == ErrRangeEnd {
			t.Log("end range")
			break
		}
		require.NoError(t, err)
		count++
		//t.Log(r)
		//t.Logf("count: %d", count)
	}
	require.EqualValues(t, 123, count)
}

type ConditionRecord struct {
	Pk    string `ts_pk:"pk" ts_table:"test_condition_table"`
	Value int64  `ts_col:"value"`
}

func TestConditionUpdate(t *testing.T) {
	EnsureTable(cli, &ConditionRecord{})
	err := UpdateRow(cli, &ConditionRecord{
		Pk:    "abc",
		Value: 100,
	})
	require.NoError(t, err)
	cond := NewSingleColumnCondition("value", CT_GREATER_THAN, int64(100))
	err = UpdateRow(cli, &ConditionRecord{
		Pk:    "abc",
		Value: 99,
	}, ColumnFilterOption(cond))
	require.True(t, errors.Is(err, ErrConditionCheckFail))
	err = UpdateRow(cli, &ConditionRecord{
		Pk:    "abc",
		Value: 101,
	}, ColumnFilterOption(cond))
	require.NoError(t, err)
}
