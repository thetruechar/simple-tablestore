package simplets

import . "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"

type Options struct {
	storeZeroValue bool
	columnFilter   ColumnFilter
	rowExistence   RowExistenceExpectation
}

type EnsureTableOption struct {
	// panic if table is not exist when call EnsureTable, if this option is not provided, EnsureTable will create table automatically
	PanicIfTableNotExist bool

	// option from tablestore sdk
	TableOption        *TableOption
	ReservedThroughput *ReservedThroughput
	StreamSpec         *StreamSpecification
	IndexMetas         []*IndexMeta
}

type Option func(*Options)

func StoreZeroValue() Option {
	return func(options *Options) {
		options.storeZeroValue = true
	}
}

func ColumnFilterOption(f ColumnFilter) Option {
	return func(options *Options) {
		options.columnFilter = f
	}
}

func RowExistenceOption(r RowExistenceExpectation) Option {
	return func(options *Options) {
		options.rowExistence = r
	}
}
