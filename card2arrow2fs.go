package card2arrow2fs

import (
	"errors"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Serial types(integers; tiny, small, normal and big).
type Serial interface {
	int8 | int16 | int32 | int64
}

// Count types(integers; small, medium and large).
type Count interface {
	uint8 | uint16 | uint32
}

type SerialBuilder[T Serial] interface {
	Append(value T)
	AppendNull()
	NewArray() arrow.Array
	Release()
}

type SmallSerialBuilder struct{ *array.Int16Builder }

func (b SmallSerialBuilder) AsSerialBuilder() SerialBuilder[int16] { return b }

type TinySerialBuilder struct{ *array.Int8Builder }

func (b TinySerialBuilder) AsSerialBuilder() SerialBuilder[int8] { return b }

type NormalSerialBuilder struct{ *array.Int32Builder }

func (b NormalSerialBuilder) AsSerialBuilder() SerialBuilder[int32] { return b }

type BigSerialBuilder struct{ *array.Int64Builder }

func (b BigSerialBuilder) AsSerialBuilder() SerialBuilder[int64] { return b }

type CountBuilder[T Count] interface {
	Append(value T)
	AppendNull()
	NewArray() arrow.Array
	Release()
}

type SmallCountBuilder struct{ *array.Uint8Builder }

func (b SmallCountBuilder) AsCountBuilder() CountBuilder[uint8] { return b }

type MediumCountBuilder struct{ *array.Uint16Builder }

func (b MediumCountBuilder) AsCountBuilder() CountBuilder[uint16] { return b }

type LargeCountBuilder struct{ *array.Uint32Builder }

func (b LargeCountBuilder) AsCountBuilder() CountBuilder[uint32] { return b }

type PrimitiveCardMap[T Serial, U Count] map[T]U

type TinySerialCardMapL PrimitiveCardMap[int8, uint32]
type TinySerialCardMapM PrimitiveCardMap[int8, uint16]
type TinySerialCardMapS PrimitiveCardMap[int8, uint8]

type SmallSerialCardMapL PrimitiveCardMap[int16, uint32]
type SmallSerialCardMapM PrimitiveCardMap[int16, uint16]
type SmallSerialCardMapS PrimitiveCardMap[int16, uint8]

type NormalSerialCardMapL PrimitiveCardMap[int32, uint32]
type NormalSerialCardMapM PrimitiveCardMap[int32, uint16]
type NormalSerialCardMapS PrimitiveCardMap[int32, uint8]

type BigSerialCardMapL PrimitiveCardMap[int64, uint32]
type BigSerialCardMapM PrimitiveCardMap[int64, uint16]
type BigSerialCardMapS PrimitiveCardMap[int64, uint8]

func (p PrimitiveCardMap[T, U]) ToBatch(
	sch *arrow.Schema,
	newSerialBuilder func() SerialBuilder[T],
	newCountBuilder func() CountBuilder[U],
) arrow.RecordBatch {
	var sbld SerialBuilder[T] = newSerialBuilder()
	defer sbld.Release()

	var cbld CountBuilder[U] = newCountBuilder()
	defer cbld.Release()

	for serial, count := range p {
		sbld.Append(serial)
		cbld.Append(count)
	}

	var sarr arrow.Array = sbld.NewArray()
	defer sarr.Release()

	var carr arrow.Array = cbld.NewArray()
	defer carr.Release()

	return array.NewRecordBatch(
		sch,
		[]arrow.Array{
			sarr,
			carr,
		},
		int64(len(p)),
	)
}

type Allocator struct{ memory.Allocator }

var AllocatorDefault Allocator = Allocator{Allocator: memory.DefaultAllocator}

func (a Allocator) NewInt8Builder() *array.Int8Builder {
	return array.NewInt8Builder(a.Allocator)
}

func (a Allocator) NewInt16Builder() *array.Int16Builder {
	return array.NewInt16Builder(a.Allocator)
}

func (a Allocator) NewInt32Builder() *array.Int32Builder {
	return array.NewInt32Builder(a.Allocator)
}

func (a Allocator) NewInt64Builder() *array.Int64Builder {
	return array.NewInt64Builder(a.Allocator)
}

func (a Allocator) NewUint8Builder() *array.Uint8Builder {
	return array.NewUint8Builder(a.Allocator)
}

func (a Allocator) NewUint16Builder() *array.Uint16Builder {
	return array.NewUint16Builder(a.Allocator)
}

func (a Allocator) NewUint32Builder() *array.Uint32Builder {
	return array.NewUint32Builder(a.Allocator)
}

func (m NormalSerialCardMapL) ToBatch(
	sch *arrow.Schema,
	mem Allocator,
) arrow.RecordBatch {
	return PrimitiveCardMap[int32, uint32](m).ToBatch(
		sch,
		func() SerialBuilder[int32] {
			return NormalSerialBuilder{Int32Builder: mem.NewInt32Builder()}
		},
		func() CountBuilder[uint32] {
			return LargeCountBuilder{Uint32Builder: mem.NewUint32Builder()}
		},
	)
}

func (m NormalSerialCardMapL) ToBatchDefault() arrow.RecordBatch {
	return m.ToBatch(
		SchemaBuilderDefault.ToSchema(),
		Allocator{Allocator: memory.DefaultAllocator},
	)
}

type ColumnInfo struct {
	Name string
	Type arrow.DataType
}

type SchemaBuilder struct {
	Serial ColumnInfo
	Count  ColumnInfo
}

func (b SchemaBuilder) ToSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: b.Serial.Name, Type: b.Serial.Type, Nullable: false},
			{Name: b.Count.Name, Type: b.Count.Type, Nullable: false},
		},
		nil,
	)
}

var SerialTypeDefault arrow.DataType = arrow.PrimitiveTypes.Int32
var CountTypeDefault arrow.DataType = arrow.PrimitiveTypes.Uint32

var SchemaBuilderDefault SchemaBuilder = SchemaBuilder{
	Serial: ColumnInfo{Name: "serial", Type: SerialTypeDefault},
	Count:  ColumnInfo{Name: "count", Type: CountTypeDefault},
}

type Batch struct{ arrow.RecordBatch }

func (b Batch) ToWriter(w io.Writer, opts ...ipc.Option) error {
	fwtr, e := ipc.NewFileWriter(w, opts...)
	if nil != e {
		return e
	}
	return errors.Join(fwtr.Write(b.RecordBatch), fwtr.Close())
}

func (b Batch) ToStdout(opts ...ipc.Option) error {
	return b.ToWriter(os.Stdout, opts...)
}

func (b Batch) ToFile(filename string, opts ...ipc.Option) error {
	wtr, e := os.Create(filename) //nolint:gosec
	if nil != e {
		return e
	}
	return errors.Join(
		b.ToWriter(wtr, opts...),
		wtr.Sync(),
		wtr.Close(),
	)
}
