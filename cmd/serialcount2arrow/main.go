package main

import (
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	ca "github.com/takanoriyanagitani/go-card2arrow2fs"
)

var counts ca.NormalSerialCardMapL = ca.NormalSerialCardMapL{
	3776: 42,
	599:  634,
}

var schema *arrow.Schema = ca.SchemaBuilderDefault.ToSchema()

var converted arrow.RecordBatch = counts.
	ToBatch(
		schema,
		ca.AllocatorDefault,
	)

func main() {
	defer converted.Release()
	e := ca.Batch{RecordBatch: converted}.ToStdout(
		ipc.WithSchema(schema),
	)
	if nil != e {
		log.Fatal(e) //nolint:gocritic
	}
}
