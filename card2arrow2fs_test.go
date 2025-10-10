package card2arrow2fs

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/stretchr/testify/assert"
)

func TestToBatchDefault(t *testing.T) {
	var counts NormalSerialCardMapL = NormalSerialCardMapL{
		3776: 42,
		599:  634,
	}

	var batch Batch = Batch{RecordBatch: counts.ToBatchDefault()}
	defer batch.Release()

	var buf bytes.Buffer
	err := batch.ToWriter(&buf, ipc.WithSchema(batch.Schema()))
	assert.NoError(t, err)

	rdr, err := ipc.NewFileReader(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	defer rdr.Close()

	rec, err := rdr.Read()
	assert.NoError(t, err)
	defer rec.Release()

	sCol := rec.Column(0)
	cCol := rec.Column(1)

	reconstructed := make(NormalSerialCardMapL)
	for i := 0; i < int(rec.NumRows()); i++ {
		sVal := sCol.(*array.Int32).Value(i)
		cVal := cCol.(*array.Uint32).Value(i)
		reconstructed[sVal] = cVal
	}

	assert.Equal(t, counts, reconstructed)
}
