//go:build go1.18
// +build go1.18

package bytestreamsplit_test

import (
	"testing"

	"github.com/dchoi-viant/parquet-go/encoding/bytestreamsplit"
	"github.com/dchoi-viant/parquet-go/encoding/fuzz"
	"github.com/dchoi-viant/parquet-go/encoding/test"
)

func FuzzEncodeFloat(f *testing.F) {
	fuzz.EncodeDouble(f, new(bytestreamsplit.Encoding))
}

func FuzzEncodeDouble(f *testing.F) {
	fuzz.EncodeDouble(f, new(bytestreamsplit.Encoding))
}

func TestEncodeFloat(t *testing.T) {
	test.EncodeFloat(t, new(bytestreamsplit.Encoding), 0, 100)
}

func TestEncodeDouble(t *testing.T) {
	test.EncodeDouble(t, new(bytestreamsplit.Encoding), 0, 100)
}
