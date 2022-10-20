package js

import (
	"encoding/json"
	"io"

	"github.com/aserto-dev/go-directory/pkg/pb"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type ArrayReader struct {
	dec   *json.Decoder
	first bool
}

func NewArrayReader(r io.Reader) (*ArrayReader, error) {

	dec := json.NewDecoder(r)

	// advance reader to array start token
	tok, _ := dec.Token()
	if delim, ok := tok.(json.Delim); !ok && delim.String() != "[" {
		return nil, errors.Errorf("file does not contain a JSON array")
	}

	return &ArrayReader{
		dec:   dec,
		first: false,
	}, nil
}

func (r *ArrayReader) Close() error {
	return nil
}

func (r *ArrayReader) Read(m proto.Message) error {
	if !r.dec.More() {
		return io.EOF
	}

	if err := pb.UnmarshalNext(r.dec, m); err != nil {
		return err
	}
	return nil
}
