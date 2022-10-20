package js

import (
	"os"

	"github.com/aserto-dev/go-directory/pkg/pb"
	"google.golang.org/protobuf/proto"
)

type ArrayWriter struct {
	w     *os.File
	first bool
}

func NewArrayWriter(path string) (*ArrayWriter, error) {
	w, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	f := ArrayWriter{
		w:     w,
		first: false,
	}

	_, _ = f.w.WriteString("[\n")

	return &f, nil
}

func (f *ArrayWriter) Close() error {
	if f.w != nil {
		_, _ = f.w.WriteString("]\n")
		f.first = false
		err := f.w.Close()
		f.w = nil
		return err
	}
	return nil
}

func (f *ArrayWriter) Write(msg proto.Message) error {
	if f.first {
		_, _ = f.w.WriteString(",")
	}

	err := pb.ProtoToBuf(f.w, msg)

	if !f.first {
		f.first = true
	}

	return err
}
