package bdb

import (
	"bytes"
	"context"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Message[T any] interface {
	proto.Message
	*T
	GetCreatedAt() *timestamppb.Timestamp
}

func Get[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, key string) (M, error) {
	buf, err := GetKey(tx, path, key)
	if err != nil {
		return nil, err
	}

	return Unmarshal[T, M](buf)
}

func Set[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, key string, t M) (M, error) {
	buf, err := Marshal(t)
	if err != nil {
		return nil, err
	}

	if err := SetKey(tx, path, key, buf); err != nil {
		return nil, err
	}

	return t, nil
}

func Delete(ctx context.Context, tx *bolt.Tx, path Path, key string) error {
	return DeleteKey(tx, path, key)
}

func Marshal[T any, M Message[T]](t M) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := pb.ProtoToBuf(buf, any(t).(proto.Message)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Unmarshal[T any, M Message[T]](b []byte) (M, error) {
	var t T
	if err := pb.BufToProto(bytes.NewReader(b), any(&t).(proto.Message)); err != nil {
		return nil, err
	}
	return &t, nil
}

func List[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, page *dsc.PaginationRequest) ([]M, *dsc.PaginationResponse, error) {
	iter, err := list(tx, path, page.Token)
	if err != nil {
		return []M{}, &dsc.PaginationResponse{}, err
	}

	var results []M

	for iter.Next() {
		v := iter.Value()
		msg, err := Unmarshal[T, M](v)
		if err != nil {
			return []M{}, &dsc.PaginationResponse{}, err
		}

		results = append(results, msg)

		if len(results) == int(page.Size) {
			break
		}
	}

	pageResp := &dsc.PaginationResponse{
		ResultSize: int32(len(results)),
		NextToken:  "",
	}

	// get NextToken value.
	if iter.Next() {
		pageResp.NextToken = string(iter.Key())
	}

	return results, pageResp, nil
}

func Scan[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, filter string) ([]M, error) {
	keys, values, err := scan(tx, path, filter)
	if err != nil {
		return []M{}, err
	}

	var results []M

	for i := 0; i < len(keys); i++ {
		v := values[i]

		msg, err := Unmarshal[T, M](v)
		if err != nil {
			return []M{}, err
		}

		results = append(results, msg)
	}

	return results, nil
}
