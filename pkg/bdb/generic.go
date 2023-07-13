package bdb

import (
	"bytes"
	"context"

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
