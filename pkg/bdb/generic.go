package bdb

import (
	"context"
	"encoding/json"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Message[T any] interface {
	proto.Message
	*T
}

var (
	marshalOpts = protojson.MarshalOptions{
		Multiline:       false,
		Indent:          "",
		AllowPartial:    false,
		UseProtoNames:   true,
		UseEnumNumbers:  false,
		EmitUnpopulated: false,
	}
	unmarshalOpts = protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}
)

func Get[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, key string) (M, error) {
	buf, err := GetKey(tx, path, key)
	if err != nil {
		return nil, err
	}

	return unmarshal[T, M](buf)
}

func List[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path) ([]M, error) {
	result := []M{}

	b, err := SetBucket(tx, path)
	if err != nil {
		return result, err
	}

	c := b.Cursor()
	for key, value := c.First(); key != nil; key, value = c.Next() {
		i, err := unmarshal[T, M](value)
		if err != nil {
			return []M{}, err
		}

		result = append(result, i)
	}

	return result, nil
}

func Set[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, key string, t M) (M, error) {
	buf, err := marshal(t)
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

func marshal[T any, M Message[T]](t M) ([]byte, error) {
	return marshalOpts.Marshal(any(t).(proto.Message))
}

func unmarshal[T any, M Message[T]](b []byte) (M, error) {
	var t T

	if err := unmarshalOpts.Unmarshal(b, any(&t).(proto.Message)); err != nil {
		return nil, err
	}

	return &t, nil
}

func GetAny[T any](ctx context.Context, tx *bolt.Tx, path Path, key string) (*T, error) {
	buf, err := GetKey(tx, path, key)
	if err != nil {
		return nil, err
	}

	return unmarshalAny[T](buf)
}

func SetAny[T any](ctx context.Context, tx *bolt.Tx, path Path, key string, t *T) (*T, error) {
	buf, err := marshalAny(t)
	if err != nil {
		return nil, err
	}

	if err := SetKey(tx, path, key, buf); err != nil {
		return nil, err
	}

	return t, nil
}

func unmarshalAny[T any](buf []byte) (*T, error) {
	var t T
	if err := json.Unmarshal(buf, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func marshalAny[T any](v T) ([]byte, error) {
	return json.Marshal(&v)
}
