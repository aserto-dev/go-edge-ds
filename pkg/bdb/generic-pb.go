package bdb

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

type Message[T any] interface {
	proto.Message
	*T
}

var (
	marshalOpts = proto.MarshalOptions{
		AllowPartial:  false,
		Deterministic: false,
		UseCachedSize: false,
	}
	unmarshalOpts = proto.UnmarshalOptions{
		Merge:          false,
		AllowPartial:   false,
		DiscardUnknown: true,
	}
)

func marshal[T any, M Message[T]](t M) ([]byte, error) {
	return marshalOpts.Marshal(t)
}

func unmarshal[T any, M Message[T]](b []byte) (M, error) {
	var t T

	if err := unmarshalOpts.Unmarshal(b, any(&t).(proto.Message)); err != nil {
		return nil, err
	}

	return &t, nil
}

func Get[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, key string) (M, error) {
	buf, err := GetKey(tx, path, key)
	if err != nil {
		return nil, err
	}

	return unmarshal[T, M](buf)
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

func Iter[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, keyFilter string, f func(M) error) error {
	b, err := SetBucket(tx, path)
	if err != nil {
		return errors.Wrapf(ErrPathNotFound, "path [%s]", path)
	}

	c := b.Cursor()

	for k, v := first(c, keyFilter); k != nil; k, v = next(c, keyFilter) {
		msg, err := unmarshal[T, M](v)
		if err != nil {
			return err
		}

		if err := f(msg); err != nil {
			return err
		}
	}

	return nil
}

func first(c *bolt.Cursor, keyFilter string) (key, value []byte) {
	if keyFilter == "" {
		return c.First()
	}
	return c.Seek([]byte(keyFilter))
}

func next(c *bolt.Cursor, keyFilter string) (key, value []byte) {
	k, v := c.Next()
	if k == nil {
		return nil, nil
	}
	if bytes.HasPrefix(k, []byte(keyFilter)) {
		return k, v
	}
	return nil, nil
}
