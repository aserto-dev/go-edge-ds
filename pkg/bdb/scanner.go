package bdb

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

type Iterator[T any, M Message[T]] interface {
	Next() bool
	Value() M
}

type ScanIterator[T any, M Message[T]] struct {
	ctx   context.Context
	tx    *bolt.Tx
	c     *bolt.Cursor
	args  *ScanArgs
	init  bool
	key   []byte
	value []byte
}

type ScanOption func(*ScanArgs)

type ScanArgs struct {
	startToken []byte
	keyFilter  []byte
}

func WithStart(token []byte) ScanOption {
	return func(a *ScanArgs) {
		a.startToken = token
	}
}

func WithFilter(filter []byte) ScanOption {
	return func(a *ScanArgs) {
		a.keyFilter = filter
	}
}

func NewScanIterator[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, opts ...ScanOption) (Iterator[T, M], error) {
	args := &ScanArgs{}
	for _, opt := range opts {
		opt(args)
	}

	if args.startToken == nil && args.keyFilter != nil {
		args.startToken = args.keyFilter
	}

	b, err := SetBucket(tx, path)
	if err != nil {
		return nil, errors.Wrapf(ErrPathNotFound, "path [%s]", path)
	}

	return &ScanIterator[T, M]{ctx: ctx, tx: tx, c: b.Cursor(), args: args, init: false}, nil
}

func (s *ScanIterator[T, M]) Next() bool {
	if !s.init {
		if s.args.startToken == nil {
			s.key, s.value = s.c.First()
		} else {
			s.key, s.value = s.c.Seek(s.args.startToken)
		}
		s.init = true
	}

	if s.init {
		s.key, s.value = s.c.Next()
	}

	return s.key != nil && bytes.HasPrefix(s.key, s.args.keyFilter)
}

func (s *ScanIterator[T, M]) Value() M {
	msg, err := Unmarshal[T, M](s.value)
	if err != nil {
		var result M
		return result
	}
	return msg
}

func (s *ScanIterator[T, M]) K() []byte {
	return s.key
}

func (s *ScanIterator[T, M]) V() []byte {
	return s.value
}
