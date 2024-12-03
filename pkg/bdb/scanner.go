package bdb

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	bolt "go.etcd.io/bbolt"
)

type Iterator[T any, M Message[T]] interface {
	Next() bool       // move cursor to next element.
	RawKey() []byte   // return raw key value ([]byte).
	RawValue() []byte // return ram value value ([]byte).
	Key() string      // return key value (string).
	Value() M         // return typed value (M).
	Delete() error    // delete element underneath cursor.
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
	pageSize   int32
}

func WithPageSize(size int32) ScanOption {
	return func(a *ScanArgs) {
		a.pageSize = size
	}
}

func WithPageToken(token string) ScanOption {
	return func(a *ScanArgs) {
		a.startToken = []byte(token)
	}
}

func WithKeyFilter(filter string) ScanOption {
	return func(a *ScanArgs) {
		a.keyFilter = []byte(filter)
	}
}

func NewScanIterator[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, opts ...ScanOption) (Iterator[T, M], error) {
	args := &ScanArgs{startToken: nil, keyFilter: nil, pageSize: 100}
	for _, opt := range opts {
		opt(args)
	}

	if len(args.startToken) == 0 && len(args.keyFilter) != 0 {
		args.startToken = args.keyFilter
	}

	b, err := SetBucket(tx, path)
	if err != nil {
		return nil, errors.Wrapf(ErrPathNotFound, "path [%s]", path)
	}

	return &ScanIterator[T, M]{ctx: ctx, tx: tx, c: b.Cursor(), args: args, init: false}, nil
}

func (s *ScanIterator[T, M]) Next() bool {
	if s.init {
		s.key, s.value = s.c.Next()
	}

	if !s.init {
		if s.args.startToken == nil {
			s.key, s.value = s.c.First()
		} else {
			s.key, s.value = s.c.Seek(s.args.startToken)
		}
		s.init = true
	}

	return s.key != nil && bytes.HasPrefix(s.key, s.args.keyFilter)
}

func (s *ScanIterator[T, M]) RawKey() []byte {
	return s.key
}

func (s *ScanIterator[T, M]) RawValue() []byte {
	return s.value
}

func (s *ScanIterator[T, M]) Key() string {
	return string(s.key)
}

func (s *ScanIterator[T, M]) Value() M {
	msg, err := unmarshal[T, M](s.value)
	if err != nil {
		var result M
		return result
	}
	return msg
}

func (s *ScanIterator[T, M]) Delete() error {
	if s.key != nil {
		log.Trace().Str("key", s.Key()).Msg("delete")
		return s.c.Delete()
	}
	return nil
}

type PagedIterator[T any, M Message[T]] interface {
	Next() bool
	Value() []M
	NextToken() string
}

type PageIterator[T any, M Message[T]] struct {
	iter      *ScanIterator[T, M]
	nextToken []byte
	values    []M
}

func NewPageIterator[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, opts ...ScanOption) (PagedIterator[T, M], error) {
	iter, err := NewScanIterator[T, M](ctx, tx, path, opts...)
	if err != nil {
		return nil, err
	}

	return &PageIterator[T, M]{iter: iter.(*ScanIterator[T, M])}, nil
}

func (p *PageIterator[T, M]) Next() bool {
	results := []M{}
	for p.iter.Next() {
		results = append(results, p.iter.Value())

		if len(results) == int(p.iter.args.pageSize) {
			break
		}
	}

	p.values = results
	p.nextToken = []byte{}

	if p.iter.Next() {
		p.nextToken = p.iter.RawKey()
	}

	return false
}

func (p *PageIterator[T, M]) Value() []M {
	return p.values
}

func (p *PageIterator[T, M]) NextToken() string {
	return string(p.nextToken)
}

func Scan[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, filter string) ([]M, error) {
	iter, err := NewScanIterator[T, M](ctx, tx, path, WithKeyFilter(filter))
	if err != nil {
		return nil, err
	}

	var results []M
	for iter.Next() {
		results = append(results, iter.Value())
	}
	return results, nil
}

func ScanX[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path Path, keyFilter string, valueFilter func(M) bool) ([]M, error) {
	iter, err := NewScanIterator[T, M](ctx, tx, path, WithKeyFilter(keyFilter))
	if err != nil {
		return nil, err
	}

	var results []M
	for iter.Next() {
		v := iter.Value()
		if valueFilter(v) {
			results = append(results, v)
		}
	}
	return results, nil
}
