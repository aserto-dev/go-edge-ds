package boltdb

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

// Error codes returned by failures to parse an expression.
var (
	ErrPathNotFound = errors.New("path not found")
	ErrKeyNotFound  = errors.New("key not found")
	ErrKeyExists    = errors.New("key already exists")
)

type Opts func(interface{})

type Config struct {
	DBPath         string
	RequestTimeout time.Duration
}

// BoltDB based key-value store.
type BoltDB struct {
	logger *zerolog.Logger
	config *Config
	db     *bolt.DB
}

func New(config *Config, logger *zerolog.Logger) (*BoltDB, error) {
	newLogger := logger.With().Str("component", "kvs").Logger()
	db := BoltDB{
		config: config,
		logger: &newLogger,
	}
	return &db, nil
}

// Open BoltDB key-value store instance.
func (s *BoltDB) Open() error {
	s.logger.Info().Msg("opening boltdb store")
	var err error

	if s.config.DBPath == "" {
		return errors.New("store path not set")
	}

	dbDir := filepath.Dir(s.config.DBPath)
	exists, err := filePathExists(dbDir)
	if err != nil {
		return errors.Wrap(err, "failed to determine if store path/file exists")
	}
	if !exists {
		if err = os.MkdirAll(dbDir, 0700); err != nil {
			return errors.Wrapf(err, "failed to create directory '%s'", dbDir)
		}
	}

	db, err := bolt.Open(s.config.DBPath, 0600, &bolt.Options{Timeout: s.config.RequestTimeout})
	if err != nil {
		return errors.Wrapf(err, "failed to open directory '%s'", s.config.DBPath)
	}

	s.db = db

	return nil
}

// Close closes BoltDB key-value store instance.
func (s *BoltDB) Close() {
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
}

func (s *BoltDB) WriteTxOpts() (Opts, func(err error) error, error) {
	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to start write transaction")
	}

	return WithTx(tx), func(err error) error {
		if err != nil {
			return tx.Rollback()
		}

		return tx.Commit()
	}, nil
}

func WithSession(id string) Opts {
	return func(opts interface{}) {
		opts.(*sessionOpts).ID = &id
	}
}

type sessionOpts struct {
	ID *string
}

func GetSessionOpts(opts []Opts) *sessionOpts {
	sessionOpts := &sessionOpts{}
	for _, opt := range opts {
		opt(sessionOpts)
	}
	return sessionOpts
}

func WithTx(tx *bolt.Tx) Opts {
	return func(opts interface{}) {
		opts.(*txOpts).tx = tx
	}
}

type txOpts struct {
	tx *bolt.Tx
}

func getTxOpts(opts []Opts) *txOpts {
	txOpts := &txOpts{}
	for _, opt := range opts {
		opt(txOpts)
	}
	return txOpts
}

func (s *BoltDB) ReadTxOpts() (Opts, func() error, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to start write transaction")
	}

	return WithTx(tx), func() error {
		return tx.Rollback()
	}, nil
}

// Read value from key in bucket path.
func (s *BoltDB) Read(path []string, key string, opts []Opts) ([]byte, error) {
	s.logger.Trace().Interface("path", path).Str("key", key).Msg("Read")

	var res []byte

	read := func(tx *bolt.Tx) error {
		b, err := s.setBucket(tx, path)
		if err != nil {
			return errors.Wrapf(ErrPathNotFound, "path [%s]", path)
		}

		res = b.Get([]byte(key))
		if res == nil {
			return errors.Wrapf(ErrKeyNotFound, "key [%s]", key)
		}

		return nil
	}

	var err error
	txo := getTxOpts(opts)
	if txo.tx == nil {
		err = s.db.View(read)
	} else {
		err = read(txo.tx)
	}

	return res, err
}

// BucketExists checks if a bucket path exists.
func (s *BoltDB) BucketExists(path []string, opts []Opts) bool {
	s.logger.Trace().Interface("path", path).Msg("PathExists")

	exists := func(tx *bolt.Tx) error {
		_, err := s.setBucket(tx, path)
		return err
	}

	var err error
	txo := getTxOpts(opts)
	if txo.tx == nil {
		err = s.db.View(exists)
	} else {
		err = exists(txo.tx)
	}

	if errors.Is(err, ErrPathNotFound) {
		return false
	}

	if err != nil {
		s.logger.Debug().Interface("err", err).Msg("PathExists err")
	}

	return err == nil
}

// KeyExists checks if a key exists at given bucket path.
func (s *BoltDB) KeyExists(path []string, key string, opts []Opts) bool {
	s.logger.Trace().Interface("path", path).Str("key", key).Msg("KeyExists")

	exists := func(tx *bolt.Tx) error {
		b, err := s.setBucket(tx, path)
		if err != nil {
			return errors.Wrapf(err, "KeyExist path [%s]", path)
		}

		buf := b.Get([]byte(key))
		if buf == nil {
			return errors.Wrapf(ErrKeyNotFound, "key [%s]", key)
		}

		return nil
	}

	var err error
	txo := getTxOpts(opts)
	if txo.tx == nil {
		err = s.db.View(exists)
	} else {
		err = exists(txo.tx)
	}

	if err != nil && !(errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrPathNotFound)) {
		s.logger.Debug().Str("err", err.Error()).Msg("KeyExists")
	}

	return err == nil
}

// ListBuckets returns pages collection of buckets.
func (s *BoltDB) ListBuckets(path []string, pageToken string, pageSize int32, opts []Opts) ([]string, string, int32, error) {
	s.logger.Trace().Interface("path", path).Str("pageToken", pageToken).Int32("pageSize", pageSize).Msg("ListBuckets")

	var (
		buckets   = make([]string, 0)
		nextToken string
		totalSize int32
	)

	list := func(tx *bolt.Tx) error {

		if len(path) == 0 {
			_ = tx.ForEach(func(name []byte, b *bolt.Bucket) error {
				buckets = append(buckets, string(name))
				return nil
			})
			nextToken = ""
			totalSize = int32(len(buckets))
			return nil
		}

		b, err := s.setBucket(tx, path)
		if err != nil {
			return err
		}

		totalSize, err = getBucketCount(b)
		if err != nil {
			return err
		}

		switch pageSize {
		case ServerSetPageSize:
			pageSize = DefaultPageSize
		case TotalsOnlyResultSet:
			pageSize = 0
		}

		cursor := b.Cursor()

		var k []byte
		for i := int32(0); i < pageSize; i++ {
			if i == 0 {
				if pageToken == "" {
					k, _ = cursor.First()
				} else {
					k, _ = cursor.Seek([]byte(pageToken))
				}
			} else {
				k, _ = cursor.Next()
			}
			if k == nil {
				break
			}

			buckets = append(buckets, string(k))
		}

		k, _ = cursor.Next()
		if k != nil {
			nextToken = string(k)
		}

		return nil
	}

	var err error
	txo := getTxOpts(opts)
	if txo.tx == nil {
		err = s.db.View(list)
	} else {
		err = list(txo.tx)
	}

	if err != nil {
		s.logger.Trace().Err(err).Msg("ListBuckets")
		return []string{}, "", 0, nil
	}

	return buckets, nextToken, totalSize, nil
}

// ListKeys returns paged collection of keys
func (s *BoltDB) ListKeys(path []string, pageToken string, pageSize int32, opts []Opts) ([]string, string, int32, error) {
	s.logger.Trace().Interface("path", path).Str("pageToken", pageToken).Int32("pageSize", pageSize).Msg("ListKeys")

	var (
		keys      = make([]string, 0)
		nextToken string
		totalSize int32
	)

	list := func(tx *bolt.Tx) error {
		b, err := s.setBucket(tx, path)
		if err != nil {
			return err
		}

		totalSize, err = getKeysCount(b)
		if err != nil {
			return err
		}

		switch pageSize {
		case ServerSetPageSize:
			pageSize = DefaultPageSize
		case TotalsOnlyResultSet:
			pageSize = 0
		}

		cursor := b.Cursor()

		var k []byte
		for i := int32(0); i < pageSize; i++ {
			if i == 0 {
				if pageToken == "" {
					k, _ = cursor.First()
				} else {
					k, _ = cursor.Seek([]byte(pageToken))
				}
			} else {
				k, _ = cursor.Next()
			}
			if k == nil {
				break
			}

			keys = append(keys, string(k))
		}

		k, _ = cursor.Next()
		if k != nil {
			nextToken = string(k)
		}

		return nil
	}

	var err error
	txo := getTxOpts(opts)
	if txo.tx == nil {
		err = s.db.View(list)
	} else {
		err = list(txo.tx)
	}

	if err != nil {
		s.logger.Trace().Err(err).Msg("ListKeys")
		return []string{}, "", 0, nil
	}

	return keys, nextToken, totalSize, nil
}

// List returns paged collection of key and value arrays
func (s *BoltDB) List(path []string, pageToken string, pageSize int32, opts []Opts) ([]string, [][]byte, string, int32, error) {
	s.logger.Trace().Interface("path", path).Str("pageToken", pageToken).Int32("pageSize", pageSize).Msg("List")

	var (
		keys      = make([]string, 0)
		values    = make([][]byte, 0)
		nextToken string
		totalSize int32
	)

	list := func(tx *bolt.Tx) error {
		b, err := s.setBucket(tx, path)
		if err != nil {
			return err
		}

		switch pageSize {
		case ServerSetPageSize:
			pageSize = DefaultPageSize
		case TotalsOnlyResultSet:
			pageSize = 0
		}

		cursor := b.Cursor()

		var k, v []byte
		for i := int32(0); i < pageSize; i++ {
			if i == 0 {
				if pageToken == "" {
					k, v = cursor.First()
				} else {
					k, v = cursor.Seek([]byte(pageToken))
				}
			} else {
				k, v = cursor.Next()
			}
			if k == nil {
				break
			}

			keys = append(keys, string(k))
			values = append(values, v)
		}

		k, _ = cursor.Next()
		if k != nil {
			nextToken = string(k)
		}

		return nil
	}

	var err error
	txo := getTxOpts(opts)
	if txo.tx == nil {
		err = s.db.View(list)
	} else {
		err = list(txo.tx)
	}

	if err != nil {
		s.logger.Trace().Err(err).Msg("List")
		return []string{}, [][]byte{}, "", 0, nil
	}

	return keys, values, nextToken, totalSize, nil
}

// Write write value at key for given path.
func (s *BoltDB) Write(path []string, key string, value []byte, opts []Opts) error {
	s.logger.Trace().Interface("path", path).Str("key", key).Msg("Write")

	write := func(tx *bolt.Tx) error {
		b, err := s.setBucketIfNotExist(tx, path)
		if err != nil {
			return errors.Wrapf(err, "bucket [%s]", path)
		}

		if err := b.Put([]byte(key), value); err != nil {
			return errors.Wrap(err, "createHandler")
		}

		return nil
	}

	var err error
	txo := getTxOpts(opts)
	if txo.tx == nil {
		err = s.db.Update(write)
	} else {
		err = write(txo.tx)
	}

	return err
}

// Delete key or bucket, convenience function combing DeleteKey and DeleteBucket.
// func (s *BoltDB) Delete(path []string, key string, opts []Opts) error {
// 	s.logger.Trace().Interface("path", path).Str("key", key).Msg("Delete")

// 	if key == "" {
// 		return errors.Wrapf(ErrKeyNotFound, "key empty")
// 	}
// 	return s.DeleteKey(path, key, opts)
// }

// DeleteKey deletes key at given path when present.
// The call does not return an error when key does not exist.
func (s *BoltDB) DeleteKey(path []string, key string, opts []Opts) error {
	s.logger.Trace().Interface("path", path).Str("key", key).Msg("DeleteKey")

	del := func(tx *bolt.Tx) error {
		b, err := s.setBucketIfNotExist(tx, path)
		if err != nil {
			return nil
		}

		if err := b.Delete([]byte(key)); err != nil {
			return errors.Wrapf(err, "delete path:[%s] key:[%s]", path, key)
		}

		return nil
	}

	var err error
	txo := getTxOpts(opts)
	if txo.tx == nil {
		err = s.db.Update(del)
	} else {
		err = del(txo.tx)
	}

	return err
}

// DeleteBucket delete bucket at the tail of the given path.
// The call does not return an error when the bucket does not exist.
func (s *BoltDB) DeleteBucket(path []string, opts []Opts) error {
	s.logger.Trace().Interface("path", path).Msg("DeleteBucket")

	del := func(tx *bolt.Tx) error {
		if len(path) == 1 {
			return tx.DeleteBucket([]byte(path[0]))
		}

		b, err := s.setBucket(tx, path[:len(path)-1])
		if err != nil {
			return nil
		}
		err = b.DeleteBucket([]byte(path[len(path)-1]))
		if err != nil && errors.Is(err, bolt.ErrBucketNotFound) {
			return nil
		}
		return err
	}

	var err error
	txo := getTxOpts(opts)
	if txo.tx == nil {
		err = s.db.Update(del)
	} else {
		err = del(txo.tx)
	}

	return err
}

// CreateBucket creates the bucket
func (s *BoltDB) CreateBucket(path []string, opts []Opts) error {
	s.logger.Trace().Interface("path", path).Msg("CreateBucket")
	create := func(tx *bolt.Tx) error {
		b, err := s.setBucketIfNotExist(tx, path)
		if err != nil {
			return errors.Wrapf(err, "bucket [%s]", path)
		}
		if b == nil {
			return errors.Wrapf(err, "bucket [%s]", path)
		}
		return nil
	}

	var err error
	txo := getTxOpts(opts)
	if txo.tx == nil {
		err = s.db.Update(create)
	} else {
		err = create(txo.tx)
	}

	return err
}

// setBucketIfNotExists, internal helper function to set the bucket context.
// The call will create the bucket path when it does not exist.
func (s *BoltDB) setBucketIfNotExist(tx *bolt.Tx, path []string) (*bolt.Bucket, error) {
	var (
		b   *bolt.Bucket
		err error
	)
	for index, p := range path {
		if index == 0 {
			b, err = tx.CreateBucketIfNotExists([]byte(p))
		} else {
			b, err = b.CreateBucketIfNotExists([]byte(p))
		}
		if err != nil {
			return nil, errors.Wrapf(err, "bucket [%s]", p)
		}
	}

	if b == nil {
		return nil, errors.Wrapf(ErrPathNotFound, "path [%s]", pathStr(path))
	}
	return b, nil
}

// setBucket, internal helper function to set the bucket context.
// When the path does not exist a ErrPathNotFound error is returned.
func (s *BoltDB) setBucket(tx *bolt.Tx, path []string) (*bolt.Bucket, error) {
	var b *bolt.Bucket

	for index, p := range path {
		if index == 0 {
			b = tx.Bucket([]byte(p))
		} else {
			b = b.Bucket([]byte(p))
		}
		if b == nil {
			return nil, errors.Wrapf(ErrPathNotFound, "path [%s]", pathStr(path))
		}
	}

	if b == nil {
		return nil, errors.Wrapf(ErrPathNotFound, "path [%s]", pathStr(path))
	}
	return b, nil
}

// filePathExists, internal helper function to detect if the file path exists
func filePathExists(path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, errors.Wrapf(err, "failed to stat file [%s]", path)
	}
}

// getBucketCount, count buckets by iterating over buckets underneath the provided bucket.
func getBucketCount(b *bolt.Bucket) (int32, error) {
	var count int32
	err := b.ForEach(func(_, v []byte) error {
		if v == nil {
			count++
		}
		return nil
	})
	return count, err
}

// getKeysCount, count keys in bucket.
func getKeysCount(b *bolt.Bucket) (int32, error) {
	if b == nil {
		return -1, errors.Errorf("nil bucket")
	}
	stats := b.Stats()
	return int32(stats.KeyN), nil
}

func pathStr(path []string) string {
	return strings.Join(path, "/")
}

type PaginationOpts struct {
	Token    string
	PageSize int32
}

type PaginationResult struct {
	NextToken string
	TotalSize int32
}

const (
	// DefaultPageSize default pagination page size.
	DefaultPageSize = int32(100)
	// MinPageSize minimum pagination page size.
	MinPageSize = int32(1)
	// MaxPageSize maximum pagination page size.
	MaxPageSize = int32(100)
	// ServerSetPageSize .
	ServerSetPageSize = int32(0)
	// TotalsOnlyResultSet .
	TotalsOnlyResultSet = int32(-1)
)

// PageSize validator.
func PageSize(input int32) int32 {
	switch {
	case input == TotalsOnlyResultSet:
		return TotalsOnlyResultSet
	case input == ServerSetPageSize:
		return DefaultPageSize
	case input < MinPageSize:
		return MinPageSize
	case input > MaxPageSize:
		return MaxPageSize
	default:
		return input
	}
}
