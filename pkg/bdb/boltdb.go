package bdb

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"time"

	cerr "github.com/aserto-dev/errors"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"

	"google.golang.org/grpc/codes"
)

// Error codes returned by failures to parse an expression.
var (
	ErrPathNotFound    = cerr.NewAsertoError("E20050", codes.NotFound, http.StatusNotFound, "path not found")
	ErrKeyNotFound     = cerr.NewAsertoError("E20051", codes.NotFound, http.StatusNotFound, "key not found")
	ErrKeyExists       = cerr.NewAsertoError("E20052", codes.AlreadyExists, http.StatusConflict, "key already exists")
	ErrMultipleResults = cerr.NewAsertoError("E20053", codes.FailedPrecondition, http.StatusExpectationFailed, "multiple results for singleton request")
)

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
	s.logger.Info().Str("db_path", s.config.DBPath).Msg("opening boltdb store")
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

// filePathExists, internal helper function to detect if the file path exists.
func filePathExists(path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, errors.Wrapf(err, "failed to stat file [%s]", path)
	}
}

func (s *BoltDB) DB() *bolt.DB {
	return s.db
}

func (s *BoltDB) Config() *Config {
	return s.config
}

// SetBucket, set bucket context to path.
func SetBucket(tx *bolt.Tx, path Path) (*bolt.Bucket, error) {
	var b *bolt.Bucket

	for index, p := range path {
		if index == 0 {
			b = tx.Bucket([]byte(p))
		} else {
			b = b.Bucket([]byte(p))
		}
		if b == nil {
			return nil, ErrPathNotFound
		}
	}

	if b == nil {
		return nil, ErrPathNotFound
	}
	return b, nil
}

// CreateBucket, create bucket path if not exists.
func CreateBucket(tx *bolt.Tx, path Path) (*bolt.Bucket, error) {
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
			return nil, err
		}
	}

	return b, nil
}

// DeleteBucket, delete tail bucket of path provided.
func DeleteBucket(tx *bolt.Tx, path Path) error {
	if len(path) == 1 {
		err := tx.DeleteBucket([]byte(path[0]))
		switch {
		case errors.Is(err, bolt.ErrBucketNotFound):
			return nil
		case err != nil:
			return err
		default:
			return nil
		}
	}

	b, err := SetBucket(tx, path[:len(path)-1])
	if err != nil {
		return nil
	}

	err = b.DeleteBucket([]byte(path[len(path)-1]))
	switch {
	case errors.Is(err, bolt.ErrBucketNotFound):
		return nil
	case err != nil:
		return err
	default:
		return nil
	}
}

// BucketExists, check if bucket path exists.
func BucketExists(tx *bolt.Tx, path Path) (bool, error) {
	_, err := SetBucket(tx, path)
	switch {
	case errors.Is(err, ErrPathNotFound):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

// SetKey, set key and value in the path specified bucket.
func SetKey(tx *bolt.Tx, path Path, key string, value []byte) error {
	b, err := SetBucket(tx, path)
	if err != nil {
		return err
	}
	if b == nil {
		return ErrPathNotFound
	}

	return b.Put([]byte(key), value)
}

// DeleteKey, delete key and value in path specified bucket, when it exists. None existing keys will not raise an error.
func DeleteKey(tx *bolt.Tx, path Path, key string) error {
	b, err := SetBucket(tx, path)
	if err != nil {
		return err
	}
	if b == nil {
		return ErrPathNotFound
	}

	return b.Delete([]byte(key))
}

// GetKey, get key and value from path specified bucket.
func GetKey(tx *bolt.Tx, path Path, key string) ([]byte, error) {
	b, err := SetBucket(tx, path)
	if err != nil {
		return []byte{}, err
	}
	if b == nil {
		return []byte{}, ErrPathNotFound
	}

	v := b.Get([]byte(key))
	if v == nil {
		return []byte{}, ErrKeyNotFound
	}

	return v, nil
}

// KeyExists, check if the key exists in the path specified bucket.
func KeyExists(tx *bolt.Tx, path Path, key string) (bool, error) {
	b, err := SetBucket(tx, path)
	if err != nil {
		return false, err
	}
	if b == nil {
		return false, ErrPathNotFound
	}

	v := b.Get([]byte(key))
	if v == nil {
		return false, nil
	}

	return true, nil
}

// List, returns a key-value iterator for the path specified bucket, with a starting position.
func list(tx *bolt.Tx, path Path, opts ...KVIteratorOption) (*KVIterator, error) {
	return NewKVIterator(tx, path, opts...)
}

// Scan, returns a key-value iterator for the specified bucket, with an enforced filter.
func scan(tx *bolt.Tx, path Path, filter string) ([][]byte, [][]byte, error) {
	var (
		keys   = make([][]byte, 0)
		values = make([][]byte, 0)
	)

	b, err := SetBucket(tx, path)
	if err != nil {
		return [][]byte{}, [][]byte{}, errors.Wrapf(ErrPathNotFound, "path [%s]", path)
	}

	c := b.Cursor()

	prefix := []byte(filter)

	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if k == nil {
			break
		}
		keys = append(keys, k)
		values = append(values, v)
	}

	return keys, values, nil
}

type KVIteratorOption func(*KVIterator)

// WithStartToken determine start position in cursor.
func WithStartToken(token string) KVIteratorOption {
	return func(i *KVIterator) {
		i.startToken = []byte(token)
	}
}

// withKeyFiler key filter to determine membership.
func WithKeyFilter(filter string) KVIteratorOption {
	return func(i *KVIterator) {
		i.keyFilter = []byte(filter)
	}
}

// KVIterator - key-value iterator.
type KVIterator struct {
	path        Path         // bucket path
	tx          *bolt.Tx     // transaction handle
	cursor      *bolt.Cursor // cursor handle
	startToken  []byte       // start token
	keyFilter   []byte       // key filter
	key         []byte       // current key (of kv)
	value       []byte       // current value  of (kv)
	err         error        // last error
	initialized bool         // iterator initialization state
}

func NewKVIterator(tx *bolt.Tx, path Path, opts ...KVIteratorOption) (*KVIterator, error) {
	b, err := SetBucket(tx, path)
	if err != nil {
		return nil, err
	}

	iter := &KVIterator{
		tx:          tx,
		path:        path,
		cursor:      b.Cursor(),
		initialized: false,
	}

	for _, opt := range opts {
		opt(iter)
	}

	// when start token not set, make key filter the starting position.
	if iter.startToken == nil && iter.keyFilter != nil {
		iter.startToken = iter.keyFilter
	}

	return iter, nil
}

func (i *KVIterator) Next() bool {
	if i.err != nil {
		return false
	}

	i.fetch()

	if i.keyFilter == nil {
		return i.key != nil
	}

	return i.key != nil && bytes.HasPrefix(i.key, i.keyFilter)
}

func (i *KVIterator) Key() []byte {
	return i.key
}

func (i *KVIterator) Value() []byte {
	return i.value
}

func (i *KVIterator) Error() error {
	return i.err
}

func (i *KVIterator) init() {
	if i.startToken == nil {
		i.key, i.value = i.cursor.First()
	} else {
		i.key, i.value = i.cursor.Seek(i.startToken)
	}
	i.initialized = true
}

func (i *KVIterator) fetch() {
	if !i.initialized {
		i.init()
		return
	}
	i.key, i.value = i.cursor.Next()
}
