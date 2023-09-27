package bdb

import (
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
	MaxBatchSize   int
	MaxBatchDelay  time.Duration
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

	db, err := bolt.Open(s.config.DBPath, 0600, &bolt.Options{
		Timeout:      s.config.RequestTimeout,
		FreelistType: bolt.FreelistMapType,
	})
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
