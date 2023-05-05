package mig

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	bolt "go.etcd.io/bbolt"
)

const (
	versionKey  string = "version"
	baseVersion string = "0.0.0"
)

func SetBucket(tx *bolt.Tx, path []string) (*bolt.Bucket, error) {
	var b *bolt.Bucket

	for index, p := range path {
		if index == 0 {
			b = tx.Bucket([]byte(p))
		} else {
			b = b.Bucket([]byte(p))
		}
		if b == nil {
			return nil, os.ErrNotExist
		}
	}

	if b == nil {
		return nil, os.ErrNotExist
	}
	return b, nil
}

func SetKey(tx *bolt.Tx, path []string, key, value []byte) error {
	b, err := SetBucket(tx, path)
	if err != nil {
		return err
	}
	if b == nil {
		return os.ErrNotExist
	}

	return b.Put(key, value)
}

func CreateBucket(path []string) func(*bolt.DB, *bolt.DB) error {
	return func(_ *bolt.DB, rwDB *bolt.DB) error {

		if err := rwDB.Update(func(tx *bolt.Tx) error {
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
					return err
				}
			}

			return nil

		}); err != nil {
			return err
		}

		return nil
	}
}

func DeleteBucket(path []string) func(*bolt.DB, *bolt.DB) error {
	return func(_ *bolt.DB, rwDB *bolt.DB) error {

		if err := rwDB.Update(func(tx *bolt.Tx) error {
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

		}); err != nil {
			return err
		}

		return nil
	}
}

func GetVersion(db *bolt.DB) (*semver.Version, error) {
	ver, _ := semver.NewVersion(baseVersion)

	err := db.View(func(tx *bolt.Tx) error {
		b, err := SetBucket(tx, boltdb.SystemPath)
		if err != nil {
			return nil
		}

		v := b.Get([]byte(versionKey))

		// if key does not exist return base version.
		if v == nil {
			return nil
		}

		ver, _ = semver.NewVersion(string(v))

		return nil
	})

	return ver, err
}

func SetVersion(db *bolt.DB, version *semver.Version) (err error) {
	return db.Update(func(tx *bolt.Tx) error {
		b, err := SetBucket(tx, boltdb.SystemPath)
		if err != nil {
			return nil
		}

		return b.Put([]byte(versionKey), []byte(version.String()))
	})
}

func EnsureBaseVersion(_, rwDB *bolt.DB) error {
	return rwDB.Update(func(tx *bolt.Tx) error {
		b, err := SetBucket(tx, boltdb.SystemPath)
		if err != nil {
			return nil
		}

		return b.Put([]byte(versionKey), []byte(baseVersion))
	})
}

func BackupFilename(dbPath string, version *semver.Version) string {
	dir, file := filepath.Split(dbPath)
	ext := filepath.Ext(file)
	base := strings.TrimSuffix(file, ext)

	return filepath.Join(dir, fmt.Sprintf("%s-%s%s",
		base,
		version.String(),
		ext,
	))
}

func Backup(db *bolt.DB, version *semver.Version) error {
	dbPath := db.Path()

	return db.View(func(tx *bolt.Tx) error {
		w, err := os.Create(BackupFilename(dbPath, version))
		if err != nil {
			return err
		}

		if _, err := tx.WriteTo(w); err != nil {
			return err
		}
		return nil
	})
}

func OpenReadOnlyDB(dbPath string, version *semver.Version) (*bolt.DB, error) {
	db, err := bolt.Open(BackupFilename(dbPath, version), 0666, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	return db, nil
}
