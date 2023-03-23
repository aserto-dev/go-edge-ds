package migrate

import (
	"github.com/Masterminds/semver"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/directory/migrate/mig002"
	"github.com/aserto-dev/go-edge-ds/pkg/types"
)

type Migration func(*boltdb.BoltDB) error

func Store(store *boltdb.BoltDB, version string) error {
	reqVersion, err := semver.NewVersion(version)
	if err != nil {
		return err
	}

	current, err := getVersion(store)
	if err != nil {
		return err
	}

	curVersion, err := semver.NewVersion(current)
	if err != nil {
		return err
	}

	if !curVersion.LessThan(reqVersion) {
		return nil
	}

	if err := migrate(store, reqVersion.String()); err != nil {
		return err
	}

	if err := setVersion(store, reqVersion.String()); err != nil {
		return err
	}

	return nil
}

func getVersion(store *boltdb.BoltDB) (string, error) {
	txOpt, cleanup, err := store.ReadTxOpts()
	if err != nil {
		return "", err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	buf, err := store.Read(types.SystemPath(), "version", []boltdb.Opts{txOpt})
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func setVersion(store *boltdb.BoltDB, version string) (err error) {
	txOpt, cleanup, err := store.WriteTxOpts()
	if err != nil {
		return err
	}
	defer func() {
		cErr := cleanup(err)
		if cErr != nil {
			err = cErr
		}
	}()

	if err := store.Write(types.SystemPath(), "version", []byte(version), []boltdb.Opts{txOpt}); err != nil {
		return err
	}

	return nil
}

func migrate(store *boltdb.BoltDB, to string) error {
	migMap := map[string]Migration{
		"0.0.2": mig002.Migrate,
	}

	fnMigrate := migMap[to]

	return fnMigrate(store)
}
