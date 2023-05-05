package migrate

import (
	"os"

	"github.com/Masterminds/semver"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate/mig"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate/mig001"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate/mig002"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type Migration func(*bolt.DB, *bolt.DB) error

// list of migration steps, keyed by version.
var migMap = map[string]Migration{
	mig001.Version: mig001.Migrate,
	mig002.Version: mig002.Migrate,
}

func Store(logger *zerolog.Logger, store *bdb.BoltDB, version string) error {
	log := logger.With().Str("component", "migrate").Logger()

	defer func() {
		if r := recover(); r != nil {
			log.Error().Msgf("recovered schema migration %s", r)
		}
	}()

	reqVersion, err := semver.NewVersion(version)
	if err != nil {
		return err
	}

	curVersion, err := mig.GetVersion(store.DB())
	if err != nil {
		return err
	}

	// if current is equal to required, no further action required.
	if curVersion.Equal(reqVersion) {
		return nil
	}

	log.Info().Str("current", curVersion.String()).Str("required", reqVersion.String()).Msg("begin schema migration")

	for {
		nextVersion := curVersion.IncPatch()
		log.Info().Str("next", nextVersion.String()).Msg("starting")

		if err := migrate(store, curVersion, &nextVersion); err != nil {
			log.Error().Err(err).Msg("migrate")
			return err
		}

		log.Info().Str("next", nextVersion.String()).Msg("finished")

		curVersion, err = mig.GetVersion(store.DB())
		if err != nil {
			return err
		}

		log.Info().Str("current", curVersion.String()).Msg("updated current version")

		if curVersion.Equal(reqVersion) {
			break
		}
	}

	log.Info().Str("current", curVersion.String()).Str("required", reqVersion.String()).Msg("finished schema migration")

	return nil
}

func migrate(store *bdb.BoltDB, curVersion, nextVersion *semver.Version) error {
	if err := mig.Backup(store.DB(), curVersion); err != nil {
		return err
	}

	roDB, err := mig.OpenReadOnlyDB(store.Config().DBPath, curVersion)
	if err != nil {
		return err
	}
	defer func() { _ = roDB.Close() }()

	if err := execute(roDB, store.DB(), nextVersion); err != nil {
		return err
	}

	if err := mig.SetVersion(store.DB(), nextVersion); err != nil {
		return err
	}

	return nil
}

func execute(roDB, rwDB *bolt.DB, newVersion *semver.Version) error {
	if fnMigrate, ok := migMap[newVersion.String()]; ok {
		return fnMigrate(roDB, rwDB)
	}
	return os.ErrNotExist
}
