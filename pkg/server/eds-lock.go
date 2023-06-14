package server

import (
	eds "github.com/aserto-dev/go-edge-ds"
	"github.com/aserto-dev/go-edge-ds/pkg/directory"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

type EdgeDirLock struct {
	open    bool
	edgeDir *directory.Directory
}

func (lock *EdgeDirLock) New(cfg *directory.Config, logger *zerolog.Logger) (*directory.Directory, error) {
	if lock.open {
		return lock.edgeDir, nil
	}

	edgeDir, err := eds.New(cfg, logger)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create edge directory server")
	}
	lock.open = true
	lock.edgeDir = edgeDir
	return edgeDir, err
}
