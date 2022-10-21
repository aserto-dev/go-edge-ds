package eds

import (
	"github.com/aserto-dev/go-edge-ds/pkg/directory"
	"github.com/rs/zerolog"
)

func New(config *directory.Config, logger *zerolog.Logger) (*directory.Directory, error) {
	newLogger := logger.With().Str("component", "edge-ds").Logger()

	ds, err := directory.New(config, &newLogger)
	if err != nil {
		return nil, err
	}

	return ds, nil
}
