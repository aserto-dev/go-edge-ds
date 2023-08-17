package v3

import (
	"bytes"
	"context"
	"hash/fnv"
	"io"
	"strconv"

	dsm3 "github.com/aserto-dev/go-directory/aserto/directory/model/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-directory/pkg/gateway/model/v3"
	"github.com/bufbuild/protovalidate-go"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/rs/zerolog"
)

type Model struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
	v      *protovalidate.Validator
}

// store layout: _metadata/{name}/{version}/[metadata|manifest]

func NewModel(logger *zerolog.Logger, store *bdb.BoltDB) *Model {
	v, _ := protovalidate.New()
	return &Model{
		logger: logger,
		store:  store,
		v:      v,
	}
}

var _ = dsm3.ModelServer(&Model{})

func (s *Model) GetManifest(req *dsm3.GetManifestRequest, stream dsm3.Model_GetManifestServer) error {
	if err := s.v.Validate(req); err != nil {
		return err
	}

	metadata := &dsm3.Metadata{UpdatedAt: timestamppb.Now(), Etag: ""}

	modelErr := s.store.DB().View(func(tx *bolt.Tx) error {
		manifest, err := ds.Manifest(metadata).Get(stream.Context(), tx)
		switch {
		case bdb.ErrIsNotFound(err):
			return derr.ErrNotFound.Msgf("manifest")
		case err != nil:
			return errors.Errorf("failed to get manifest")
		}

		if err := stream.Send(&dsm3.GetManifestResponse{
			Msg: &dsm3.GetManifestResponse_Metadata{
				Metadata: manifest.Metadata,
			},
		}); err != nil {
			return err
		}

		body := &dsm3.Body{}

		for curByte := 0; curByte < len(manifest.Body.Data); curByte += model.MaxChunkSizeBytes {
			if curByte+model.MaxChunkSizeBytes > len(manifest.Body.Data) {
				body.Data = manifest.Body.Data[curByte:len(manifest.Body.Data)]
			} else {
				body.Data = manifest.Body.Data[curByte : curByte+model.MaxChunkSizeBytes]
			}

			if err := stream.Send(&dsm3.GetManifestResponse{
				Msg: &dsm3.GetManifestResponse_Body{
					Body: body,
				},
			}); err != nil {
				return err
			}
		}

		return nil
	})

	return modelErr
}

func (s *Model) SetManifest(stream dsm3.Model_SetManifestServer) error {
	logger := s.logger.With().Str("method", "SetManifest").Logger()
	logger.Trace().Send()

	h := fnv.New64a()
	h.Reset()

	metadata := &dsm3.Metadata{}
	data := bytes.NewBuffer([]byte{})

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return errors.Wrap(err, "failed to receive manifest")
		}

		// if md, ok := msg.GetMsg().(*dsm3.SetManifestRequest_Metadata); ok {
		// 	if err := s.v.Validate(md.Metadata); err != nil {
		// 		return err
		// 	}
		// 	metadata = md.Metadata
		// }

		if body, ok := msg.GetMsg().(*dsm3.SetManifestRequest_Body); ok {
			if err := s.v.Validate(body.Body); err != nil {
				return err
			}
			data.Write(body.Body.Data)
			_, _ = h.Write(data.Bytes())
		}
	}

	if err := stream.SendAndClose(&dsm3.SetManifestResponse{}); err != nil {
		return errors.Wrap(err, "failed to send manifest response")
	}

	if metadata.UpdatedAt == nil {
		metadata.UpdatedAt = timestamppb.Now()
	}

	metadata.Etag = strconv.FormatUint(h.Sum64(), 10)

	if err := s.v.Validate(metadata); err != nil {
		return err
	}

	modelErr := s.store.DB().Update(func(tx *bolt.Tx) error {
		if err := ds.Manifest(metadata).Set(stream.Context(), tx, data); err != nil {
			return errors.Errorf("failed to set manifest")
		}
		return nil
	})

	logger.Info().Msg("manifest updated")

	return modelErr
}

func (s *Model) DeleteManifest(ctx context.Context, req *dsm3.DeleteManifestRequest) (*dsm3.DeleteManifestResponse, error) {
	if err := s.v.Validate(req); err != nil {
		return &dsm3.DeleteManifestResponse{}, err
	}

	metadata := &dsm3.Metadata{}

	modelErr := s.store.DB().Update(func(tx *bolt.Tx) error {
		if err := ds.Manifest(metadata).Delete(ctx, tx); err != nil {
			return errors.Errorf("failed to delete manifest")
		}
		return nil
	})

	return &dsm3.DeleteManifestResponse{Result: &emptypb.Empty{}}, modelErr
}
