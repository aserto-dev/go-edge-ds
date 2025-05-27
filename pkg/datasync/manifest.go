package datasync

import (
	"bytes"
	"context"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/aserto-dev/azm/model"
	v3 "github.com/aserto-dev/azm/v3"
	"github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	"github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-directory/pkg/pb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Sync) syncManifest(ctx context.Context, conn *grpc.ClientConn) error {
	runStartTime := time.Now().UTC()

	s.logger.Info().Str(syncStatus, syncStarted).Str("mode", Manifest.String()).Msg(syncManifest)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	remoteManifest, err := s.getRemoteManifest(ctx, conn)
	if err != nil {
		return err
	}

	localManifest, err := s.getLocalManifest(ctx)
	if err != nil {
		return err
	}

	s.logger.Debug().
		Str("local.etag", localManifest.GetEtag()).Str("remote.etag", remoteManifest.GetEtag()).
		Bool("identical", localManifest.GetEtag() == remoteManifest.GetEtag()).Msg(syncManifest)

	if localManifest.GetEtag() == remoteManifest.GetEtag() {
		return nil
	}

	m, err := s.setManifest(ctx, remoteManifest)
	if err != nil {
		return err
	}

	runEndTime := time.Now().UTC()

	s.logger.Info().
		Str(syncStatus, syncFinished).Str("mode", Manifest.String()).
		Str("duration", runEndTime.Sub(runStartTime).String()).Msg(syncManifest)

	return s.store.MC().UpdateModel(m)
}

func (s *Sync) setManifest(ctx context.Context, man *common.Manifest) (*model.Model, error) {
	// calc new ETag from remote manifest.
	h := fnv.New64a()
	h.Reset()
	_, _ = h.Write(man.GetBody())

	man.UpdatedAt = timestamppb.Now()
	man.Etag = strconv.FormatUint(h.Sum64(), 10)

	mdl, err := v3.Load(bytes.NewReader(man.GetBody()))
	if err != nil {
		return nil, derr.ErrInvalidArgument.Msg(err.Error())
	}

	r, err := mdl.Reader()
	if err != nil {
		return nil, err
	}

	m := pb.NewStruct()
	if err := pb.BufToProto(r, m); err != nil {
		return nil, err
	}

	mod := &common.Model{
		Model:     m,
		UpdatedAt: man.GetUpdatedAt(),
		Etag:      man.GetEtag(),
	}

	if err := s.store.DB().Update(func(tx *bolt.Tx) error {
		stats, err := ds.CalculateStats(ctx, tx)
		if err != nil {
			return derr.ErrUnknown.Msgf("failed to calculate stats: %s", err.Error())
		}

		if err := s.store.MC().CanUpdate(mdl, stats); err != nil {
			return err
		}

		if _, err := bdb.Set(ctx, tx, bdb.ManifestPathV2, bdb.ManifestKey, man); err != nil {
			return err
		}

		if _, err := bdb.Set(ctx, tx, bdb.ManifestPathV2, bdb.ModelKey, mod); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return mdl, nil
}

func (s *Sync) getLocalManifest(ctx context.Context) (*common.Manifest, error) {
	resp := &common.Manifest{}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		result, err := s.getManifest(ctx, tx)
		if err != nil {
			return err
		}

		resp = result

		return nil
	})

	return resp, err
}

func (s *Sync) getManifest(ctx context.Context, tx *bolt.Tx) (*common.Manifest, error) {
	resp := &common.Manifest{}

	manifest, err := bdb.Get[common.Manifest](ctx, tx, bdb.ManifestPathV2, bdb.ManifestKey)

	switch {
	case status.Code(err) == codes.NotFound:
		return resp, nil

	case err != nil:
		return resp, errors.Errorf("failed to get manifest")

	default:
		resp = manifest
		return resp, nil
	}
}

func (s *Sync) getRemoteManifest(ctx context.Context, conn *grpc.ClientConn) (*common.Manifest, error) {
	resp := &common.Manifest{}

	rdr := reader.NewReaderClient(conn)

	result, err := rdr.GetManifest(ctx, &reader.GetManifestRequest{Empty: &emptypb.Empty{}})
	if err != nil {
		return resp, err
	}

	resp = result.GetManifest()

	return resp, nil
}
