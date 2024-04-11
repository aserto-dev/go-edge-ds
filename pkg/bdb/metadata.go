package bdb

import (
	"context"
	"errors"
	"time"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func UpdateMetadataObject(ctx context.Context, tx *bolt.Tx, path []string, key string, msg *dsc3.Object) (*dsc3.Object, error) {
	// get timestamp once for transaction.
	ts := timestamppb.New(time.Now().UTC())

	// get current instance.
	cur, err := GetObject(ctx, tx, path, key)
	switch {
	case errors.Is(err, ErrKeyNotFound):
		// new instance, set created_at timestamp.
		msg.CreatedAt = ts
		// if new instance set Etag to empty string.
		msg.Etag = ""

	case err != nil:
		return nil, err
	default:
		// existing instance, propagate created_at timestamp.
		msg.CreatedAt = cur.GetCreatedAt()
	}

	// always set updated_at timestamp.
	msg.UpdatedAt = ts

	if cur.GetEtag() != "" {
		msg.Etag = cur.GetEtag()
	}

	return msg, nil
}

func UpdateMetadataRelation(ctx context.Context, tx *bolt.Tx, path []string, key string, msg *dsc3.Relation) (*dsc3.Relation, error) {
	// get timestamp once for transaction.
	ts := timestamppb.New(time.Now().UTC())

	// get current instance.
	cur, err := GetRelation(ctx, tx, path, key)
	switch {
	case errors.Is(err, ErrKeyNotFound):
		// new instance, set created_at timestamp.
		msg.CreatedAt = ts
		// if new instance set Etag to empty string.
		msg.Etag = ""

	case err != nil:
		return nil, err
	default:
		// existing instance, propagate created_at timestamp.
		msg.CreatedAt = cur.CreatedAt
	}

	// always set updated_at timestamp.
	msg.UpdatedAt = ts

	if cur.GetEtag() != "" {
		msg.Etag = cur.GetEtag()
	}

	return msg, nil
}
