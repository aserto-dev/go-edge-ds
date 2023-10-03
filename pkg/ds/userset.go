package ds

import (
	"context"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	bolt "go.etcd.io/bbolt"
)

// CreateUserSet, create the computed user set of a subject.
func CreateUserSet(ctx context.Context, tx *bolt.Tx, subject *dsc3.ObjectIdentifier) ([]*dsc3.ObjectIdentifier, error) {
	result := []*dsc3.ObjectIdentifier{subject}

	filter := ObjectIdentifier(subject).Key() + InstanceSeparator + "member"
	relations, err := bdb.Scan[dsc3.Relation](ctx, tx, bdb.RelationsSubPath, filter)
	if err != nil {
		return nil, err
	}

	for _, r := range relations {
		if r.GetObjectType() == "group" {
			result = append(result, Relation(r).Object())
		}
	}

	return result, nil
}
