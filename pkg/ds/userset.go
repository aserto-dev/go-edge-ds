package ds

import (
	"context"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	bolt "go.etcd.io/bbolt"
)

// CreateUserSet, create the computed user set of a subject.
func CreateUserSet(ctx context.Context, tx *bolt.Tx, subject *dsc2.ObjectIdentifier) ([]*dsc2.ObjectIdentifier, error) {
	result := []*dsc2.ObjectIdentifier{subject}

	filter := ObjectIdentifier(subject).Key() + InstanceSeparator + "member"
	relations, err := bdb.Scan[dsc2.Relation](ctx, tx, bdb.RelationsSubPath, filter)
	if err != nil {
		return nil, err
	}

	for _, r := range relations {
		if r.Object.GetType() == "group" {
			result = append(result, r.Object)
		}
	}

	return result, nil
}
