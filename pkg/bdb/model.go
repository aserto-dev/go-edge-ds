package bdb

import (
	"context"

	"github.com/aserto-dev/azm"
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"

	bolt "go.etcd.io/bbolt"
)

func (s *BoltDB) LoadModel() error {
	ctx := context.Background()
	opts := []ScanOption{}

	objectTypes := map[string]*azm.ObjectType{}
	permissions := map[string]struct{}{}

	err := s.db.View(func(tx *bolt.Tx) error {

		objectTypeIter, err := NewScanIterator[dsc2.ObjectType](ctx, tx, ObjectTypesPath, opts...)
		if err != nil {
			return err
		}

		for objectTypeIter.Next() {

			objectType := objectTypeIter.Value()

			if _, ok := objectTypes[objectType.Name]; !ok {
				objectTypes[objectType.Name] = &azm.ObjectType{
					RelationTypes: map[string]*azm.RelationType{},
					Permissions:   map[string]*azm.RelationType{},
				}
			}
		}

		relationTypeIter, err := NewScanIterator[dsc2.RelationType](ctx, tx, RelationTypesPath, opts...)
		if err != nil {
			return err
		}

		for relationTypeIter.Next() {

			relationType := relationTypeIter.Value()

			if ot, ok := objectTypes[relationType.ObjectType]; ok {
				if _, ok := ot.RelationTypes[relationType.Name]; !ok {
					rt := &azm.RelationType{Union: map[string]struct{}{}}
					for _, union := range relationType.Unions {
						if _, ok := rt.Union[union]; !ok {
							rt.Union[union] = struct{}{}
						}
					}
					ot.RelationTypes[relationType.Name] = rt

					for _, permission := range relationType.Permissions {
						if p, ok := ot.Permissions[permission]; !ok {
							p = &azm.RelationType{Union: map[string]struct{}{}}
							p.Union[relationType.Name] = struct{}{}
							ot.Permissions[permission] = p
						} else {
							if _, ok := p.Union[relationType.Name]; !ok {
								p.Union[relationType.Name] = struct{}{}
								ot.Permissions[permission] = p
							}
						}
					}
				}
			}
		}

		permissionIter, err := NewScanIterator[dsc2.Permission](ctx, tx, PermissionsPath, opts...)
		if err != nil {
			return err
		}

		for permissionIter.Next() {
			permission := permissionIter.Value()

			if _, ok := permissions[permission.Name]; !ok {
				permissions[permission.Name] = struct{}{}
			}
		}

		return err
	})
	if err != nil {
		return err
	}

	s.model.SetObjectTypes(objectTypes)
	s.model.SetPermissions(permissions)

	return nil
}
