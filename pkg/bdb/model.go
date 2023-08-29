package bdb

import (
	"context"

	"github.com/aserto-dev/azm"
	"github.com/aserto-dev/azm/model"
	v2 "github.com/aserto-dev/azm/v2"
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"

	bolt "go.etcd.io/bbolt"
)

func (s *BoltDB) LoadModel() error {
	ctx := context.Background()
	opts := []ScanOption{}

	m := &model.Model{
		Version: model.ModelVersion,
		Objects: map[model.ObjectName]*model.Object{},
	}

	relationTypes := []*dsc2.RelationType{}

	err := s.db.View(func(tx *bolt.Tx) error {

		objectTypeIter, err := NewScanIterator[dsc2.ObjectType](ctx, tx, ObjectTypesPath, opts...)
		if err != nil {
			return err
		}

		for objectTypeIter.Next() {
			objectType := objectTypeIter.Value()

			on := model.ObjectName(objectType.Name)

			// create object type if not exists
			if _, ok := m.Objects[on]; !ok {
				m.Objects[on] = &model.Object{
					Relations:   map[model.RelationName][]*model.Relation{},
					Permissions: map[model.PermissionName]*model.Permission{},
				}
			}
		}

		relationTypeIter, err := NewScanIterator[dsc2.RelationType](ctx, tx, RelationTypesPath, opts...)
		if err != nil {
			return err
		}

		for relationTypeIter.Next() {
			relationType := relationTypeIter.Value()
			relationTypes = append(relationTypes, relationType)

			on := model.ObjectName(relationType.ObjectType)
			rn := model.RelationName(relationType.Name)

			if _, ok := m.Objects[on]; !ok {
				m.Objects[on] = &model.Object{
					Relations:   map[model.RelationName][]*model.Relation{},
					Permissions: map[model.PermissionName]*model.Permission{},
				}
			}

			if _, ok := m.Objects[on].Relations[rn]; !ok {
				m.Objects[on].Relations[rn] = []*model.Relation{}
			}
		}

		return err
	})
	if err != nil {
		return err
	}

	for _, relationType := range relationTypes {
		on := model.ObjectName(relationType.ObjectType)
		rn := model.RelationName(relationType.Name)

		// get object type instance.
		o := m.Objects[on]

		for _, v := range relationType.Unions {
			rs, ok := o.Relations[model.RelationName(v)]
			if !ok {
				return azm.ErrRelationNotFound.Msg(v)
			}

			rs = append(rs, &model.Relation{Subject: &model.SubjectRelation{
				Object:   on,
				Relation: rn,
			}})

			o.Relations[model.RelationName(v)] = rs
		}

		for _, v := range relationType.Permissions {
			pn := model.PermissionName(v2.NormalizePermission(v))

			// if permission does not exist, create permission definition.
			if pd, ok := o.Permissions[pn]; !ok {
				p := &model.Permission{
					Union: []string{string(rn)},
				}
				o.Permissions[pn] = p
			} else {
				pd.Union = append(pd.Union, string(rn))
				o.Permissions[pn] = pd
			}
		}
	}

	return s.model.UpdateModel(m)
}
