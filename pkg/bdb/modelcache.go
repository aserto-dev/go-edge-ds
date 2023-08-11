package bdb

import (
	"context"
	"encoding/json"
	"os"
	"sync"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"

	bolt "go.etcd.io/bbolt"
)

type ModelCache struct {
	ObjectTypes map[string]*ObjectType `json:"object_types"`
	Permissions map[string]struct{}    `json:"permissions"`
	m           sync.RWMutex           `json:"-"`
}

type ObjectType struct {
	RelationTypes map[string]*RelationType `json:"relation_types,omitempty"`
	Permissions   map[string]*RelationType `json:"permissions,omitempty"`
}

type RelationType struct {
	Union     map[string]struct{} `json:"union,omitempty"`     // OR
	Intersect map[string]struct{} `json:"intersect,omitempty"` // AND
	Exclude   *Exclusion          `json:"exclude,omitempty"`   // NOT
}

type Exclusion struct {
	Base     string `json:"base"`
	Subtract string `json:"subtract"`
}

func NewModelCache() *ModelCache {
	return &ModelCache{
		ObjectTypes: map[string]*ObjectType{},
		Permissions: map[string]struct{}{},
		m:           sync.RWMutex{},
	}
}

func (c *ModelCache) Load(db *BoltDB) error {
	ctx := context.Background()
	opts := []ScanOption{}

	objectTypes := map[string]*ObjectType{}
	permissions := map[string]struct{}{}

	err := db.DB().View(func(tx *bolt.Tx) error {

		objectTypeIter, err := NewScanIterator[dsc2.ObjectType](ctx, tx, ObjectTypesPath, opts...)
		if err != nil {
			return err
		}

		for objectTypeIter.Next() {

			objectType := objectTypeIter.Value()

			if _, ok := objectTypes[objectType.Name]; !ok {
				objectTypes[objectType.Name] = &ObjectType{
					RelationTypes: map[string]*RelationType{},
					Permissions:   map[string]*RelationType{},
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
					rt := &RelationType{Union: map[string]struct{}{}}
					for _, union := range relationType.Unions {
						if _, ok := rt.Union[union]; !ok {
							rt.Union[union] = struct{}{}
						}
					}
					ot.RelationTypes[relationType.Name] = rt

					for _, permission := range relationType.Permissions {
						if p, ok := ot.Permissions[permission]; !ok {
							p = &RelationType{Union: map[string]struct{}{}}
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

	c.m.Lock()
	defer c.m.Unlock()

	c.ObjectTypes = nil
	c.Permissions = nil
	c.ObjectTypes = objectTypes
	c.Permissions = permissions

	return err
}

func (c *ModelCache) Dump(filepath string) {
	w, err := os.Create(filepath)
	if err != nil {
		return
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(c); err != nil {
		return
	}
}

func (c *ModelCache) ObjectTypeExists(ot string) bool {
	c.m.RLock()
	defer c.m.RUnlock()
	_, ok := c.ObjectTypes[ot]
	return ok
}

func (c *ModelCache) RelationTypeExists(ot, rt string) bool {
	c.m.RLock()
	defer c.m.RUnlock()
	if o, ok := c.ObjectTypes[ot]; ok {
		_, ok := o.RelationTypes[rt]
		return ok
	}
	return false
}

// func (c *ModelCache) GetObjectType() (*dsc2.ObjectType, error) {
// 	return &dsc2.ObjectType{}, nil
// }

// func (c *ModelCache) GetObjectTypes() ([]*dsc2.ObjectType, error) {
// 	return []*dsc2.ObjectType{}, nil
// }

// func (c *ModelCache) GetRelationType() (*dsc2.RelationType, error) {
// 	return &dsc2.RelationType{}, nil
// }

// func (c *ModelCache) GetRelationTypes() ([]*dsc2.RelationType, error) {
// 	return []*dsc2.RelationType{}, nil
// }

// func (c *ModelCache) GetPermission() (*dsc2.Permission, error) {
// 	return &dsc2.Permission{}, nil
// }

// func (c *ModelCache) GetPermissions() ([]*dsc2.Permission, error) {
// 	return []*dsc2.Permission{}, nil
// }
