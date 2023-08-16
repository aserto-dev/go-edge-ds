package bdb

import (
	"context"

	"github.com/aserto-dev/azm"
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"

	bolt "go.etcd.io/bbolt"
)

// type ModelCache struct {
// 	ObjectTypes map[string]*ObjectType `json:"object_types"`
// 	Permissions map[string]struct{}    `json:"permissions"`
// 	m           sync.RWMutex           `json:"-"`
// }

// type ObjectType struct {
// 	RelationTypes map[string]*RelationType `json:"relation_types,omitempty"`
// 	Permissions   map[string]*RelationType `json:"permissions,omitempty"`
// }

// type RelationType struct {
// 	Union     map[string]struct{} `json:"union,omitempty"`     // OR
// 	Intersect map[string]struct{} `json:"intersect,omitempty"` // AND
// 	Exclude   *Exclusion          `json:"exclude,omitempty"`   // NOT
// }

// func (r *RelationType) unions() []string {
// 	results := []string{}
// 	for k := range r.Union {
// 		results = append(results, k)
// 	}
// 	return results
// }

// func (o *ObjectType) permissions(relation string) []string {
// 	results := []string{}
// 	for name, rt := range o.Permissions {
// 		if _, ok := rt.Union[relation]; ok {
// 			results = append(results, name)
// 		}
// 	}
// 	return results
// }

// type Exclusion struct {
// 	Base     string `json:"base"`
// 	Subtract string `json:"subtract"`
// }

// func NewModelCache() *ModelCache {
// 	return &ModelCache{
// 		ObjectTypes: map[string]*ObjectType{},
// 		Permissions: map[string]struct{}{},
// 		m:           sync.RWMutex{},
// 	}
// }

func (s *BoltDB) LoadModel() (*azm.Model, error) {
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
		return nil, err
	}

	model := azm.NewModel()
	model.ObjectTypes = objectTypes
	model.Permissions = permissions

	return model, nil
}

// // ObjectTypeExists, checks if given object type name exists in the model cache.
// func (c *ModelCache) ObjectTypeExists(ot string) bool {
// 	c.m.RLock()
// 	defer c.m.RUnlock()
// 	_, ok := c.ObjectTypes[ot]
// 	return ok
// }

// // RelationTypeExists, checks if given relation type, for the given object type, exists in the model cache.
// func (c *ModelCache) RelationTypeExists(ot, rt string) bool {
// 	c.m.RLock()
// 	defer c.m.RUnlock()
// 	if o, ok := c.ObjectTypes[ot]; ok {
// 		_, ok := o.RelationTypes[rt]
// 		return ok
// 	}
// 	return false
// }

// // PermissionExists, checks if given permission, for the given object type, exists in the model cache.
// func (c *ModelCache) PermissionExists(ot, p string) bool {
// 	c.m.RLock()
// 	defer c.m.RUnlock()
// 	if o, ok := c.ObjectTypes[ot]; ok {
// 		_, ok := o.Permissions[p]
// 		return ok
// 	}
// 	return false
// }

// // ExpandRelation, returns list of relations which are a union of the given relation.
// // For example, when a writer relation inherits reader, the expansion of a reader = reader + writer.
// func (c *ModelCache) ExpandRelation(objectType, relation string) []string {
// 	c.m.RLock()
// 	defer c.m.RUnlock()

// 	results := []string{}

// 	// starting object type and relation must exist in order to be expanded.
// 	if ot, ok := c.ObjectTypes[objectType]; !ok {
// 		return results
// 	} else if _, ok := ot.RelationTypes[relation]; !ok {
// 		return results
// 	}

// 	// include given permission in result set
// 	results = append(results, relation)

// 	// iterate through each relation for the given object type, determine if it unions with the given relation.
// 	for name, relationType := range c.ObjectTypes[objectType].RelationTypes {
// 		if _, ok := relationType.Union[relation]; ok {
// 			results = append(results, c.ExpandRelation(objectType, name)...)
// 		}
// 	}

// 	return lo.Uniq(results)
// }

// // ExpandPermission returns list of relations which cover the given permission for the given object type.
// func (c *ModelCache) ExpandPermission(objectType, permission string) []string {
// 	c.m.RLock()
// 	defer c.m.RUnlock()

// 	results := []string{}

// 	// starting object type and permission must exist in order to be expanded.
// 	if ot, ok := c.ObjectTypes[objectType]; !ok {
// 		return results
// 	} else if _, ok := ot.Permissions[permission]; !ok {
// 		return results
// 	}

// 	// determine relation(s) that cover the permission.
// 	relationType := c.ObjectTypes[objectType].Permissions[permission]
// 	for name := range relationType.Union {
// 		results = append(results, c.ExpandRelation(objectType, name)...)
// 	}

// 	return lo.Uniq(results)
// }

// // Dump serializes the model cache to a JSON file, used for validation and testing.
// func (c *ModelCache) Dump(filepath string) {
// 	w, err := os.Create(filepath)
// 	if err != nil {
// 		return
// 	}

// 	enc := json.NewEncoder(w)
// 	enc.SetEscapeHTML(false)
// 	enc.SetIndent("", "  ")
// 	if err := enc.Encode(c); err != nil {
// 		return
// 	}
// }

// // GetObjectType, v2 backwards-compatibility accessor function, returns v2 ObjectType by name.
// func (c *ModelCache) GetObjectType(objectType string) (*dsc2.ObjectType, error) {
// 	c.m.RLock()
// 	defer c.m.RUnlock()

// 	if _, ok := c.ObjectTypes[objectType]; ok {
// 		return &dsc2.ObjectType{
// 			Name:        objectType,
// 			DisplayName: title(objectType),
// 			IsSubject:   false,
// 			Ordinal:     0,
// 			Status:      0,
// 			Schema:      &structpb.Struct{},
// 			CreatedAt:   timestamppb.Now(),
// 			UpdatedAt:   timestamppb.Now(),
// 			Hash:        "",
// 		}, nil
// 	}

// 	return &dsc2.ObjectType{}, derr.ErrObjectTypeNotFound.Msg(objectType)
// }

// // GetObjectTypes, v2 backwards-compatibility accessor function, returns list of v2.ObjectType instances.
// func (c *ModelCache) GetObjectTypes() ([]*dsc2.ObjectType, error) {
// 	c.m.RLock()
// 	defer c.m.RUnlock()

// 	results := []*dsc2.ObjectType{}

// 	for objectType := range c.ObjectTypes {
// 		results = append(results, &dsc2.ObjectType{
// 			Name:        objectType,
// 			DisplayName: title(objectType),
// 			IsSubject:   false,
// 			Ordinal:     0,
// 			Status:      0,
// 			Schema:      &structpb.Struct{},
// 			CreatedAt:   timestamppb.Now(),
// 			UpdatedAt:   timestamppb.Now(),
// 			Hash:        "",
// 		})
// 	}

// 	return results, nil
// }

// // GetRelationType, v2 backwards-compatibility accessor function, returns v2 RelationType by object type and relation name.
// func (c *ModelCache) GetRelationType(objectType, relation string) (*dsc2.RelationType, error) {
// 	c.m.RLock()
// 	defer c.m.RUnlock()

// 	ot, ok := c.ObjectTypes[objectType]
// 	if !ok {
// 		return &dsc2.RelationType{}, derr.ErrObjectTypeNotFound.Msg(objectType)
// 	}

// 	rt, ok := ot.RelationTypes[relation]
// 	if !ok {
// 		return &dsc2.RelationType{}, derr.ErrRelationNotFound.Msg(objectType + ":" + relation)
// 	}

// 	return &dsc2.RelationType{
// 		ObjectType:  objectType,
// 		Name:        relation,
// 		DisplayName: objectType + ":" + relation,
// 		Ordinal:     0,
// 		Status:      0,
// 		Unions:      rt.unions(),
// 		Permissions: ot.permissions(relation),
// 		CreatedAt:   timestamppb.Now(),
// 		UpdatedAt:   timestamppb.Now(),
// 		Hash:        "",
// 	}, nil
// }

// // GetRelationTypes, v2 backwards-compatibility accessor function, returns list of v2 RelationType instances, optionally filtered by by object type.
// func (c *ModelCache) GetRelationTypes(objectType string) ([]*dsc2.RelationType, error) {
// 	c.m.RLock()
// 	defer c.m.RUnlock()

// 	results := []*dsc2.RelationType{}

// 	objectTypes := c.ObjectTypes
// 	if objectType != "" {
// 		if ot, ok := c.ObjectTypes[objectType]; !ok {
// 			return results, derr.ErrObjectTypeNotFound.Msg(objectType)
// 		} else {
// 			objectTypes = map[string]*ObjectType{objectType: ot}
// 		}
// 	}

// 	for otn, ot := range objectTypes {
// 		for rtn, rt := range ot.RelationTypes {

// 			results = append(results, &dsc2.RelationType{
// 				ObjectType:  otn,
// 				Name:        rtn,
// 				DisplayName: otn + ":" + rtn,
// 				Ordinal:     0,
// 				Status:      0,
// 				Unions:      rt.unions(),
// 				Permissions: ot.permissions(rtn),
// 				CreatedAt:   timestamppb.Now(),
// 				UpdatedAt:   timestamppb.Now(),
// 				Hash:        "",
// 			})
// 		}
// 	}

// 	return results, nil
// }

// // GetPermission, v2 backwards-compatibility accessor function, returns v2 Permission by permission name.
// func (c *ModelCache) GetPermission(permission string) (*dsc2.Permission, error) {
// 	c.m.RLock()
// 	defer c.m.RUnlock()

// 	if _, ok := c.Permissions[permission]; ok {
// 		return &dsc2.Permission{
// 			Name:        permission,
// 			DisplayName: permission,
// 			CreatedAt:   timestamppb.Now(),
// 			UpdatedAt:   timestamppb.Now(),
// 			Hash:        "",
// 		}, nil
// 	}

// 	return &dsc2.Permission{}, derr.ErrPermissionNotFound.Msg(permission)
// }

// // GetPermissions, v2 backwards-compatibility accessor function, returns list of v2 Permission instances.
// func (c *ModelCache) GetPermissions() ([]*dsc2.Permission, error) {
// 	c.m.RLock()
// 	defer c.m.RUnlock()

// 	results := []*dsc2.Permission{}

// 	for permission := range c.Permissions {
// 		results = append(results, &dsc2.Permission{
// 			Name:        permission,
// 			DisplayName: permission,
// 			CreatedAt:   timestamppb.Now(),
// 			UpdatedAt:   timestamppb.Now(),
// 			Hash:        "",
// 		})
// 	}

// 	return results, nil
// }

// func title(s string) string {
// 	return cases.Title(language.AmericanEnglish, cases.NoLower).String(s)
// }
