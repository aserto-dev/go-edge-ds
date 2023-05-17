package model

// model contains type system related items

// type Model struct {
// 	Relations   map[string][]string
// 	Permissions map[string]struct{}
// }

// func NewResolver() (*Model, error) {
// 	model := Model{
// 		Relations:   map[string][]string{},
// 		Permissions: map[string]struct{}{},
// 	}
// 	return &model, nil
// }

// func (r *Model) Update(ctx context.Context, tx *bolt.Tx) error {
// 	page := &dsc.PaginationRequest{Size: 100}

// 	// get object types
// 	for {
// 		results, next, err := bdb.List[dsc.ObjectType](ctx, tx, bdb.ObjectTypesPath, page)
// 		if err != nil {
// 			return err
// 		}

// 		for _, objType := range results {
// 			_, ok := r.Relations[objType.Name]
// 			if !ok {
// 				r.Relations[objType.Name] = []string{}
// 			}
// 		}

// 		if next.GetNextToken() == "" {
// 			break
// 		}

// 		page.Token = next.GetNextToken()
// 	}

// 	// get relation types + unions
// 	for {
// 		results, next, err := bdb.List[dsc.RelationType](ctx, tx, bdb.RelationTypesPath, page)
// 		if err != nil {
// 			return err
// 		}

// 		for _, relType := range results {
// 			_, ok := r.Relations[relType.ObjectType+"/"+relType.Name]
// 			if !ok {
// 				r.Relations[relType.ObjectType+"/"+relType.Name] = relType.Unions
// 			}
// 		}

// 		// get permissions
// 		for _, relType := range results {
// 			for _, u := range relType.Unions {
// 				_ = u
// 				for _, p := range relType.Permissions {
// 					_, ok := r.Permissions[relType.ObjectType+"/"+relType.Name+"/"+p]
// 					if !ok {
// 						r.Permissions[relType.ObjectType+"/"+relType.Name+"/"+p] = struct{}{}
// 					}
// 				}
// 			}
// 		}

// 		if next.GetNextToken() == "" {
// 			break
// 		}

// 		page.Token = next.GetNextToken()
// 	}

// 	return nil
// }

// func (r *Model) ObjectTypeExists(objectType string) bool {
// 	_, ok := r.Relations[objectType]
// 	return ok
// }

// func (r *Model) RelationExists(objectType, relation string) bool {
// 	_, ok := r.Relations[objectType+"/"+relation]
// 	return ok
// }

// func (r *Model) PermissionExists(objectType, relation, permission string) bool {
// 	_, ok := r.Permissions[objectType+"/"+relation+"/"+permission]
// 	return ok
// }

// func (r *Model) ResolveRelation(objectType, relation string) ([]string, error) {
// 	return []string{}, nil
// }

// func (r *Model) ResolvePermission(objectType, permission string) ([]string, error) {
// 	return []string{}, nil
// }

// type Relation map[string][]string

// type ObjectRelation map[string]Relation

// type Manifest struct {
// 	SchemaVersion string `yaml:"schema_version,omitempty" json:"schema_version,omitempty"`
// }

// type ObjectTypes map[string]ObjectRelation

// func Open(filename string) (*Model, error) {
// 	buf, err := os.ReadFile(filename)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// manifest := make(ObjectTypes, 0)
// 	m := yaml.MapSlice{}
// 	if err := yaml.Unmarshal(buf, &m); err != nil {
// 		return nil, err
// 	}

// 	// manifestEntriesWithUnions := make(map[string]map[string][]string, 0)
// 	// permissions := make(map[string]bool, 0)

// 	// for objectType, manifestEntry := range manifest {
// 	// 	objType := &dsc.ObjectType{
// 	// 		Name: objectType,
// 	// 	}

// 	// 	for relationType, data := range manifestEntry {
// 	// 		// at first we create relation types that don't have unions
// 	// 		if len(data["union"]) > 0 {
// 	// 			manifestEntriesWithUnions[relationType] = data
// 	// 		} else {
// 	// 			err := c.setPermissions(data["permissions"], permissions)
// 	// 			if err != nil {
// 	// 				return errors.Wrapf(err, "failed to set permissions for relation %s", relationType)
// 	// 			}

// 	// 			req := &writer.SetRelationTypeRequest{
// 	// 				RelationType: &v2.RelationType{
// 	// 					Name:        relationType,
// 	// 					Permissions: data["permissions"],
// 	// 					ObjectType:  objectType,
// 	// 				}}
// 	// 			_, err = c.Writer.SetRelationType(ctx, req)
// 	// 			if err != nil {
// 	// 				return errors.Wrapf(err, "failed to set relation type %s", relationType)
// 	// 			}
// 	// 		}
// 	// 	}

// 	// 	for relationType, data := range manifestEntriesWithUnions {
// 	// 		err := c.setPermissions(data["permissions"], permissions)
// 	// 		if err != nil {
// 	// 			return errors.Wrapf(err, "failed to set permissions for relation %s", relationType)
// 	// 		}

// 	// 		req := &writer.SetRelationTypeRequest{
// 	// 			RelationType: &v2.RelationType{
// 	// 				Name:        relationType,
// 	// 				Permissions: data["permissions"],
// 	// 				ObjectType:  objectType,
// 	// 				Unions:      data["union"],
// 	// 			}}
// 	// 		_, err = c.Writer.SetRelationType(ctx, req)
// 	// 		if err != nil {
// 	// 			return errors.Wrapf(err, "failed to set relation type %s", relationType)
// 	// 		}
// 	// 	}
// 	// }
// 	return &Model{}, nil
// }

// func (c *Client) Load(ctx context.Context, file string) error {
// 	yfile, err := os.ReadFile(file)

// 	if err != nil {
// 		return err
// 	}

// 	manifestData := make(Manifest, 0)

// 	err = yaml.Unmarshal(yfile, &manifestData)

// 	if err != nil {
// 		return err
// 	}

// 	manifestEntriesWithUnions := make(map[string]map[string][]string, 0)
// 	permissions := make(map[string]bool, 0)

// 	for objectType, manifestEntry := range manifestData {
// 		req := &writer.SetObjectTypeRequest{
// 			ObjectType: &v2.ObjectType{
// 				Name: objectType,
// 			}}
// 		_, err := c.Writer.SetObjectType(ctx, req)
// 		if err != nil {
// 			return errors.Wrapf(err, "failed to set object type %s", objectType)
// 		}
// 		for relationType, data := range manifestEntry {
// 			// at first we create relation types that don't have unions
// 			if len(data["union"]) > 0 {
// 				manifestEntriesWithUnions[relationType] = data
// 			} else {
// 				err := c.setPermissions(data["permissions"], permissions)
// 				if err != nil {
// 					return errors.Wrapf(err, "failed to set permissions for relation %s", relationType)
// 				}

// 				req := &writer.SetRelationTypeRequest{
// 					RelationType: &v2.RelationType{
// 						Name:        relationType,
// 						Permissions: data["permissions"],
// 						ObjectType:  objectType,
// 					}}
// 				_, err = c.Writer.SetRelationType(ctx, req)
// 				if err != nil {
// 					return errors.Wrapf(err, "failed to set relation type %s", relationType)
// 				}
// 			}
// 		}

// 		for relationType, data := range manifestEntriesWithUnions {
// 			err := c.setPermissions(data["permissions"], permissions)
// 			if err != nil {
// 				return errors.Wrapf(err, "failed to set permissions for relation %s", relationType)
// 			}

// 			req := &writer.SetRelationTypeRequest{
// 				RelationType: &v2.RelationType{
// 					Name:        relationType,
// 					Permissions: data["permissions"],
// 					ObjectType:  objectType,
// 					Unions:      data["union"],
// 				}}
// 			_, err = c.Writer.SetRelationType(ctx, req)
// 			if err != nil {
// 				return errors.Wrapf(err, "failed to set relation type %s", relationType)
// 			}
// 		}
// 	}

// 	return nil
// }

// func (c *Client) setPermissions(permissions []string, alreadyAddedPerms map[string]bool) error {
// 	for _, perm := range permissions {
// 		if !alreadyAddedPerms[perm] {
// 			req := &writer.SetPermissionRequest{Permission: &v2.Permission{Name: perm}}
// 			_, err := c.Writer.SetPermission(context.Background(), req)
// 			if err != nil {
// 				return err
// 			}
// 			alreadyAddedPerms[perm] = true
// 		}
// 	}
// 	return nil
// }
