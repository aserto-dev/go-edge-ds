package ds

// // model contains type system related items
// type Model struct {
// 	ObjectTypes map[string]model.RelationType
// }

// func NewResolver() (*model.Model, error) {
// 	model := Model{
// 		ObjectTypes: make(map[string]RelationType),
// 	}
// 	return &model, nil
// }

// func (r *Model) Update(ctx context.Context, tx *bolt.Tx) error {
// 	page := &dsc.PaginationRequest{Size: 10}

// 	for {
// 		results, next, err := List[dsc.ObjectType](ctx, tx, boltdb.ObjectTypesPath, page)
// 		if err != nil {
// 			return err
// 		}

// 		for _, objType := range results {
// 			_, ok := r.ObjectTypes[objType.Name]
// 			if !ok {
// 				r.ObjectTypes[objType.Name] = RelationType{}
// 			}
// 		}

// 		if next.GetNextToken() == "" {
// 			break
// 		}

// 		page.Token = next.GetNextToken()
// 	}

// 	for {
// 		results, next, err := List[dsc.RelationType](ctx, tx, boltdb.RelationTypesPath, page)
// 		if err != nil {
// 			return err
// 		}

// 		for _, relType := range results {
// 			objType, ok := r.ObjectTypes[relType.ObjectType]
// 			if !ok {
// 				r.ObjectTypes[relType.ObjectType] = RelationType{}
// 			}

// 			rt, ok := objType[relType.Name]
// 			}
// 			if !ok {
// 				objType[relType.Name] = Relation{
// 					Unions: map[string]struct{}{},
// 					Permissions: map[string]struct{}{},
// 				}
// 			}
// 			for _, union := range relType.Unions {
// 				rt.Union[union] = struct{}{}
// 			}
// 			for _, perm := range relType.Permissions {
// 				rt.Permissions[perm] = struct{}{}
// 			}
// 		}

// 		if next.GetNextToken() == "" {
// 			break
// 		}

// 		page.Token = next.GetNextToken()
// 	}

// 	return nil
// }

// func (r *Model) ResolveRelation(objectType, relation string) ([]string, error) {
// 	return []string{}, nil
// }

// func (r *Model) ResolvePermission(objectType, permission string) ([]string, error) {
// 	return []string{}, nil
// }
