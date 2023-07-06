package convert

import (
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
)

func Object2(r *dsc2.Object) *dsc3.Object {
	return &dsc3.Object{
		Type:        r.Type,
		Id:          r.Key,
		DisplayName: r.DisplayName,
		Properties:  r.Properties,
		CreatedAt:   r.CreatedAt,
		UpdatedAt:   r.UpdatedAt,
		Etag:        r.Hash,
	}
}

func Object3(r *dsc3.Object) *dsc2.Object {
	return &dsc2.Object{
		Type:        r.Type,
		Key:         r.Id,
		DisplayName: r.DisplayName,
		Properties:  r.Properties,
		CreatedAt:   r.CreatedAt,
		UpdatedAt:   r.UpdatedAt,
		Hash:        r.Etag,
	}
}

func ObjectIdentifier3(r *dsc3.Object) *dsc2.ObjectIdentifier {
	return &dsc2.ObjectIdentifier{
		Type: &r.Type,
		Key:  &r.Id,
	}
}

func Relation2(r *dsc2.Relation) *dsc3.Relation {
	return &dsc3.Relation{
		ObjectType:      r.Object.GetType(),
		ObjectId:        r.Object.GetKey(),
		Relation:        r.Relation,
		SubjectType:     r.Subject.GetType(),
		SubjectId:       r.Subject.GetKey(),
		SubjectRelation: "",
		CreatedAt:       r.CreatedAt,
		UpdatedAt:       r.UpdatedAt,
		Etag:            r.Hash,
	}
}

func Relation3(r *dsc3.Relation) *dsc2.Relation {
	return &dsc2.Relation{
		Object: &dsc2.ObjectIdentifier{
			Type: &r.ObjectType,
			Key:  &r.ObjectId,
		},
		Relation: r.Relation,
		Subject: &dsc2.ObjectIdentifier{
			Type: &r.SubjectType,
			Key:  &r.SubjectId,
		},
		CreatedAt: r.CreatedAt,
		UpdatedAt: r.UpdatedAt,
		Hash:      r.Etag,
	}
}

func RelationIdentifier3(r *dsc3.Relation) *dsc2.RelationIdentifier {
	return &dsc2.RelationIdentifier{
		Object: &dsc2.ObjectIdentifier{
			Type: &r.ObjectType,
			Key:  &r.ObjectId,
		},
		Relation: &dsc2.RelationTypeIdentifier{
			ObjectType: &r.ObjectType,
			Name:       &r.Relation,
		},
		Subject: &dsc2.ObjectIdentifier{
			Type: &r.SubjectType,
			Key:  &r.SubjectId,
		},
	}
}

func RelationTypeIdentifier3(r *dsc3.Relation) *dsc2.RelationTypeIdentifier {
	return &dsc2.RelationTypeIdentifier{
		ObjectType: &r.ObjectType,
		Name:       &r.Relation,
	}
}

func ObjectDependency2(r *dsc2.ObjectDependency) *dsc3.ObjectDependency {
	return &dsc3.ObjectDependency{
		ObjectType:      r.ObjectType,
		ObjectId:        r.ObjectKey,
		Relation:        r.Relation,
		SubjectType:     r.SubjectType,
		SubjectId:       r.SubjectKey,
		SubjectRelation: "",
		Depth:           r.Depth,
		IsCycle:         r.IsCycle,
		Path:            r.Path,
	}
}

func PaginationRequest3(r *dsc3.PaginationRequest) *dsc2.PaginationRequest {
	if r == nil {
		return &dsc2.PaginationRequest{}
	}
	return &dsc2.PaginationRequest{
		Size:  r.Size,
		Token: r.Token,
	}
}

func PaginationResponse2(r *dsc2.PaginationResponse) *dsc3.PaginationResponse {
	return &dsc3.PaginationResponse{
		NextToken: r.NextToken,
	}
}
