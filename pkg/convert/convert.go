package convert

import (
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func ObjectIdentifierToV2(i *dsc3.ObjectIdentifier) *dsc2.ObjectIdentifier {
	return &dsc2.ObjectIdentifier{
		Type: &i.ObjectType,
		Key:  &i.ObjectId,
	}
}

func ObjectIdentifierArrayToV2(in []*dsc3.ObjectIdentifier) []*dsc2.ObjectIdentifier {
	result := make([]*dsc2.ObjectIdentifier, len(in))
	for i := 0; i < len(in); i++ {
		result[i] = ObjectIdentifierToV2(in[i])
	}
	return result
}

func ObjectIdentifierToV3(i *dsc2.ObjectIdentifier) *dsc3.ObjectIdentifier {
	return &dsc3.ObjectIdentifier{
		ObjectType: i.GetType(),
		ObjectId:   i.GetKey(),
	}
}

func ObjectIdentifierArrayToV3(in []*dsc2.ObjectIdentifier) []*dsc3.ObjectIdentifier {
	result := make([]*dsc3.ObjectIdentifier, len(in))
	for i := 0; i < len(in); i++ {
		result[i] = ObjectIdentifierToV3(in[i])
	}
	return result
}

func ObjectToV2(in *dsc3.Object) *dsc2.Object {
	if in.Properties == nil {
		in.Properties = pb.NewStruct()
	}
	p := proto.Clone(in.Properties).(*structpb.Struct)

	return &dsc2.Object{
		Type:        in.GetType(),
		Key:         in.GetId(),
		DisplayName: in.GetDisplayName(),
		Properties:  p,
		CreatedAt:   in.GetCreatedAt(),
		UpdatedAt:   in.GetUpdatedAt(),
		Hash:        in.GetEtag(),
	}
}

func ObjectArrayToV2(in []*dsc3.Object) []*dsc2.Object {
	result := make([]*dsc2.Object, len(in))
	for i := 0; i < len(in); i++ {
		result[i] = ObjectToV2(in[i])
	}
	return result
}

func ObjectMapToV2(in map[string]*dsc3.Object) map[string]*dsc2.Object {
	result := make(map[string]*dsc2.Object, len(in))
	for k, v := range in {
		result[k] = ObjectToV2(v)
	}
	return result
}

func ObjectToV3(in *dsc2.Object) *dsc3.Object {
	if in.Properties == nil {
		in.Properties = pb.NewStruct()
	}
	p := proto.Clone(in.Properties).(*structpb.Struct)

	return &dsc3.Object{
		Type:        in.GetType(),
		Id:          in.GetKey(),
		DisplayName: in.GetDisplayName(),
		Properties:  p,
		CreatedAt:   in.GetCreatedAt(),
		UpdatedAt:   in.GetUpdatedAt(),
		Etag:        in.GetHash(),
	}
}

func ObjectArrayToV3(in []*dsc2.Object) []*dsc3.Object {
	result := make([]*dsc3.Object, len(in))
	for i := 0; i < len(in); i++ {
		result[i] = ObjectToV3(in[i])
	}
	return result
}

func ObjectDependencyToV3(in *dsc2.ObjectDependency) *dsc3.ObjectDependency {
	return &dsc3.ObjectDependency{
		ObjectType:  in.GetObjectType(),
		ObjectId:    in.GetObjectKey(),
		Relation:    in.GetRelation(),
		SubjectType: in.GetSubjectType(),
		SubjectId:   in.GetSubjectKey(),
		Depth:       in.GetDepth(),
		IsCycle:     in.GetIsCycle(),
		Path:        in.GetPath(),
	}
}

func ObjectDependencyArrayToV3(in []*dsc2.ObjectDependency) []*dsc3.ObjectDependency {
	result := make([]*dsc3.ObjectDependency, len(in))
	for i := 0; i < len(in); i++ {
		result[i] = ObjectDependencyToV3(in[i])
	}
	return result
}

func RelationToV2(in *dsc3.Relation) *dsc2.Relation {
	return &dsc2.Relation{
		Object: &dsc2.ObjectIdentifier{
			Type: &in.ObjectType,
			Key:  &in.ObjectId,
		},
		Relation: in.Relation,
		Subject: &dsc2.ObjectIdentifier{
			Type: &in.SubjectType,
			Key:  &in.SubjectId,
		},
		CreatedAt: in.CreatedAt,
		UpdatedAt: in.UpdatedAt,
		Hash:      in.Etag,
	}
}

func RelationArrayToV2(in []*dsc3.Relation) []*dsc2.Relation {
	result := make([]*dsc2.Relation, len(in))
	for i := 0; i < len(in); i++ {
		result[i] = RelationToV2(in[i])
	}
	return result
}

func RelationToV3(in *dsc2.Relation) *dsc3.Relation {
	return &dsc3.Relation{
		ObjectType:  in.GetObject().GetType(),
		ObjectId:    in.GetObject().GetKey(),
		Relation:    in.GetRelation(),
		SubjectType: in.GetSubject().GetType(),
		SubjectId:   in.GetSubject().GetKey(),
		CreatedAt:   in.GetCreatedAt(),
		UpdatedAt:   in.GetUpdatedAt(),
		Etag:        in.GetHash(),
	}
}

func RelationArrayToV3(in []*dsc2.Relation) []*dsc3.Relation {
	result := make([]*dsc3.Relation, len(in))
	for i := 0; i < len(in); i++ {
		result[i] = RelationToV3(in[i])
	}
	return result
}

func PaginationRequestToV3(in *dsc2.PaginationRequest) *dsc3.PaginationRequest {
	if in == nil {
		return &dsc3.PaginationRequest{Size: 100, Token: ""}
	}

	return &dsc3.PaginationRequest{
		Size:  in.Size,
		Token: in.Token,
	}
}

func PaginationResponseToV2(in *dsc3.PaginationResponse) *dsc2.PaginationResponse {
	if in == nil {
		return &dsc2.PaginationResponse{NextToken: ""}
	}

	return &dsc2.PaginationResponse{
		NextToken: in.NextToken,
	}
}
