package v2

import (
	"context"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/convert"
	v3 "github.com/aserto-dev/go-edge-ds/pkg/directory/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"

	"github.com/rs/zerolog"
)

type Reader struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
	r3     dsr3.ReaderServer
}

func NewReader(logger *zerolog.Logger, store *bdb.BoltDB, r3 *v3.Reader) *Reader {
	return &Reader{
		logger: logger,
		store:  store,
		r3:     r3,
	}
}

// Get object type (metadata).
func (s *Reader) GetObjectType(ctx context.Context, req *dsr2.GetObjectTypeRequest) (*dsr2.GetObjectTypeResponse, error) {
	resp := &dsr2.GetObjectTypeResponse{}

	if ok, err := ds.ObjectTypeIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	objectType, err := s.store.MC().GetObjectType(req.Param.GetName())
	if err != nil {
		return resp, err
	}

	resp.Result = objectType

	return resp, err
}

// Get all objects types (metadata) (paginated).
func (s *Reader) GetObjectTypes(ctx context.Context, req *dsr2.GetObjectTypesRequest) (*dsr2.GetObjectTypesResponse, error) {
	resp := &dsr2.GetObjectTypesResponse{Results: []*dsc2.ObjectType{}}

	if req.Page == nil {
		req.Page = &dsc2.PaginationRequest{Size: 100}
	}

	objectTypes, err := s.store.MC().GetObjectTypes()

	resp.Results = objectTypes
	resp.Page = &dsc2.PaginationResponse{
		NextToken:  "",
		ResultSize: int32(len(resp.Results)),
	}

	return resp, err
}

// Get relation type (metadata).
func (s *Reader) GetRelationType(ctx context.Context, req *dsr2.GetRelationTypeRequest) (*dsr2.GetRelationTypeResponse, error) {
	resp := &dsr2.GetRelationTypeResponse{}

	if ok, err := ds.RelationTypeIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	relationType, err := s.store.MC().GetRelationType(req.Param.GetObjectType(), req.Param.GetName())
	if err != nil {
		return resp, err
	}

	resp.Result = relationType

	return resp, err
}

// Get all relation types, optionally filtered by object type (metadata) (paginated).
func (s *Reader) GetRelationTypes(ctx context.Context, req *dsr2.GetRelationTypesRequest) (*dsr2.GetRelationTypesResponse, error) {
	resp := &dsr2.GetRelationTypesResponse{Results: []*dsc2.RelationType{}, Page: &dsc2.PaginationResponse{}}

	if req.Param == nil {
		req.Param = &dsc2.ObjectTypeIdentifier{}
	}

	if ok, err := ds.ObjectTypeSelector(req.Param).Validate(); !ok {
		return resp, err
	}

	if req.Page == nil {
		req.Page = &dsc2.PaginationRequest{Size: 100}
	}

	relationTypes, err := s.store.MC().GetRelationTypes(req.Param.GetName())
	if err != nil {
		return resp, err
	}

	resp.Results = relationTypes
	resp.Page = &dsc2.PaginationResponse{
		NextToken:  "",
		ResultSize: int32(len(resp.Results)),
	}

	return resp, err
}

// Get permission (metadata).
func (s *Reader) GetPermission(ctx context.Context, req *dsr2.GetPermissionRequest) (*dsr2.GetPermissionResponse, error) {
	resp := &dsr2.GetPermissionResponse{}

	if ok, err := ds.PermissionIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	permission, err := s.store.MC().GetPermission(req.Param.GetName())
	if err != nil {
		return resp, err
	}

	resp.Result = permission

	return resp, err
}

// Get all permissions (metadata) (paginated).
func (s *Reader) GetPermissions(ctx context.Context, req *dsr2.GetPermissionsRequest) (*dsr2.GetPermissionsResponse, error) {
	resp := &dsr2.GetPermissionsResponse{Results: []*dsc2.Permission{}}

	if req.Page == nil {
		req.Page = &dsc2.PaginationRequest{Size: 100}
	}

	permissions, err := s.store.MC().GetPermissions()
	if err != nil {
		return resp, err
	}

	resp.Results = permissions
	resp.Page = &dsc2.PaginationResponse{
		NextToken:  "",
		ResultSize: int32(len(resp.Results)),
	}

	return resp, err
}

// Get single object instance.
func (s *Reader) GetObject(ctx context.Context, req *dsr2.GetObjectRequest) (*dsr2.GetObjectResponse, error) {
	r3, err := s.r3.GetObject(ctx, &dsr3.GetObjectRequest{
		ObjectType:    req.GetParam().GetType(),
		ObjectId:      req.GetParam().GetKey(),
		WithRelations: req.GetWithRelations(),
		Page: &dsc3.PaginationRequest{
			Size:  req.GetPage().GetSize(),
			Token: req.GetPage().GetToken(),
		},
	})
	if err != nil {
		return &dsr2.GetObjectResponse{}, err
	}

	r2 := &dsr2.GetObjectResponse{
		Result:    convert.ObjectToV2(r3.Result),
		Relations: convert.RelationArrayToV2(r3.Relations),
		Page:      convert.PaginationResponseToV2(r3.Page),
	}

	return r2, err
}

// Get multiple object instances by id or type+key, in a single request.
func (s *Reader) GetObjectMany(ctx context.Context, req *dsr2.GetObjectManyRequest) (*dsr2.GetObjectManyResponse, error) {

	r3, err := s.r3.GetObjectMany(ctx, &dsr3.GetObjectManyRequest{
		Param: convert.ObjectIdentifierArrayToV3(req.Param),
	})
	if err != nil {
		return &dsr2.GetObjectManyResponse{}, err
	}

	r2 := &dsr2.GetObjectManyResponse{
		Results: convert.ObjectArrayToV2(r3.Results),
	}

	return r2, err
}

// Get all object instances, optionally filtered by object type. (paginated).
func (s *Reader) GetObjects(ctx context.Context, req *dsr2.GetObjectsRequest) (*dsr2.GetObjectsResponse, error) {
	r3, err := s.r3.GetObjects(ctx, &dsr3.GetObjectsRequest{})
	if err != nil {
		return &dsr2.GetObjectsResponse{}, err
	}

	r2 := &dsr2.GetObjectsResponse{
		Results: convert.ObjectArrayToV2(r3.Results),
		Page:    convert.PaginationResponseToV2(r3.Page),
	}

	return r2, err
}

// Get relation instances based on subject, relation, object filter.
func (s *Reader) GetRelation(ctx context.Context, req *dsr2.GetRelationRequest) (*dsr2.GetRelationResponse, error) {
	r3, err := s.r3.GetRelation(ctx, &dsr3.GetRelationRequest{
		ObjectType:  req.GetParam().GetObject().GetType(),
		ObjectId:    req.GetParam().GetObject().GetKey(),
		Relation:    req.GetParam().GetRelation().GetName(),
		SubjectType: req.GetParam().GetSubject().GetType(),
		SubjectId:   req.GetParam().GetSubject().GetKey(),
		WithObjects: req.GetWithObjects(),
	})
	if err != nil {
		return &dsr2.GetRelationResponse{}, err
	}

	r2 := &dsr2.GetRelationResponse{
		Results: convert.RelationArrayToV2([]*dsc3.Relation{r3.Result}),
		Objects: map[string]*dsc2.Object{},
	}

	for k, v := range r3.Objects {
		r2.Objects[k] = convert.ObjectToV2(v)
	}

	return r2, err
}

// Get relation instances based on subject, relation, object filter (paginated).
func (s *Reader) GetRelations(ctx context.Context, req *dsr2.GetRelationsRequest) (*dsr2.GetRelationsResponse, error) {
	r3, err := s.r3.GetRelations(ctx, &dsr3.GetRelationsRequest{
		ObjectType:      req.GetParam().GetObject().GetType(),
		ObjectId:        req.GetParam().GetObject().GetKey(),
		Relation:        req.GetParam().GetRelation().GetName(),
		SubjectType:     req.GetParam().GetSubject().GetType(),
		SubjectId:       req.GetParam().GetSubject().GetKey(),
		SubjectRelation: "",
		WithObjects:     false,
		Page:            convert.PaginationRequestToV3(req.Page),
	})
	if err != nil {
		return &dsr2.GetRelationsResponse{}, err
	}

	r2 := &dsr2.GetRelationsResponse{
		Results: convert.RelationArrayToV2(r3.Results),
		Page:    convert.PaginationResponseToV2(r3.Page),
	}

	return r2, err
}

// Check if subject has permission on object.
func (s *Reader) CheckPermission(ctx context.Context, req *dsr2.CheckPermissionRequest) (*dsr2.CheckPermissionResponse, error) {
	r3, err := s.r3.CheckPermission(ctx, &dsr3.CheckPermissionRequest{
		ObjectType:  req.GetObject().GetType(),
		ObjectId:    req.GetObject().GetKey(),
		Permission:  req.GetPermission().GetName(),
		SubjectType: req.GetSubject().GetType(),
		SubjectId:   req.GetSubject().GetKey(),
		Trace:       req.GetTrace(),
	})
	if err != nil {
		return &dsr2.CheckPermissionResponse{}, err
	}

	r2 := &dsr2.CheckPermissionResponse{
		Check: r3.GetCheck(),
		Trace: r3.GetTrace(),
	}

	return r2, err
}

// Check if subject has relation to object.
func (s *Reader) CheckRelation(ctx context.Context, req *dsr2.CheckRelationRequest) (*dsr2.CheckRelationResponse, error) {
	r3, err := s.r3.CheckRelation(ctx, &dsr3.CheckRelationRequest{
		ObjectType:  req.GetObject().GetType(),
		ObjectId:    req.GetObject().GetKey(),
		Relation:    req.GetRelation().GetName(),
		SubjectType: req.GetSubject().GetType(),
		SubjectId:   req.GetSubject().GetKey(),
		Trace:       req.GetTrace(),
	})
	if err != nil {
		return &dsr2.CheckRelationResponse{}, err
	}

	r2 := &dsr2.CheckRelationResponse{
		Check: r3.GetCheck(),
		Trace: r3.GetTrace(),
	}

	return r2, err
}

// Get object dependency graph.
func (s *Reader) GetGraph(ctx context.Context, req *dsr2.GetGraphRequest) (*dsr2.GetGraphResponse, error) {
	r3, err := s.r3.GetGraph(ctx, &dsr3.GetGraphRequest{
		AnchorType:  req.GetAnchor().GetType(),
		AnchorId:    req.GetAnchor().GetKey(),
		ObjectType:  req.GetObject().GetType(),
		ObjectId:    req.GetObject().GetKey(),
		Relation:    req.GetRelation().GetName(),
		SubjectType: req.GetSubject().GetType(),
		SubjectId:   req.GetSubject().GetKey(),
	})
	if err != nil {
		return &dsr2.GetGraphResponse{}, err
	}

	r2 := &dsr2.GetGraphResponse{
		Results: []*dsc2.ObjectDependency{},
	}

	for _, v := range r3.Results {
		r2.Results = append(r2.Results, &dsc2.ObjectDependency{
			ObjectType:  v.GetObjectType(),
			ObjectKey:   v.GetObjectId(),
			Relation:    v.GetRelation(),
			SubjectType: v.GetSubjectType(),
			SubjectKey:  v.GetSubjectId(),
			Depth:       v.GetDepth(),
			IsCycle:     v.GetIsCycle(),
			Path:        v.GetPath(),
		})
	}

	return r2, err
}
