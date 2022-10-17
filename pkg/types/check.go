package types

import (
	"context"
	"fmt"
	"strings"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-grpc/aserto/api/v2"
	"github.com/aserto-dev/go-utils/cerr"

	"github.com/aserto-dev/edge-ds/pkg/boltdb"
)

type CheckResult struct {
	Check bool
	Trace []string
}

func CheckRelation(ctx context.Context, req *dsr.CheckRelationRequest, store *boltdb.BoltDB, opts ...boltdb.Opts) (*CheckResult, error) {
	sc := StoreContext{
		Context: ctx,
		Store:   store,
		Opts:    opts,
	}

	subjectID, err := GetObjectID(ctx, req.Subject, store, opts...)
	if err != nil {
		return nil, cerr.ErrInvalidArgument
	}

	objectID, err := GetObjectID(ctx, req.Object, store, opts...)
	if err != nil {
		return nil, cerr.ErrInvalidArgument
	}

	relationTypeID, err := GetRelationTypeID(ctx, req.Relation, store, opts...)
	if err != nil {
		return nil, cerr.ErrInvalidArgument
	}

	r, err := sc.check(subjectID, objectID, []int32{relationTypeID}, req.Trace)

	return &CheckResult{Check: r.Check, Trace: r.Trace}, err
}

func CheckPermission(ctx context.Context, req *dsr.CheckPermissionRequest, store *boltdb.BoltDB, opts ...boltdb.Opts) (*CheckResult, error) {
	sc := StoreContext{
		Context: ctx,
		Store:   store,
		Opts:    opts,
	}

	// resolve permission to covering relations
	relations := []int32{}
	r, err := sc.check(req.Subject.GetId(), req.Object.GetId(), relations, req.Trace)

	return &CheckResult{Check: r.Check, Trace: r.Trace}, err
}

func (sc *StoreContext) check(subjectID, objectID string, relationIDs []int32, trace bool) (*CheckResult, error) {
	// expand relation union
	relations := sc.expandUnions(relationIDs)

	objDeps, err := GetGraph(sc.Context, &dsr.GetGraphRequest{Anchor: &dsc.ObjectIdentifier{Id: &subjectID}}, sc.Store, sc.Opts...)
	if err != nil {
		return &CheckResult{}, err
	}

	result := CheckResult{}

	for _, objDep := range objDeps {

		if trace {
			result.Trace = append(result.Trace, fmt.Sprintf("depth:%d, is_cycle:%t, path:%q",
				objDep.Depth, objDep.IsCycle, objDep.Path))
		}

		// object_id check
		if objectID == objDep.ObjectId {

			// check if relation in relation set which contain the requested permission
			relationInSet := false
			for _, relation := range relations {
				if relation == objDep.Relation {
					relationInSet = true
					break
				}
			}
			if !relationInSet {
				continue
			}

			result.Check = true

			// if not tracing, exit on check == true
			if !trace {
				break
			}
		}
	}

	return &result, nil
}

func (sc *StoreContext) expandUnions(relationIDs []int32) []string {
	relTypeMap := map[string]*RelationType{}
	result := []string{}
	for _, relationID := range relationIDs {
		rid := relationID
		relType, _ := GetRelationType(sc.Context, &dsc.RelationTypeIdentifier{Id: &rid}, sc.Store, sc.Opts...)
		result = append(result, relType.Name)

		// get all relation types for given object type of relType, to find the ones that union the relType
		objRelTypes, _, _ := GetRelationTypes(sc.Context, &dsr.GetRelationTypesRequest{
			Param: &dsc.ObjectTypeIdentifier{Name: &relType.ObjectType},
			Page:  &api.PaginationRequest{},
		}, sc.Store, sc.Opts...)

		for _, objRelType := range objRelTypes {
			for _, union := range objRelType.Unions {
				if strings.EqualFold(union, relType.Name) {
					relTypeMap[objRelType.Name] = objRelType
				}
			}
		}
	}

	for _, v := range relTypeMap {
		result = append(result, v.Name)
	}

	return result
}
