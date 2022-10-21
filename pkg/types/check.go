package types

import (
	"fmt"
	"strings"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type CheckResult struct {
	Check bool
	Trace []string
}

func (sc *StoreContext) CheckRelation(req *dsr.CheckRelationRequest) (*CheckResult, error) {
	if req == nil {
		return nil, derr.ErrInvalidArgument
	}

	subjectID, err := sc.GetObjectID(&ObjectIdentifier{req.Subject})
	if err != nil {
		return nil, derr.ErrInvalidArgument
	}

	objectID, err := sc.GetObjectID(&ObjectIdentifier{req.Object})
	if err != nil {
		return nil, derr.ErrInvalidArgument
	}

	relationTypeID, err := sc.GetRelationTypeID(&RelationTypeIdentifier{req.Relation})
	if err != nil {
		return nil, derr.ErrInvalidArgument
	}

	r, err := sc.check(subjectID, objectID, []int32{relationTypeID}, req.Trace)

	return &CheckResult{Check: r.Check, Trace: r.Trace}, err
}

func (sc *StoreContext) CheckPermission(req *dsr.CheckPermissionRequest) (*CheckResult, error) {
	if req == nil {
		return nil, derr.ErrInvalidArgument
	}
	// TBD
	// resolve permission to covering relations
	relations := []int32{}
	r, err := sc.check(req.Subject.GetId(), req.Object.GetId(), relations, req.Trace)

	return &CheckResult{Check: r.Check, Trace: r.Trace}, err
}

func (sc *StoreContext) check(subjectID, objectID string, relationIDs []int32, trace bool) (*CheckResult, error) {
	// expand relation union
	relations := sc.expandUnions(relationIDs)

	deps := []*ObjectDependency{}
	for _, relationID := range relationIDs {
		relID := relationID
		objDeps, err := sc.GetGraph(&dsr.GetGraphRequest{
			Anchor:   &dsc.ObjectIdentifier{Id: &subjectID},
			Subject:  &dsc.ObjectIdentifier{Id: &subjectID},
			Relation: &dsc.RelationTypeIdentifier{Id: &relID},
			Object:   &dsc.ObjectIdentifier{Id: &objectID},
		})
		if err != nil {
			return &CheckResult{}, err
		}
		deps = append(deps, objDeps...)
	}

	result := CheckResult{}

	for _, objDep := range deps {

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
		relType, err := sc.GetRelationType(&RelationTypeIdentifier{&dsc.RelationTypeIdentifier{Id: &rid}})
		if err != nil {
			continue
		}
		result = append(result, relType.Name)

		// get all relation types for given object type of relType, to find the ones that union the relType
		objRelTypes, _, err := sc.GetRelationTypes(&ObjectTypeIdentifier{&dsc.ObjectTypeIdentifier{Name: &relType.ObjectType}}, &PaginationRequest{&dsc.PaginationRequest{}})
		if err != nil {
			continue
		}

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
