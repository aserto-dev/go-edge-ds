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

	subject, err := sc.GetObject(&ObjectIdentifier{req.Subject})
	if err != nil {
		return nil, derr.ErrInvalidArgument
	}

	object, err := sc.GetObject(&ObjectIdentifier{req.Object})
	if err != nil {
		return nil, derr.ErrInvalidArgument
	}

	relType, err := sc.GetRelationType(&RelationTypeIdentifier{req.Relation})
	if err != nil {
		return nil, derr.ErrInvalidArgument
	}

	r, err := sc.check(subject, object, []*RelationType{relType}, req.Trace)

	return &CheckResult{Check: r.Check, Trace: r.Trace}, err
}

func (sc *StoreContext) CheckPermission(req *dsr.CheckPermissionRequest) (*CheckResult, error) {
	if req == nil {
		return nil, derr.ErrInvalidArgument
	}

	subject, err := sc.GetObject(&ObjectIdentifier{req.Subject})
	if err != nil {
		return nil, derr.ErrInvalidArgument
	}

	object, err := sc.GetObject(&ObjectIdentifier{req.Object})
	if err != nil {
		return nil, derr.ErrInvalidArgument
	}

	// resolve permission to covering relations
	relations := []*RelationType{}
	r, err := sc.check(subject, object, relations, req.Trace)

	return &CheckResult{Check: r.Check, Trace: r.Trace}, err
}

func (sc *StoreContext) check(subject, object *Object, relations []*RelationType, trace bool) (*CheckResult, error) {
	// expand relation union
	relations = sc.expandUnions(relations)

	deps, err := sc.GetGraph(&dsr.GetGraphRequest{
		Anchor: &dsc.ObjectIdentifier{Id: &subject.Id, Type: &subject.Type, Key: &subject.Key},
		Object: &dsc.ObjectIdentifier{Id: &object.Id, Type: &object.Type, Key: &object.Key},
	})
	if err != nil {
		return &CheckResult{}, err
	}

	result := CheckResult{}

	for _, dep := range deps {

		if trace {
			result.Trace = append(result.Trace, fmt.Sprintf("depth:%d, is_cycle:%t, path:%q",
				dep.Depth, dep.IsCycle, dep.Path))
		}

		// object_id check
		if object.GetId() == dep.ObjectId {

			// check if relation in relation set which contain the requested permission
			relationInSet := false
			for _, relation := range relations {
				if relation.GetName() == dep.Relation {
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

func (sc *StoreContext) expandUnions(relations []*RelationType) []*RelationType {
	relTypeMap := map[string]*RelationType{}

	result := []*RelationType{}
	for _, relation := range relations {
		result = append(result, relation)

		// get all relation types for given object type of relType, to find the ones that union the relType
		objRelTypes, _, err := sc.GetRelationTypes(&ObjectTypeIdentifier{&dsc.ObjectTypeIdentifier{Name: &relation.ObjectType}}, &PaginationRequest{&dsc.PaginationRequest{}})
		if err != nil {
			continue
		}

		for _, objRelType := range objRelTypes {
			for _, union := range objRelType.Unions {
				if strings.EqualFold(union, relation.Name) {
					relTypeMap[objRelType.Name] = objRelType
				}
			}
		}
	}

	for _, v := range relTypeMap {
		result = append(result, v)
	}

	return result
}
