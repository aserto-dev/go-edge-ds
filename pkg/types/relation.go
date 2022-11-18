package types

import (
	"bytes"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	"github.com/aserto-dev/go-edge-ds/pkg/session"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Relation struct {
	*dsc.Relation
}

func NewRelation(i *dsc.Relation) *Relation {
	if i == nil {
		return &Relation{Relation: &dsc.Relation{}}
	}
	return &Relation{Relation: i}
}

func (i *Relation) Validate() (bool, error) {
	if i.Relation == nil {
		return false, derr.ErrInvalidRelation
	}

	if i.Subject == nil {
		return false, derr.ErrInvalidArgument.Msg("subject not set")
	}
	subject := ObjectIdentifier{ObjectIdentifier: i.Subject}
	if ok, err := subject.Validate(); !ok {
		return false, err
	}

	if i.Object == nil {
		return false, derr.ErrInvalidArgument.Msg("object not set")
	}
	object := ObjectIdentifier{ObjectIdentifier: i.Object}
	if ok, err := object.Validate(); !ok {
		return false, err
	}

	if strings.TrimSpace(i.GetRelation()) == "" {
		return false, derr.ErrInvalidArgument.Msg("relation not set")
	}
	return true, nil
}

func (i *Relation) Normalize() error {
	i.Relation.Relation = strings.ToLower(i.Relation.GetRelation())
	*i.Relation.Subject.Type = strings.ToLower(i.Relation.Subject.GetType())
	*i.Relation.Object.Type = strings.ToLower(i.Relation.Object.GetType())
	return nil
}

func (i *Relation) Msg() *dsc.Relation {
	if i == nil || i.Relation == nil {
		return &dsc.Relation{}
	}
	return i.Relation
}

func (i *Relation) GetHash() (string, error) {
	h := fnv.New64a()
	h.Reset()

	if i != nil && i.Relation != nil {
		if i.Relation.Subject != nil {
			if i.Relation.Subject.Id != nil {
				if _, err := h.Write([]byte(i.Relation.Subject.GetId())); err != nil {
					return DefaultHash, err
				}
				if _, err := h.Write([]byte(i.Relation.Subject.GetKey())); err != nil {
					return DefaultHash, err
				}
				if _, err := h.Write([]byte(i.Relation.Subject.GetType())); err != nil {
					return DefaultHash, err
				}
			}
		}
		if _, err := h.Write([]byte(i.Relation.Relation)); err != nil {
			return DefaultHash, err
		}
		if i.Relation.Object != nil {
			if i.Relation.Object.Id != nil {
				if _, err := h.Write([]byte(i.Relation.Object.GetId())); err != nil {
					return DefaultHash, err
				}
				if _, err := h.Write([]byte(i.Relation.Object.GetKey())); err != nil {
					return DefaultHash, err
				}
				if _, err := h.Write([]byte(i.Relation.Object.GetType())); err != nil {
					return DefaultHash, err
				}
			}
		}
	}

	return strconv.FormatUint(h.Sum64(), 10), nil
}

func (i *Relation) Key() string {
	return i.Object.GetType() + ":" + i.GetRelation()
}

func (i *Relation) SubjectKey() string {
	return i.Subject.GetId() + "|" + i.Key() + "|" + i.Object.GetId()
}

func (i *Relation) ObjectKey() string {
	return i.Object.GetId() + "|" + i.Key() + "|" + i.Subject.GetId()
}

func (sc *StoreContext) getRelation(relationIdentifier *RelationIdentifier) (*Relation, error) {
	ri, err := relationIdentifier.Resolve(sc)
	if err != nil {
		return &Relation{}, err
	}

	buf, err := sc.Store.Read(RelationsObjPath(), ri.ObjKey(), sc.Opts)
	if err != nil {
		return nil, err
	}

	var rel dsc.Relation
	if err := pb.BufToProto(bytes.NewReader(buf), &rel); err != nil {
		return nil, err
	}

	return &Relation{&rel}, nil
}

// nolint: gocyclo
func (sc *StoreContext) GetRelation(relationIdentifier *RelationIdentifier) ([]*Relation, error) {
	var subID, objID, objType, relName, filter string
	var path []string

	subIdentifier := &ObjectIdentifier{relationIdentifier.Subject}
	if ok, _ := subIdentifier.Validate(); ok {
		if subIdentifier.Id != nil {
			subID = subIdentifier.GetId()
		} else if subIdentifier.GetKey() != "" && subIdentifier.GetType() != "" {
			buf, err := sc.Store.Read(ObjectsKeyPath(), subIdentifier.Key(), sc.Opts)
			if err != nil {
				return nil, err
			}
			subID = string(buf)
		}
	}

	objIdentifier := &ObjectIdentifier{relationIdentifier.Object}
	if ok, _ := objIdentifier.Validate(); ok {
		if objIdentifier.Id != nil {
			objID = objIdentifier.GetId()
		} else if objIdentifier.GetType() != "" && objIdentifier.GetKey() != "" {
			buf, err := sc.Store.Read(ObjectsKeyPath(), objIdentifier.Key(), sc.Opts)
			if err != nil {
				return nil, err
			}
			objID = string(buf)
		} else if objIdentifier.Type != nil {
			objType = objIdentifier.GetType()
		}
	}

	relIdentifier := &RelationTypeIdentifier{relationIdentifier.Relation}
	if relIdentifier.GetObjectType() == "" {
		relIdentifier.ObjectType = &objType
	}
	if ok, _ := relIdentifier.Validate(); ok {
		relTypeName, err := sc.GetRelationTypeName(relIdentifier)
		if err != nil {
			return nil, err
		}
		relName = relTypeName
	}

	switch {
	case ID.IsValid(objID):
		path = RelationsObjPath()
		filter = makeFilter(objID, "|", relName, "|", subID)
	case ID.IsValid(subID):
		path = RelationsSubPath()
		filter = makeFilter(subID, "|", relName, "|", objID)
	default:
		return []*Relation{}, derr.ErrInvalidArgument.Msg("no anchor: subject or object id")
	}

	_, values, err := sc.Store.ReadScan(path, filter, sc.Opts)
	if err != nil {
		return nil, err
	}

	relations := []*Relation{}
	for i := 0; i < len(values); i++ {
		var rel dsc.Relation
		if err := pb.BufToProto(bytes.NewReader(values[i]), &rel); err != nil {
			return nil, err
		}
		relations = append(relations, &Relation{&rel})
	}

	return relations, nil
}

func (sc *StoreContext) GetRelations(param *RelationIdentifier, page *PaginationRequest) ([]*Relation, *PaginationResponse, error) {
	_, values, nextToken, _, err := sc.Store.List(RelationsSubPath(), page.Token, page.Size, sc.Opts)
	if err != nil {
		return nil, &PaginationResponse{}, err
	}

	relations := []*Relation{}
	for i := 0; i < len(values); i++ {
		var relation dsc.Relation
		if err := pb.BufToProto(bytes.NewReader(values[i]), &relation); err != nil {
			return nil, nil, err
		}
		relations = append(relations, &Relation{&relation})
	}

	relations = filterRelations(param, relations)

	return relations, &PaginationResponse{&dsc.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(relations))}}, nil
}

func (sc *StoreContext) SetRelation(rel *Relation) (*Relation, error) {
	sessionID := session.ExtractSessionID(sc.Context)

	sub, err := NewObjectIdentifier(rel.Subject).Resolve(sc)
	if err != nil {
		return &Relation{}, err
	}

	obj, err := NewObjectIdentifier(rel.Object).Resolve(sc)
	if err != nil {
		return &Relation{}, err
	}

	r, err := NewRelationTypeIdentifier(&dsc.RelationTypeIdentifier{
		Name:       &rel.Relation.Relation,
		ObjectType: obj.Type,
	}).Resolve(sc)
	if err != nil {
		return &Relation{}, err
	}

	relation := &Relation{&dsc.Relation{
		Subject:   sub.Msg(),
		Relation:  r.GetName(),
		Object:    obj.Msg(),
		CreatedAt: rel.CreatedAt,
		UpdatedAt: rel.UpdatedAt,
		DeletedAt: rel.DeletedAt,
		Hash:      rel.Hash,
	}}

	if ok, err := relation.Validate(); !ok {
		return &Relation{}, err
	}

	if err := relation.Normalize(); err != nil {
		return &Relation{}, err
	}

	curHash := ""
	current, curErr := sc.getRelation(NewRelationIdentifier(&dsc.RelationIdentifier{
		Subject:  sub.Msg(),
		Relation: r.Msg(),
		Object:   obj.Msg(),
	}))

	if curErr == nil {
		curHash = current.Hash
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		relation.Hash = curHash
	}

	if curHash != relation.Hash {
		return &Relation{}, derr.ErrHashMismatch.Str("current", curHash).Str("incoming", relation.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		relation.CreatedAt = ts
	}
	relation.UpdatedAt = ts

	newHash, _ := relation.GetHash()
	relation.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		relation.CreatedAt = current.CreatedAt
		relation.UpdatedAt = current.UpdatedAt
		return relation, nil
	}

	buf := new(bytes.Buffer)
	if err := pb.ProtoToBuf(buf, relation); err != nil {
		return &Relation{}, err
	}

	if err := sc.Store.Write(RelationsSubPath(), relation.SubjectKey(), buf.Bytes(), sc.Opts); err != nil {
		return &Relation{}, err
	}

	if err := sc.Store.Write(RelationsObjPath(), relation.ObjectKey(), buf.Bytes(), sc.Opts); err != nil {
		return &Relation{}, err
	}

	return relation, nil
}

func (sc *StoreContext) DeleteRelation(relIdentifier *RelationIdentifier) error {
	if ok, err := relIdentifier.Validate(); !ok {
		return err
	}

	current, err := sc.getRelation(relIdentifier)
	switch {
	case errors.Is(err, boltdb.ErrKeyNotFound):
		return nil
	case err != nil:
		return err
	}

	if err := sc.Store.DeleteKey(RelationsSubPath(), current.SubjectKey(), sc.Opts); err != nil {
		return err
	}

	if err := sc.Store.DeleteKey(RelationsObjPath(), current.ObjectKey(), sc.Opts); err != nil {
		return err
	}

	return nil
}
