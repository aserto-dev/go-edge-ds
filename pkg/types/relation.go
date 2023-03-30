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
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	"github.com/aserto-dev/go-edge-ds/pkg/session"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type relation struct {
	*dsc.Relation
}

func Relation(i *dsc.Relation) *relation { return &relation{i} }

func (i *relation) Validate() (bool, error) {
	if i.Relation == nil {
		return false, derr.ErrInvalidRelation
	}

	if ok, err := ObjectIdentifier(i.Subject).Validate(); !ok {
		return false, err
	}

	if ok, err := ObjectIdentifier(i.Object).Validate(); !ok {
		return false, err
	}

	if strings.TrimSpace(i.GetRelation()) == "" {
		return false, derr.ErrInvalidArgument.Msg("relation not set")
	}

	return true, nil
}

func (i *relation) Normalize() error {
	i.Relation.Relation = strings.ToLower(i.Relation.GetRelation())
	*i.Relation.Subject.Type = strings.ToLower(i.Relation.Subject.GetType())
	*i.Relation.Object.Type = strings.ToLower(i.Relation.Object.GetType())
	return nil
}

func (i *relation) GetHash() (string, error) {
	h := fnv.New64a()
	h.Reset()

	if i != nil && i.Relation != nil {
		if i.Relation.Subject != nil {
			if _, err := h.Write([]byte(i.Relation.Subject.GetKey())); err != nil {
				return DefaultHash, err
			}
			if _, err := h.Write([]byte(i.Relation.Subject.GetType())); err != nil {
				return DefaultHash, err
			}
		}
		if _, err := h.Write([]byte(i.Relation.Relation)); err != nil {
			return DefaultHash, err
		}
		if i.Relation.Object != nil {
			if _, err := h.Write([]byte(i.Relation.Object.GetKey())); err != nil {
				return DefaultHash, err
			}
			if _, err := h.Write([]byte(i.Relation.Object.GetType())); err != nil {
				return DefaultHash, err
			}
		}
	}

	return strconv.FormatUint(h.Sum64(), 10), nil
}

func (i *relation) Key() string {
	return i.Object.GetType() + ":" + i.GetRelation()
}

// TODO: validate, check if SubjectKey is correct.
func (i *relation) SubjectKey() string {
	return i.Subject.GetKey() + "|" + i.Key() + "|" + i.Object.GetKey()
}

// TODO: validate, check if ObjectKey is correct.
func (i *relation) ObjectKey() string {
	return i.Object.GetKey() + "|" + i.Key() + "|" + i.Subject.GetKey()
}

func (sc *StoreContext) getRelation(relationIdentifier *dsc.RelationIdentifier) (*dsc.Relation, error) {
	ri, err := RelationIdentifier(relationIdentifier).Resolve(sc)
	if err != nil {
		return &dsc.Relation{}, err
	}

	buf, err := sc.Store.Read(RelationsObjPath(), RelationIdentifier(ri).ObjKey(), sc.Opts)
	if err != nil {
		return nil, err
	}

	var rel dsc.Relation
	if err := pb.BufToProto(bytes.NewReader(buf), &rel); err != nil {
		return nil, err
	}

	return &rel, nil
}

// nolint: gocyclo
func (sc *StoreContext) GetRelation(relationIdentifier *dsc.RelationIdentifier) ([]*dsc.Relation, error) {
	resp := []*dsc.Relation{}

	// TODO: if object type is concrete, check existence
	// obj, err := ds.ObjectIdentifier(relationIdentifier.Object).Resolve(sc.Store)
	// if err != nil {
	// 	return resp, err
	// }

	// TODO: if object type is concrete, check existence
	// subj, err := ds.ObjectIdentifier(relationIdentifier.Subject).Resolve(sc.Store)
	// if err != nil {
	// 	return resp, err
	// }

	path, filter, err := ds.RelationIdentifier(relationIdentifier).PathAndFilter()
	if err != nil {
		return resp, err
	}

	_, values, err := sc.Store.ReadScan(path, filter, sc.Opts)
	if err != nil {
		return nil, err
	}

	relations := []*dsc.Relation{}
	for i := 0; i < len(values); i++ {
		var rel dsc.Relation
		if err := pb.BufToProto(bytes.NewReader(values[i]), &rel); err != nil {
			return nil, err
		}
		relations = append(relations, &rel)
	}

	return relations, nil
}

func (sc *StoreContext) GetRelations(param *dsc.RelationIdentifier, page *dsc.PaginationRequest) ([]*dsc.Relation, *dsc.PaginationResponse, error) {
	_, values, nextToken, _, err := sc.Store.List(RelationsSubPath(), page.Token, page.Size, sc.Opts)
	if err != nil {
		return nil, &dsc.PaginationResponse{}, err
	}

	relations := []*dsc.Relation{}
	for i := 0; i < len(values); i++ {
		var relation dsc.Relation
		if err := pb.BufToProto(bytes.NewReader(values[i]), &relation); err != nil {
			return nil, nil, err
		}
		relations = append(relations, &relation)
	}

	relations = filterRelations(param, relations)

	return relations, &dsc.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(relations))}, nil
}

func (sc *StoreContext) SetRelation(rel *dsc.Relation) (*dsc.Relation, error) {
	sessionID := session.ExtractSessionID(sc.Context)

	sub, err := ObjectIdentifier(rel.Subject).Resolve(sc)
	if err != nil {
		return &dsc.Relation{}, err
	}

	obj, err := ObjectIdentifier(rel.Object).Resolve(sc)
	if err != nil {
		return &dsc.Relation{}, err
	}

	r, err := RelationTypeIdentifier(&dsc.RelationTypeIdentifier{
		Name:       &rel.Relation,
		ObjectType: obj.Type,
	}).Resolve(sc)
	if err != nil {
		return &dsc.Relation{}, err
	}

	relation := &dsc.Relation{
		Subject:   sub,
		Relation:  r.GetName(),
		Object:    obj,
		CreatedAt: rel.CreatedAt,
		UpdatedAt: rel.UpdatedAt,
		Hash:      rel.Hash,
	}

	if ok, err := Relation(relation).Validate(); !ok {
		return &dsc.Relation{}, err
	}

	if err := Relation(relation).Normalize(); err != nil {
		return &dsc.Relation{}, err
	}

	curHash := ""
	current, curErr := sc.getRelation(&dsc.RelationIdentifier{
		Subject:  sub,
		Relation: r,
		Object:   obj,
	})

	if curErr == nil {
		curHash = current.Hash
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		relation.Hash = curHash
	}

	if curHash != relation.Hash {
		return &dsc.Relation{}, derr.ErrHashMismatch.Str("current", curHash).Str("incoming", relation.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		relation.CreatedAt = ts
	}
	relation.UpdatedAt = ts

	newHash, _ := Relation(relation).GetHash()
	relation.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		relation.CreatedAt = current.CreatedAt
		relation.UpdatedAt = current.UpdatedAt
		return relation, nil
	}

	buf := new(bytes.Buffer)
	if err := pb.ProtoToBuf(buf, relation); err != nil {
		return &dsc.Relation{}, err
	}

	if err := sc.Store.Write(RelationsSubPath(), Relation(relation).SubjectKey(), buf.Bytes(), sc.Opts); err != nil {
		return &dsc.Relation{}, err
	}

	if err := sc.Store.Write(RelationsObjPath(), Relation(relation).ObjectKey(), buf.Bytes(), sc.Opts); err != nil {
		return &dsc.Relation{}, err
	}

	return relation, nil
}

func (sc *StoreContext) DeleteRelation(relIdentifier *dsc.RelationIdentifier) error {
	if ok, err := RelationIdentifier(relIdentifier).Validate(); !ok {
		return err
	}

	current, err := sc.getRelation(relIdentifier)
	switch {
	case errors.Is(err, boltdb.ErrKeyNotFound):
		return nil
	case err != nil:
		return err
	}

	if err := sc.Store.DeleteKey(RelationsSubPath(), Relation(current).SubjectKey(), sc.Opts); err != nil {
		return err
	}

	if err := sc.Store.DeleteKey(RelationsObjPath(), Relation(current).ObjectKey(), sc.Opts); err != nil {
		return err
	}

	return nil
}
