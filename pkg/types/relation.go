package types

import (
	"bytes"
	"context"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/aserto-dev/edge-ds/pkg/boltdb"
	"github.com/aserto-dev/edge-ds/pkg/pb"
	"github.com/aserto-dev/edge-ds/pkg/session"
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	av2 "github.com/aserto-dev/go-grpc/aserto/api/v2"
	"github.com/aserto-dev/go-utils/cerr"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pkg/errors"
)

type Relation struct {
	*dsc.Relation
}

func NewRelation(i *dsc.Relation) *Relation {
	return &Relation{
		Relation: i,
	}
}

func (i *Relation) Validate() (bool, error) {
	if i.Relation == nil {
		return false, errors.Errorf("relation not instantiated")
	}
	if ok, err := ObjectIdentifier.Validate(i.Object); !ok {
		return false, err
	}
	if ok, err := ObjectIdentifier.Validate(i.Subject); !ok {
		return false, err
	}
	if strings.TrimSpace(i.GetRelation()) == "" {
		return false, cerr.ErrInvalidArgument.Msg("relation cannot be empty")
	}
	return true, nil
}

func (i *Relation) Normalize() error {
	i.Relation.Relation = strings.ToLower(i.Relation.Relation)
	*i.Relation.Subject.Type = strings.ToLower(*i.Relation.Subject.Type)
	*i.Relation.Object.Type = strings.ToLower(*i.Relation.Object.Type)
	return nil
}

func GetRelation(ctx context.Context, i *dsc.RelationIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) (*Relation, error) {
	var (
		subID   string
		subType string

		relName string
		// relID   int32
		objID   string
		objType string
	)

	if ok, _ := ObjectIdentifier.Validate(i.Subject); ok {
		if i.Subject.Id != nil {
			subID = *i.Subject.Id
		} else if i.Subject.Key != nil && i.Subject.Type != nil {
			subKey := *i.Subject.Type + "|" + *i.Subject.Key

			buf, err := store.Read(ObjectsKeyPath(), subKey, opts)
			if err != nil {
				return nil, err
			}
			subID = string(buf)
			// var obj dsc.Object
			// if err := pb.BufToProto(bytes.NewReader(buf), &obj); err != nil {
			// 	return nil, err
			// }
			// subID = obj.GetId()
		} else if i.Subject.Type != nil {
			subType = i.Subject.GetType()
		}
	}
	_ = subType

	if ok, _ := ObjectIdentifier.Validate(i.Object); ok {
		if i.Object.Id != nil {
			objID = *i.Object.Id
		} else if i.Object.Type != nil && i.Object.Key != nil {
			objKey := *i.Object.Type + "|" + *i.Object.Key

			buf, err := store.Read(ObjectsKeyPath(), objKey, opts)
			if err != nil {
				return nil, err
			}
			objID = string(buf)
			// var obj dsc.Object
			// if err := pb.BufToProto(bytes.NewReader(buf), &obj); err != nil {
			// 	return nil, err
			// }
			// objID = obj.GetId()
		} else if i.Object.Type != nil {
			objType = i.Object.GetType()
		}
	}
	_ = objType

	if ok, _ := RelationTypeIdentifier.Validate(i.Relation); ok {
		var relID int32
		if i.Relation.Id != nil && *i.Relation.Id > 0 {
			relID = *i.Relation.Id
		} else {
			key := *i.Relation.ObjectType + "|" + *i.Relation.Name
			idBuf, err := store.Read(RelationTypesNamePath(), key, opts)
			if err != nil {
				return nil, err
			}
			relID = StrToInt32(string(idBuf))
		}

		buf, err := store.Read(RelationTypesPath(), Int32ToStr(relID), opts)
		if err != nil {
			return nil, err
		}
		var relType dsc.RelationType
		if err := pb.BufToProto(bytes.NewReader(buf), &relType); err != nil {
			return nil, err
		}
		relName = relType.Name
	}

	filter := relName + "|" + objID + "|" + subID

	buf, err := store.ReadPrefix(RelationsObjPath(), filter, opts)
	if err != nil {
		return nil, err
	}

	var rel dsc.Relation
	if err := pb.BufToProto(bytes.NewReader(buf), &rel); err != nil {
		return nil, err
	}

	return &Relation{
		Relation: &rel,
	}, nil
}

func GetRelations(ctx context.Context, page *av2.PaginationRequest, store *boltdb.BoltDB, opts ...boltdb.Opts) ([]*Relation, *av2.PaginationResponse, error) {
	_, values, nextToken, _, err := store.List(RelationsSubPath(), page.Token, page.Size, opts)
	if err != nil {
		return nil, &av2.PaginationResponse{}, err
	}

	relations := []*Relation{}
	for i := 0; i < len(values); i++ {
		var relation dsc.Relation
		if err := pb.BufToProto(bytes.NewReader(values[i]), &relation); err != nil {
			return nil, nil, err
		}
		relations = append(relations, &Relation{&relation})
	}

	if err != nil {
		return nil, &av2.PaginationResponse{}, err
	}

	return relations, &av2.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(relations))}, nil
}

func (i *Relation) Set(ctx context.Context, store *boltdb.BoltDB, opts ...boltdb.Opts) error {
	sessionID := session.ExtractSessionID(ctx)

	if ok, err := i.Validate(); !ok {
		return err
	}
	if err := i.Normalize(); err != nil {
		return err
	}

	ri := &dsc.RelationIdentifier{
		Subject: &dsc.ObjectIdentifier{
			Id:   i.Subject.Id,
			Key:  i.Subject.Key,
			Type: i.Subject.Type,
		},
		Relation: &dsc.RelationTypeIdentifier{
			Name:       &i.Relation.Relation,
			ObjectType: i.Object.Type,
		},
		Object: &dsc.ObjectIdentifier{
			Id:   i.Object.Id,
			Key:  i.Object.Key,
			Type: i.Object.Type,
		},
	}
	curHash := ""
	current, err := GetRelation(ctx, ri, store, opts...)
	if err == nil {
		curHash = current.Relation.Hash
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		i.Relation.Hash = curHash
	}

	if curHash != i.Relation.Hash {
		return derr.ErrHashMismatch.Str("current", curHash).Str("incoming", i.Relation.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		i.Relation.CreatedAt = ts
	}
	i.Relation.UpdatedAt = ts

	newHash, _ := i.Hash()
	i.Relation.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		i.Relation.CreatedAt = current.CreatedAt
		i.Relation.UpdatedAt = current.UpdatedAt
		return nil
	}

	buf := new(bytes.Buffer)
	if err := pb.ProtoToBuf(buf, i); err != nil {
		return err
	}

	if err := store.Write(RelationsSubPath(), i.SubjectKey(), buf.Bytes(), opts); err != nil {
		return err
	}

	if err := store.Write(RelationsObjPath(), i.ObjectKey(), buf.Bytes(), opts); err != nil {
		return err
	}

	return nil
}

func DeleteRelation(ctx context.Context, i *dsc.RelationIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) error {
	if ok, err := RelationIdentifier.Validate(i); !ok {
		return err
	}

	current, err := GetRelation(ctx, i, store, opts...)
	switch {
	case errors.Is(err, boltdb.ErrKeyNotFound):
		return nil
	case err != nil:
		return err
	}

	if err := store.DeleteKey(RelationsSubPath(), current.SubjectKey(), opts); err != nil {
		return err
	}

	if err := store.DeleteKey(RelationsObjPath(), current.ObjectKey(), opts); err != nil {
		return err
	}

	return nil
}

func (i *Relation) Msg() *dsc.Relation {
	if i == nil || i.Relation == nil {
		return &dsc.Relation{}
	}
	return i.Relation
}

func (i *Relation) Hash() (string, error) {
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

func (i *Relation) SubjectKey() string {
	return i.Subject.GetId() + "|" + i.GetRelation() + "|" + i.Object.GetId()
}

func (i *Relation) ObjectKey() string {
	return i.Object.GetId() + "|" + i.GetRelation() + "|" + i.Subject.GetId()
}
