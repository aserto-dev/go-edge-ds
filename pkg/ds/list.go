package ds

import (
	"bytes"
	"context"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Message[T any] interface {
	proto.Message
	*T
	GetCreatedAt() *timestamppb.Timestamp
	GetUpdatedAt() *timestamppb.Timestamp
	GetHash() string
}

type DirectoryType interface {
	*dsc.Object | *dsc.Relation | *dsc.ObjectType | *dsc.RelationType | *dsc.Permission
	proto.Message
}

type IdentifierType interface {
	*dsc.ObjectIdentifier | *dsc.RelationIdentifier | *dsc.ObjectTypeIdentifier | *dsc.RelationTypeIdentifier | *dsc.PermissionIdentifier
	proto.Message
}

func List[T DirectoryType](ctx context.Context, tx *bolt.Tx, path []string, t T, page *dsc.PaginationRequest) ([]T, *dsc.PaginationResponse, error) {
	iter, err := boltdb.List(tx, path, page.Token)
	if err != nil {
		return []T{}, &dsc.PaginationResponse{}, err
	}

	var results []T

	for iter.Next() {
		v := iter.Value()
		if err := pb.BufToProto(bytes.NewReader(v), any(t).(proto.Message)); err != nil {
			return []T{}, &dsc.PaginationResponse{}, err
		}

		result := proto.Clone(t)
		results = append(results, result.(T))

		if len(results) == int(page.Size) {
			break
		}
	}

	pageResp := &dsc.PaginationResponse{
		ResultSize: int32(len(results)),
		NextToken:  "",
	}

	// get NextToken value.
	if iter.Next() {
		pageResp.NextToken = string(iter.Key())
	}

	return results, pageResp, nil
}

func Get[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path []string, key string) (M, error) {
	buf, err := boltdb.GetKey(tx, path, key)
	if err != nil {
		return nil, err
	}

	return Unmarshal[T, M](buf)
}

func Set[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path []string, key string, t M) (M, error) {
	buf, err := Marshal(t)
	if err != nil {
		return nil, err
	}

	if err := boltdb.SetKey(tx, path, key, buf); err != nil {
		return nil, err
	}

	return t, nil
}

func Delete(ctx context.Context, tx *bolt.Tx, path []string, key string) error {
	return boltdb.DeleteKey(tx, path, key)
}

func Marshal[T any, M Message[T]](t M) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := pb.ProtoToBuf(buf, any(t).(proto.Message)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Unmarshal[T any, M Message[T]](b []byte) (M, error) {
	var t T
	if err := pb.BufToProto(bytes.NewReader(b), any(&t).(proto.Message)); err != nil {
		return nil, err
	}
	return &t, nil
}
