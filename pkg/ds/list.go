package ds

import (
	"bytes"
	"context"
	"fmt"
	"reflect"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

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

func Get[T DirectoryType](ctx context.Context, tx *bolt.Tx, path []string, key string, t T) (T, error) {
	buf, err := boltdb.GetKey(tx, path, key)
	if err != nil {
		return nil, err
	}

	if err := pb.BufToProto(bytes.NewReader(buf), any(t).(proto.Message)); err != nil {
		return nil, err
	}

	return t, nil
}

func Set[T DirectoryType](ctx context.Context, tx *bolt.Tx, path []string, key string, t T) (T, error) {
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

// Marshal msg to buffer.
func Marshal[T DirectoryType](t T) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := pb.ProtoToBuf(buf, any(t).(proto.Message)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal, buffer to msg.
func Unmarshal[T DirectoryType](b []byte, t T) (T, error) {
	err := pb.BufToProto(bytes.NewReader(b), any(t).(proto.Message))
	if err != nil {
		return nil, err
	}
	return t, nil
}

func SetFieldProperty[T any](target *T, fieldName string, value interface{}) error {
	// Get the reflect.Value of the target object
	targetValue := reflect.ValueOf(target).Elem()

	// Get the reflect.Value of the field we want to set
	field := targetValue.FieldByName(fieldName)

	// Check if the field exists
	if !field.IsValid() {
		return fmt.Errorf("field %s does not exist", fieldName)
	}

	// Check if the field is settable
	if !field.CanSet() {
		return fmt.Errorf("field %s is not settable", fieldName)
	}

	// Get the reflect.Value of the value we want to set
	valueToSet := reflect.ValueOf(value)

	// Check if the type of the value we want to set is assignable to the type of the field
	if !valueToSet.Type().AssignableTo(field.Type()) {
		return fmt.Errorf("cannot assign value of type %s to field of type %s", valueToSet.Type(), field.Type())
	}

	// Set the value of the field
	field.Set(valueToSet)

	return nil
}
