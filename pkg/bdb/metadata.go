package bdb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func UpdateMetadata[T any, M Message[T]](ctx context.Context, tx *bolt.Tx, path []string, key string, msg *T) (M, error) {
	// get timestamp once for transaction.
	ts := timestamppb.New(time.Now().UTC())

	// get current instance.
	cur, err := Get[T, M](ctx, tx, path, key)
	switch {
	case errors.Is(err, ErrKeyNotFound):
		// new instance, set created_at timestamp.
		if err := SetFieldProperty(msg, "CreatedAt", ts); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	default:
		// existing instance, propagate created_at timestamp.
		if err := SetFieldProperty(msg, "CreatedAt", cur.GetCreatedAt()); err != nil {
			return nil, err
		}
	}

	// always set updated_at timestamp.
	if err := SetFieldProperty(msg, "UpdatedAt", ts); err != nil {
		return nil, err
	}

	return msg, nil
}

func SetFieldProperty[T any](target *T, fieldName string, value interface{}) error {
	// Get the reflect.Value of the target object.
	targetValue := reflect.ValueOf(target).Elem()

	// Get the reflect.Value of the field we want to set.
	field := targetValue.FieldByName(fieldName)

	// Check if the field exists.
	if !field.IsValid() {
		return fmt.Errorf("field %s does not exist", fieldName) //nolint: goerr113
	}

	// Check if the field is settable.
	if !field.CanSet() {
		return fmt.Errorf("field %s is not settable", fieldName) //nolint: goerr113
	}

	// Get the reflect.Value of the value we want to set.
	valueToSet := reflect.ValueOf(value)

	// Check if the type of the value we want to set is assignable to the type of the field.
	if !valueToSet.Type().AssignableTo(field.Type()) {
		return fmt.Errorf("cannot assign value of type %s to field of type %s", valueToSet.Type(), field.Type()) //nolint: goerr113
	}

	// Set the value of the field.
	field.Set(valueToSet)

	return nil
}
