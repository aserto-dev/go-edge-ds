package pb

import "google.golang.org/protobuf/types/known/structpb"

// NewStruct, returns *structpb.Struct instance with initialized Fields map.
func NewStruct() *structpb.Struct {
	return &structpb.Struct{Fields: map[string]*structpb.Value{}}
}
