package tests_test

import (
	"testing"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"

	"github.com/aserto-dev/go-directory/pkg/pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestObjectTypes(t *testing.T) {
	tcs := []*TestCase{}

	tcs = append(tcs, objectTypeTestCasesWithID...)
	tcs = append(tcs, objectTypeTestCasesWithoutID...)
	tcs = append(tcs, objectTypeTestCasesStreamMode...)

	testRunner(t, tcs)
}

var objectTypeTestCasesWithID = []*TestCase{
	{
		Name: "create test-obj_type-1",
		Req: &dsw.SetObjectTypeRequest{
			ObjectType: &dsc.ObjectType{
				Name:        "test-obj_type-1",
				DisplayName: "test obj type 1",
				IsSubject:   false,
				Ordinal:     0,
				Status:      uint32(dsc.Flag_FLAG_UNKNOWN),
				Schema:      pb.NewStruct(),
				Hash:        "",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetObjectTypeResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "test-obj_type-1", resp.Result.Name)
				assert.Equal(t, "test obj type 1", resp.Result.DisplayName)
				assert.Equal(t, false, resp.Result.IsSubject)
				assert.Equal(t, int32(0), resp.Result.Ordinal)
				assert.Equal(t, uint32(0), resp.Result.Status)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "2445254762944799642", resp.Result.Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "get test-obj_type-1",
		Req: &dsr.GetObjectTypeRequest{
			Param: &dsc.ObjectTypeIdentifier{
				Name: proto.String("test-obj_type-1"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetObjectTypeResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "test-obj_type-1", resp.Result.Name)
				assert.Equal(t, "test obj type 1", resp.Result.DisplayName)
				assert.Equal(t, false, resp.Result.IsSubject)
				assert.Equal(t, int32(0), resp.Result.Ordinal)
				assert.Equal(t, uint32(0), resp.Result.Status)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "2445254762944799642", resp.Result.Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "update test-obj_type-1",
		Req: &dsw.SetObjectTypeRequest{
			ObjectType: &dsc.ObjectType{
				Name:        "test-obj_type-1",
				DisplayName: "test obj type 11",
				IsSubject:   false,
				Ordinal:     123,
				Status:      uint32(dsc.Flag_FLAG_SYSTEM | dsc.Flag_FLAG_READONLY),
				Schema:      pb.NewStruct(),
				Hash:        "2445254762944799642",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetObjectTypeResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "test-obj_type-1", resp.Result.Name)
				assert.Equal(t, "test obj type 11", resp.Result.DisplayName)
				assert.Equal(t, false, resp.Result.IsSubject)
				assert.Equal(t, int32(123), resp.Result.Ordinal)
				assert.Equal(t, uint32(6), resp.Result.Status)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "8646926669383729786", resp.Result.Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "get updated test-obj_type-1",
		Req: &dsr.GetObjectTypeRequest{
			Param: &dsc.ObjectTypeIdentifier{
				Name: proto.String("test-obj_type-1"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetObjectTypeResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "test-obj_type-1", resp.Result.Name)
				assert.Equal(t, "test obj type 11", resp.Result.DisplayName)
				assert.Equal(t, false, resp.Result.IsSubject)
				assert.Equal(t, int32(123), resp.Result.Ordinal)
				assert.Equal(t, uint32(6), resp.Result.Status)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "8646926669383729786", resp.Result.Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "delete test-obj_type-1",
		Req: &dsw.DeleteObjectTypeRequest{
			Param: &dsc.ObjectTypeIdentifier{
				Name: proto.String("test-obj_type-1"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.DeleteObjectTypeResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "get deleted test-obj_type-1",
		Req: &dsr.GetObjectTypeRequest{
			Param: &dsc.ObjectTypeIdentifier{
				Name: proto.String("test-obj_type-1"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.Nil(t, msg)
			assert.Error(t, tErr)
			return func(proto.Message) {}
		},
	},
	{
		Name: "delete deleted test-obj_type-1 by id",
		Req: &dsw.DeleteObjectTypeRequest{
			Param: &dsc.ObjectTypeIdentifier{
				Name: proto.String("test-obj_type-1"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.DeleteObjectTypeResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
			}
			return func(proto.Message) {}
		},
	},
}

var objectTypeTestCasesWithoutID = []*TestCase{
	{
		Name: "create test-obj_type with no-id",
		Req: &dsw.SetObjectTypeRequest{
			ObjectType: &dsc.ObjectType{
				Name:        "test-obj_type",
				DisplayName: "test obj type",
				IsSubject:   false,
				Ordinal:     0,
				Status:      uint32(dsc.Flag_FLAG_UNKNOWN),
				Schema:      pb.NewStruct(),
				Hash:        "",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetObjectTypeResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "test-obj_type", resp.Result.Name)
				assert.Equal(t, "test obj type", resp.Result.DisplayName)
				assert.Equal(t, false, resp.Result.IsSubject)
				assert.Equal(t, int32(0), resp.Result.Ordinal)
				assert.Equal(t, uint32(0), resp.Result.Status)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.True(t, len(resp.Result.Hash) > 4)

				return func(req proto.Message) {
					lastHash := resp.Result.Hash

					switch r := req.(type) {
					case *dsw.SetObjectTypeRequest:
						r.ObjectType.Hash = lastHash
					}
				}
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "update test-obj_type with no-id",
		Req: &dsw.SetObjectTypeRequest{
			ObjectType: &dsc.ObjectType{
				Name:        "test-obj_type",
				DisplayName: "test obj type updated",
				IsSubject:   false,
				Ordinal:     9999,
				Status:      uint32(dsc.Flag_FLAG_READONLY),
				Schema:      pb.NewStruct(),
				Hash:        "",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetObjectTypeResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "test-obj_type", resp.Result.Name)
				assert.Equal(t, "test obj type updated", resp.Result.DisplayName)
				assert.Equal(t, false, resp.Result.IsSubject)
				assert.Equal(t, int32(9999), resp.Result.Ordinal)
				assert.Equal(t, uint32(dsc.Flag_FLAG_READONLY), resp.Result.Status)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.True(t, len(resp.Result.Hash) > 4)
			}
			return func(proto.Message) {}
		},
	},
}

var objectTypeTestCasesStreamMode = []*TestCase{}
