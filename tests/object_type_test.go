package tests_test

import (
	"testing"
)

func TestObjectTypes(t *testing.T) {
	tcs := []*TestCase{}

	tcs = append(tcs, objectTypeTestCasesWithID...)
	tcs = append(tcs, objectTypeTestCasesWithoutID...)
	tcs = append(tcs, objectTypeTestCasesStreamMode...)

	testRunner(t, tcs)
}

var objectTypeTestCasesWithID = []*TestCase{
	// {
	// 	Name: "create test-obj_type-1",
	// 	Req: &dsw2.SetObjectTypeRequest{
	// 		ObjectType: &dsc2.ObjectType{
	// 			Name:        "test-obj_type-1",
	// 			DisplayName: "test obj type 1",
	// 			IsSubject:   false,
	// 			Ordinal:     0,
	// 			Status:      uint32(dsc2.Flag_FLAG_UNKNOWN),
	// 			Schema:      pb.NewStruct(),
	// 			Hash:        "",
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsw2.SetObjectTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-obj_type-1", resp.Result.Name)
	// 			assert.Equal(t, "test obj type 1", resp.Result.DisplayName)
	// 			assert.Equal(t, false, resp.Result.IsSubject)
	// 			assert.Equal(t, int32(0), resp.Result.Ordinal)
	// 			assert.Equal(t, uint32(0), resp.Result.Status)
	// 			assert.NotEmpty(t, resp.Result.Hash)
	// 			assert.Equal(t, "15190328005927951280", resp.Result.Hash)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "get test-obj_type-1",
	// 	Req: &dsr2.GetObjectTypeRequest{
	// 		Param: &dsc2.ObjectTypeIdentifier{
	// 			Name: proto.String("test-obj_type-1"),
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsr2.GetObjectTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-obj_type-1", resp.Result.Name)
	// 			// assert.Equal(t, "test obj type 1", resp.Result.DisplayName)
	// 			assert.Equal(t, false, resp.Result.IsSubject)
	// 			assert.Equal(t, int32(0), resp.Result.Ordinal)
	// 			assert.Equal(t, uint32(0), resp.Result.Status)
	// 			// assert.NotEmpty(t, resp.Result.Hash)
	// 			// assert.Equal(t, "15190328005927951280", resp.Result.Hash)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "update test-obj_type-1",
	// 	Req: &dsw2.SetObjectTypeRequest{
	// 		ObjectType: &dsc2.ObjectType{
	// 			Name:        "test-obj_type-1",
	// 			DisplayName: "test obj type 11",
	// 			IsSubject:   false,
	// 			Ordinal:     123,
	// 			Status:      uint32(dsc2.Flag_FLAG_SYSTEM | dsc2.Flag_FLAG_READONLY),
	// 			Schema:      pb.NewStruct(),
	// 			Hash:        "15190328005927951280",
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsw2.SetObjectTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-obj_type-1", resp.Result.Name)
	// 			// assert.Equal(t, "test obj type 11", resp.Result.DisplayName)
	// 			assert.Equal(t, false, resp.Result.IsSubject)
	// 			assert.Equal(t, int32(123), resp.Result.Ordinal)
	// 			assert.Equal(t, uint32(6), resp.Result.Status)
	// 			assert.NotEmpty(t, resp.Result.Hash)
	// 			assert.Equal(t, "5821969820026102548", resp.Result.Hash)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "get updated test-obj_type-1",
	// 	Req: &dsr2.GetObjectTypeRequest{
	// 		Param: &dsc2.ObjectTypeIdentifier{
	// 			Name: proto.String("test-obj_type-1"),
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsr2.GetObjectTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-obj_type-1", resp.Result.Name)
	// 			// assert.Equal(t, "test obj type 11", resp.Result.DisplayName)
	// 			assert.Equal(t, false, resp.Result.IsSubject)
	// 			// assert.Equal(t, int32(123), resp.Result.Ordinal)
	// 			// assert.Equal(t, uint32(6), resp.Result.Status)
	// 			// assert.NotEmpty(t, resp.Result.Hash)
	// 			// assert.Equal(t, "5821969820026102548", resp.Result.Hash)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "delete test-obj_type-1",
	// 	Req: &dsw2.DeleteObjectTypeRequest{
	// 		Param: &dsc2.ObjectTypeIdentifier{
	// 			Name: proto.String("test-obj_type-1"),
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsw2.DeleteObjectTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "get deleted test-obj_type-1",
	// 	Req: &dsr2.GetObjectTypeRequest{
	// 		Param: &dsc2.ObjectTypeIdentifier{
	// 			Name: proto.String("test-obj_type-1"),
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.Nil(t, msg)
	// 		assert.Error(t, tErr)
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "delete deleted test-obj_type-1 by id",
	// 	Req: &dsw2.DeleteObjectTypeRequest{
	// 		Param: &dsc2.ObjectTypeIdentifier{
	// 			Name: proto.String("test-obj_type-1"),
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsw2.DeleteObjectTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
}

var objectTypeTestCasesWithoutID = []*TestCase{
	// {
	// 	Name: "create test-obj_type with no-id",
	// 	Req: &dsw2.SetObjectTypeRequest{
	// 		ObjectType: &dsc2.ObjectType{
	// 			Name:        "test-obj_type",
	// 			DisplayName: "test obj type",
	// 			IsSubject:   false,
	// 			Ordinal:     0,
	// 			Status:      uint32(dsc2.Flag_FLAG_UNKNOWN),
	// 			Schema:      pb.NewStruct(),
	// 			Hash:        "",
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsw2.SetObjectTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-obj_type", resp.Result.Name)
	// 			assert.Equal(t, "test obj type", resp.Result.DisplayName)
	// 			assert.Equal(t, false, resp.Result.IsSubject)
	// 			assert.Equal(t, int32(0), resp.Result.Ordinal)
	// 			assert.Equal(t, uint32(0), resp.Result.Status)
	// 			assert.NotEmpty(t, resp.Result.Hash)
	// 			assert.True(t, len(resp.Result.Hash) > 4)

	// 			return func(req proto.Message) {
	// 				lastHash := resp.Result.Hash

	// 				switch r := req.(type) {
	// 				case *dsw2.SetObjectTypeRequest:
	// 					r.ObjectType.Hash = lastHash
	// 				}
	// 			}
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "update test-obj_type with no-id",
	// 	Req: &dsw2.SetObjectTypeRequest{
	// 		ObjectType: &dsc2.ObjectType{
	// 			Name:        "test-obj_type",
	// 			DisplayName: "test obj type updated",
	// 			IsSubject:   false,
	// 			Ordinal:     9999,
	// 			Status:      uint32(dsc2.Flag_FLAG_READONLY),
	// 			Schema:      pb.NewStruct(),
	// 			Hash:        "",
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsw2.SetObjectTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-obj_type", resp.Result.Name)
	// 			assert.Equal(t, "test obj type updated", resp.Result.DisplayName)
	// 			assert.Equal(t, false, resp.Result.IsSubject)
	// 			assert.Equal(t, int32(9999), resp.Result.Ordinal)
	// 			assert.Equal(t, uint32(dsc2.Flag_FLAG_READONLY), resp.Result.Status)
	// 			assert.NotEmpty(t, resp.Result.Hash)
	// 			assert.True(t, len(resp.Result.Hash) > 4)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
}

var objectTypeTestCasesStreamMode = []*TestCase{}
