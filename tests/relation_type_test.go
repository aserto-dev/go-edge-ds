package tests_test

import (
	"testing"
)

func TestRelationTypes(t *testing.T) {
	tcs := []*TestCase{}

	tcs = append(tcs, relationTypeTestCasesWithID...)
	tcs = append(tcs, relationTypeTestCasesWithoutID...)
	tcs = append(tcs, relationTypeTestCasesStreamMode...)

	testRunner(t, tcs)
}

var relationTypeTestCasesWithID = []*TestCase{
	// {
	// 	Name: "create test-rel_type-1",
	// 	Req: &dsw2.SetRelationTypeRequest{
	// 		RelationType: &dsc2.RelationType{
	// 			Name:        "test-rel_type-1",
	// 			DisplayName: "test rel type 1",
	// 			ObjectType:  "user",
	// 			Ordinal:     0,
	// 			Unions:      []string{},
	// 			Permissions: []string{},
	// 			Status:      uint32(dsc2.Flag_FLAG_UNKNOWN),
	// 			Hash:        "",
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsw2.SetRelationTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-rel_type-1", resp.Result.Name)
	// 			assert.Equal(t, "user", resp.Result.ObjectType)
	// 			assert.Len(t, resp.Result.Unions, 0)
	// 			assert.Len(t, resp.Result.Permissions, 0)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "get test-rel_type-1",
	// 	Req: &dsr2.GetRelationTypeRequest{
	// 		Param: &dsc2.RelationTypeIdentifier{
	// 			Name:       proto.String("test-rel_type-1"),
	// 			ObjectType: proto.String("user"),
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsr2.GetRelationTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-rel_type-1", resp.Result.Name)
	// 			assert.Equal(t, "user", resp.Result.ObjectType)
	// 			assert.Len(t, resp.Result.Unions, 0)
	// 			assert.Len(t, resp.Result.Permissions, 0)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "update test-rel_type-1",
	// 	Req: &dsw2.SetRelationTypeRequest{
	// 		RelationType: &dsc2.RelationType{
	// 			Name:        "test-rel_type-1",
	// 			DisplayName: "test rel type 11",
	// 			ObjectType:  "user",
	// 			Ordinal:     321,
	// 			Status:      uint32(dsc2.Flag_FLAG_UNKNOWN),
	// 			Unions:      []string{},
	// 			Permissions: []string{},
	// 			Hash:        "6601616304534273683",
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsw2.SetRelationTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-rel_type-1", resp.Result.Name)
	// 			assert.Equal(t, "user", resp.Result.ObjectType)
	// 			assert.Len(t, resp.Result.Unions, 0)
	// 			assert.Len(t, resp.Result.Permissions, 0)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "get updated test-rel_type-1",
	// 	Req: &dsr2.GetRelationTypeRequest{
	// 		Param: &dsc2.RelationTypeIdentifier{
	// 			Name:       proto.String("test-rel_type-1"),
	// 			ObjectType: proto.String("user"),
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsr2.GetRelationTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-rel_type-1", resp.Result.Name)
	// 			assert.Equal(t, "user", resp.Result.ObjectType)
	// 			assert.Len(t, resp.Result.Unions, 0)
	// 			assert.Len(t, resp.Result.Permissions, 0)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "delete test-rel_type-1",
	// 	Req: &dsw2.DeleteRelationTypeRequest{
	// 		Param: &dsc2.RelationTypeIdentifier{
	// 			Name:       proto.String("test-rel_type-1"),
	// 			ObjectType: proto.String("user"),
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsw2.DeleteRelationTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "get deleted test-rel_type-1",
	// 	Req: &dsr2.GetRelationTypeRequest{
	// 		Param: &dsc2.RelationTypeIdentifier{
	// 			Name:       proto.String("test-rel_type-1"),
	// 			ObjectType: proto.String("user"),
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		assert.Nil(t, msg)
	// 		assert.Error(t, tErr)
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "delete deleted test-rel_type-1 by id",
	// 	Req: &dsw2.DeleteRelationTypeRequest{
	// 		Param: &dsc2.RelationTypeIdentifier{
	// 			Name:       proto.String("test-rel_type-1"),
	// 			ObjectType: proto.String("user"),
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsw2.DeleteRelationTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
}

var relationTypeTestCasesWithoutID = []*TestCase{
	// {
	// 	Name: "create test-rel_type with no id",
	// 	Req: &dsw2.SetRelationTypeRequest{
	// 		RelationType: &dsc2.RelationType{
	// 			Name:        "test-rel_type",
	// 			DisplayName: "test rel type",
	// 			ObjectType:  "user",
	// 			Ordinal:     0,
	// 			Unions:      []string{},
	// 			Permissions: []string{},
	// 			Status:      uint32(dsc2.Flag_FLAG_UNKNOWN),
	// 			Hash:        "",
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsw2.SetRelationTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-rel_type", resp.Result.Name)
	// 			assert.Equal(t, "user", resp.Result.ObjectType)
	// 			assert.Len(t, resp.Result.Unions, 0)
	// 			assert.Len(t, resp.Result.Permissions, 0)

	// 			return func(req proto.Message) {
	// 				lastHash := resp.Result.Hash

	// 				switch r := req.(type) {
	// 				case *dsw2.SetRelationTypeRequest:
	// 					r.RelationType.Hash = lastHash
	// 				}
	// 			}
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "update test-rel_type with no id",
	// 	Req: &dsw2.SetRelationTypeRequest{
	// 		RelationType: &dsc2.RelationType{
	// 			Name:        "test-rel_type",
	// 			DisplayName: "test rel type NO-ID",
	// 			ObjectType:  "user",
	// 			Ordinal:     0,
	// 			Unions:      []string{},
	// 			Permissions: []string{},
	// 			Status:      uint32(dsc2.Flag_FLAG_UNKNOWN),
	// 			Hash:        "",
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsw2.SetRelationTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-rel_type", resp.Result.Name)
	// 			assert.Equal(t, "user", resp.Result.ObjectType)
	// 			assert.Len(t, resp.Result.Unions, 0)
	// 			assert.Len(t, resp.Result.Permissions, 0)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
	// {
	// 	Name: "get test-rel_type with no id",
	// 	Req: &dsr2.GetRelationTypeRequest{
	// 		Param: &dsc2.RelationTypeIdentifier{
	// 			Name:       proto.String("test-rel_type"),
	// 			ObjectType: proto.String("user"),
	// 		},
	// 	},
	// 	Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
	// 		require.NotNil(t, msg)
	// 		switch resp := msg.(type) {
	// 		case *dsr2.GetRelationTypeResponse:
	// 			assert.NoError(t, tErr)
	// 			assert.NotNil(t, resp)
	// 			assert.NotNil(t, resp.Result)
	// 			t.Logf("resp hash:%s", resp.Result.Hash)

	// 			assert.Equal(t, "test-rel_type", resp.Result.Name)
	// 			assert.Equal(t, "user", resp.Result.ObjectType)
	// 			assert.Len(t, resp.Result.Unions, 0)
	// 			assert.Len(t, resp.Result.Permissions, 0)
	// 		}
	// 		return func(proto.Message) {}
	// 	},
	// },
}

var relationTypeTestCasesStreamMode = []*TestCase{}
