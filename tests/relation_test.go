package tests_test

import (
	"testing"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	dsw2 "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	dsw3 "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"

	"github.com/aserto-dev/go-directory/pkg/pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestRelations(t *testing.T) {
	tcs := []*TestCase{}

	tcs = append(tcs, relationTestCasesV3...)
	tcs = append(tcs, relationTestCasesV2...)
	tcs = append(tcs, relationTestCasesStreamMode...)

	testRunner(t, tcs)
}

var relationTestCasesV3 = []*TestCase{
	{
		Name: "create nested groups",
		Req: &dsw3.SetRelationRequest{
			Relation: &dsc3.Relation{
				ObjectType:      "group",
				ObjectId:        "parent-group",
				Relation:        "member",
				SubjectType:     "group",
				SubjectId:       "child-group",
				SubjectRelation: "member",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NoError(t, tErr)
			switch resp := msg.(type) {
			case *dsw3.SetRelationResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.Equal(t, "group", resp.Result.ObjectType)
				assert.Equal(t, "parent-group", resp.Result.ObjectId)
				assert.Equal(t, "member", resp.Result.Relation)
				assert.Equal(t, "group", resp.Result.SubjectType)
				assert.Equal(t, "child-group", resp.Result.SubjectId)
				assert.Equal(t, "member", resp.Result.SubjectRelation)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "add user to parent group",
		Req: &dsw3.SetRelationRequest{
			Relation: &dsc3.Relation{
				ObjectType:  "group",
				ObjectId:    "parent-group",
				Relation:    "member",
				SubjectType: "user",
				SubjectId:   "test-user-1@acmecorp.com",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NoError(t, tErr)
			switch resp := msg.(type) {
			case *dsw3.SetRelationResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.Equal(t, "group", resp.Result.ObjectType)
				assert.Equal(t, "parent-group", resp.Result.ObjectId)
				assert.Equal(t, "member", resp.Result.Relation)
				assert.Equal(t, "user", resp.Result.SubjectType)
				assert.Equal(t, "test-user-1@acmecorp.com", resp.Result.SubjectId)
				assert.Empty(t, resp.Result.SubjectRelation)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "list all members of parent group",
		Req: &dsr3.GetRelationsRequest{
			ObjectType: "group",
			ObjectId:   "parent-group",
			Relation:   "member",
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NoError(t, tErr)
			switch resp := msg.(type) {
			case *dsr3.GetRelationsResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.Len(t, resp.Results, 2)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "list member relations of parent group excluding subject relation",
		Req: &dsr3.GetRelationsRequest{
			ObjectType:               "group",
			ObjectId:                 "parent-group",
			Relation:                 "member",
			WithEmptySubjectRelation: true,
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NoError(t, tErr)
			switch resp := msg.(type) {
			case *dsr3.GetRelationsResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.Len(t, resp.Results, 1)

				assert.Equal(t, "user", resp.Results[0].SubjectType)
			}
			return func(proto.Message) {}
		},
	},
}

var relationTestCasesV2 = []*TestCase{
	{
		Name: "create test-user-1",
		Req: &dsw2.SetObjectRequest{
			Object: &dsc2.Object{
				Type:        "user",
				Key:         "test-user-1@acmecorp.com",
				DisplayName: "test user 1",
				Properties:  pb.NewStruct(),
				Hash:        "",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw2.SetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "create test-user-2",
		Req: &dsw2.SetObjectRequest{
			Object: &dsc2.Object{
				Type:        "user",
				Key:         "test-user-2@acmecorp.com",
				DisplayName: "test user 2",
				Properties:  pb.NewStruct(),
				Hash:        "",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw2.SetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "create test-rel-1",
		Req: &dsw2.SetRelationRequest{
			Relation: &dsc2.Relation{
				Subject: &dsc2.ObjectIdentifier{
					Type: proto.String("user"),
					Key:  proto.String("test-user-1@acmecorp.com"),
				},
				Relation: "manager",
				Object: &dsc2.ObjectIdentifier{
					Type: proto.String("user"),
					Key:  proto.String("test-user-2@acmecorp.com"),
				},
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NoError(t, tErr)
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw2.SetRelationResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)

				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "user", resp.Result.Subject.GetType())
				assert.Equal(t, "test-user-1@acmecorp.com", resp.Result.Subject.GetKey())

				assert.Equal(t, "manager", resp.Result.Relation)

				assert.Equal(t, "user", resp.Result.Object.GetType())
				assert.Equal(t, "test-user-2@acmecorp.com", resp.Result.Object.GetKey())

				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "2966454309474781929", resp.Result.Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "get test-rel-1",
		Req: &dsr2.GetRelationRequest{
			Param: &dsc2.RelationIdentifier{
				Subject: &dsc2.ObjectIdentifier{
					Type: proto.String("user"),
					Key:  proto.String("test-user-1@acmecorp.com"),
				},
				Relation: &dsc2.RelationTypeIdentifier{
					Name: proto.String("manager"),
				},
				Object: &dsc2.ObjectIdentifier{
					Type: proto.String("user"),
					Key:  proto.String("test-user-2@acmecorp.com"),
				},
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr2.GetRelationResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)

				assert.NotNil(t, resp.Results)
				require.Len(t, resp.Results, 1)

				t.Logf("resp hash:%s", resp.Results[0].Hash)

				assert.Equal(t, "user", resp.Results[0].Subject.GetType())
				assert.Equal(t, "test-user-1@acmecorp.com", resp.Results[0].Subject.GetKey())

				assert.Equal(t, "manager", resp.Results[0].Relation)

				assert.Equal(t, "user", resp.Results[0].Object.GetType())
				assert.Equal(t, "test-user-2@acmecorp.com", resp.Results[0].Object.GetKey())

				assert.NotEmpty(t, resp.Results[0].Hash)
				assert.Equal(t, "2966454309474781929", resp.Results[0].Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "delete test-rel-1",
		Req: &dsw2.DeleteRelationRequest{
			Param: &dsc2.RelationIdentifier{
				Subject: &dsc2.ObjectIdentifier{
					Type: proto.String("user"),
					Key:  proto.String("test-user-1@acmecorp.com"),
				},
				Relation: &dsc2.RelationTypeIdentifier{
					Name: proto.String("manager"),
				},
				Object: &dsc2.ObjectIdentifier{
					Type: proto.String("user"),
					Key:  proto.String("test-user-2@acmecorp.com"),
				},
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw2.DeleteRelationResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "get deleted test-rel-1",
		Req: &dsr2.GetRelationRequest{
			Param: &dsc2.RelationIdentifier{
				Subject: &dsc2.ObjectIdentifier{
					Type: proto.String("user"),
					Key:  proto.String("test-user-1@acmecorp.com"),
				},
				Relation: &dsc2.RelationTypeIdentifier{
					Name: proto.String("manager"),
				},
				Object: &dsc2.ObjectIdentifier{
					Type: proto.String("user"),
					Key:  proto.String("test-user-2@acmecorp.com"),
				},
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			assert.Error(t, tErr)
			assert.Contains(t, tErr.Error(), "key not found")
			assert.Nil(t, msg)
			return func(req proto.Message) {}
		},
	},
	{
		Name: "delete deleted test-rel-1",
		Req: &dsw2.DeleteRelationRequest{
			Param: &dsc2.RelationIdentifier{
				Subject: &dsc2.ObjectIdentifier{
					Type: proto.String("user"),
					Key:  proto.String("test-user-1@acmecorp.com"),
				},
				Relation: &dsc2.RelationTypeIdentifier{
					Name: proto.String("manager"),
				},
				Object: &dsc2.ObjectIdentifier{
					Type: proto.String("user"),
					Key:  proto.String("test-user-2@acmecorp.com"),
				},
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw2.DeleteRelationResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
			}
			return func(proto.Message) {}
		},
	},
}

var relationTestCasesStreamMode = []*TestCase{}
