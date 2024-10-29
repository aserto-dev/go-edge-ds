package tests_test

import (
	"testing"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	dsw3 "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestRelations(t *testing.T) {
	tcs := []*TestCase{}

	tcs = append(tcs, relationTestCasesV3...)
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

var relationTestCasesStreamMode = []*TestCase{}
