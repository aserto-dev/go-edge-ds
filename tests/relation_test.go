package tests_test

import (
	"testing"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsw2 "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"

	"github.com/aserto-dev/go-directory/pkg/pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestRelations(t *testing.T) {
	tcs := []*TestCase{}

	tcs = append(tcs, relationTestCasesWithID...)
	tcs = append(tcs, relationTestCasesWithoutID...)
	tcs = append(tcs, relationTestCasesStreamMode...)

	testRunner(t, tcs)
}

var relationTestCasesWithID = []*TestCase{
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

// no user cases for setting relation without an ID, as a relation specifies all key values regardlessly.
var relationTestCasesWithoutID = []*TestCase{}

var relationTestCasesStreamMode = []*TestCase{}
