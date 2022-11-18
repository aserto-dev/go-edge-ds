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
		Req: &dsw.SetObjectRequest{
			Object: &dsc.Object{
				Id:          "ad20c049-ea32-42b3-9ced-c4037a330261",
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
			case *dsw.SetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "create test-user-2",
		Req: &dsw.SetObjectRequest{
			Object: &dsc.Object{
				Id:          "bd20c049-ea32-42b3-9ced-c4037a330262",
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
			case *dsw.SetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "create test-rel-1",
		Req: &dsw.SetRelationRequest{
			Relation: &dsc.Relation{
				Subject: &dsc.ObjectIdentifier{
					Type: proto.String("user"),
					Id:   proto.String("bd20c049-ea32-42b3-9ced-c4037a330262"),
				},
				Relation: "manager",
				Object: &dsc.ObjectIdentifier{
					Type: proto.String("user"),
					Id:   proto.String("ad20c049-ea32-42b3-9ced-c4037a330261"),
				},
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetRelationResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)

				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "user", resp.Result.Object.GetType())
				assert.Equal(t, "ad20c049-ea32-42b3-9ced-c4037a330261", resp.Result.Object.GetId())
				assert.Equal(t, "manager", resp.Result.Relation)
				assert.Equal(t, "user", resp.Result.Subject.GetType())
				assert.Equal(t, "bd20c049-ea32-42b3-9ced-c4037a330262", resp.Result.Subject.GetId())
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "14268186579071905229", resp.Result.Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "get test-rel-1",
		Req: &dsr.GetRelationRequest{
			Param: &dsc.RelationIdentifier{
				Subject: &dsc.ObjectIdentifier{
					Type: proto.String("user"),
					Id:   proto.String("bd20c049-ea32-42b3-9ced-c4037a330262"),
				},
				Relation: &dsc.RelationTypeIdentifier{
					Name:       proto.String("manager"),
					ObjectType: proto.String("user"),
				},
				Object: &dsc.ObjectIdentifier{
					Type: proto.String("user"),
					Id:   proto.String("ad20c049-ea32-42b3-9ced-c4037a330261"),
				},
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetRelationResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)

				assert.NotNil(t, resp.Results)
				require.Len(t, resp.Results, 1)

				t.Logf("resp hash:%s", resp.Results[0].Hash)

				assert.Equal(t, "user", resp.Results[0].Object.GetType())
				assert.Equal(t, "ad20c049-ea32-42b3-9ced-c4037a330261", resp.Results[0].Object.GetId())
				assert.Equal(t, "manager", resp.Results[0].Relation)
				assert.Equal(t, "user", resp.Results[0].Subject.GetType())
				assert.Equal(t, "bd20c049-ea32-42b3-9ced-c4037a330262", resp.Results[0].Subject.GetId())

				assert.NotEmpty(t, resp.Results[0].Hash)
				assert.Equal(t, "14268186579071905229", resp.Results[0].Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "delete test-rel-1",
		Req: &dsw.DeleteRelationRequest{
			Param: &dsc.RelationIdentifier{
				Subject: &dsc.ObjectIdentifier{
					Type: proto.String("user"),
					Id:   proto.String("bd20c049-ea32-42b3-9ced-c4037a330262"),
				},
				Relation: &dsc.RelationTypeIdentifier{
					Name:       proto.String("manager"),
					ObjectType: proto.String("user"),
				},
				Object: &dsc.ObjectIdentifier{
					Type: proto.String("user"),
					Id:   proto.String("ad20c049-ea32-42b3-9ced-c4037a330261"),
				},
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.DeleteRelationResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "get deleted test-rel-1",
		Req: &dsr.GetRelationRequest{
			Param: &dsc.RelationIdentifier{
				Subject: &dsc.ObjectIdentifier{
					Type: proto.String("user"),
					Id:   proto.String("bd20c049-ea32-42b3-9ced-c4037a330262"),
				},
				Relation: &dsc.RelationTypeIdentifier{
					Name:       proto.String("manager"),
					ObjectType: proto.String("user"),
				},
				Object: &dsc.ObjectIdentifier{
					Type: proto.String("user"),
					Id:   proto.String("ad20c049-ea32-42b3-9ced-c4037a330261"),
				},
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetRelationResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.Nil(t, resp.Results)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "delete deleted test-rel-1",
		Req: &dsw.DeleteRelationRequest{
			Param: &dsc.RelationIdentifier{
				Subject: &dsc.ObjectIdentifier{
					Type: proto.String("user"),
					Id:   proto.String("bd20c049-ea32-42b3-9ced-c4037a330262"),
				},
				Relation: &dsc.RelationTypeIdentifier{
					Name:       proto.String("manager"),
					ObjectType: proto.String("user"),
				},
				Object: &dsc.ObjectIdentifier{
					Type: proto.String("user"),
					Id:   proto.String("ad20c049-ea32-42b3-9ced-c4037a330261"),
				},
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.DeleteRelationResponse:
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
