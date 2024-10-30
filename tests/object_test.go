package tests_test

import (
	"testing"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"
	"github.com/aserto-dev/go-directory/pkg/pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestObjects(t *testing.T) {
	tcs := []*TestCase{}

	tcs = append(tcs, objectTestCasesWithID...)
	tcs = append(tcs, objectTestCasesWithoutID...)
	tcs = append(tcs, objectTestCasesStreamMode...)

	testRunner(t, tcs)
}

var objectTestCasesWithID = []*TestCase{
	{
		Name: "create test-obj-1",
		Req: &dsw.SetObjectRequest{
			Object: &dsc.Object{
				Type:        "user",
				Id:          "test-user@acmecorp.com",
				DisplayName: "test obj 1",
				Properties:  pb.NewStruct(),
				Etag:        "",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp etag:%s", resp.Result.Etag)

				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user@acmecorp.com", resp.Result.Id)
				assert.Equal(t, "test obj 1", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Etag)
				assert.Equal(t, "3016620182482667549", resp.Result.Etag)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "get test-obj-1",
		Req: &dsr.GetObjectRequest{
			ObjectType: "user",
			ObjectId:   "test-user@acmecorp.com",
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp etag:%s", resp.Result.Etag)

				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user@acmecorp.com", resp.Result.Id)
				assert.Equal(t, "test obj 1", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Etag)
				assert.Equal(t, "3016620182482667549", resp.Result.Etag)
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "update test-obj-1",
		Req: &dsw.SetObjectRequest{
			Object: &dsc.Object{
				Type:        "user",
				Id:          "test-user-11@acmecorp.com",
				DisplayName: "test obj 11",
				Etag:        "3016620182482667549",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp etag:%s", resp.Result.Etag)

				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user-11@acmecorp.com", resp.Result.Id)
				assert.Equal(t, "test obj 11", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Etag)
				assert.Equal(t, "2708540687187161441", resp.Result.Etag)
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "get updated test-obj-11",
		Req: &dsr.GetObjectRequest{
			ObjectType: "user",
			ObjectId:   "test-user-11@acmecorp.com",
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp etag:%s", resp.Result.Etag)

				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user-11@acmecorp.com", resp.Result.Id)
				assert.Equal(t, "test obj 11", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Etag)
				assert.Equal(t, "2708540687187161441", resp.Result.Etag)
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "delete test-obj-11",
		Req: &dsw.DeleteObjectRequest{
			ObjectType: "user",
			ObjectId:   "test-user-11@acmecorp.com",
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.DeleteObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "get deleted test-obj-11",
		Req: &dsr.GetObjectRequest{
			ObjectType: "user",
			ObjectId:   "test-user-11@acmecorp.com",
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			assert.Error(t, tErr)
			assert.Contains(t, tErr.Error(), "key not found")
			assert.Nil(t, msg)
			return func(req proto.Message) {}
		},
	},
	{
		Name: "delete deleted test-obj-11 by id",
		Req: &dsw.DeleteObjectRequest{
			ObjectType: "user",
			ObjectId:   "test-user-11@acmecorp.com",
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.DeleteObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
			}
			return func(req proto.Message) {}
		},
	},
}

// create object without id.
var objectTestCasesWithoutID = []*TestCase{
	{
		Name: "create test-obj-2 with no-id",
		Req: &dsw.SetObjectRequest{
			Object: &dsc.Object{
				Type:        "user",
				Id:          "test-user-2@acmecorp.com",
				DisplayName: "test obj 2",
				Properties:  pb.NewStruct(),
				Etag:        "",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp etag:%s", resp.Result.Etag)

				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user-2@acmecorp.com", resp.Result.Id)
				assert.Equal(t, "test obj 2", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Etag)
				assert.True(t, len(resp.Result.Etag) > 4)

				return func(req proto.Message) {
					lastHash := resp.Result.Etag

					switch r := req.(type) {
					case *dsw.SetObjectRequest:
						r.Object.Etag = lastHash
					}
					t.Logf("propagated hash:%s", lastHash)
				}
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "get test-obj-2",
		Req: &dsr.GetObjectRequest{
			ObjectType: "user",
			ObjectId:   "test-user-2@acmecorp.com",
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp etag:%s", resp.Result.Etag)

				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user-2@acmecorp.com", resp.Result.Id)
				assert.Equal(t, "test obj 2", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Etag)
				assert.True(t, len(resp.Result.Etag) > 4)

				return func(req proto.Message) {
					lastHash := resp.Result.Etag

					switch r := req.(type) {
					case *dsw.SetObjectRequest:
						r.Object.Etag = lastHash
					}
					t.Logf("propagated hash:%s", lastHash)
				}
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "update test-obj-2",
		Req: &dsw.SetObjectRequest{
			Object: &dsc.Object{
				Type:        "user",
				Id:          "test-user-2@acmecorp.com",
				DisplayName: "test obj 22",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp etag:%s", resp.Result.Etag)

				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user-2@acmecorp.com", resp.Result.Id)
				assert.Equal(t, "test obj 22", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Etag)
				assert.True(t, len(resp.Result.Etag) > 4)
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "get updated test-obj-2",
		Req: &dsr.GetObjectRequest{
			ObjectType: "user",
			ObjectId:   "test-user-2@acmecorp.com",
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp etag:%s", resp.Result.Etag)

				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user-2@acmecorp.com", resp.Result.Id)
				assert.Equal(t, "test obj 22", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Etag)
				assert.True(t, len(resp.Result.Etag) > 4)
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "delete test-obj-2",
		Req: &dsw.DeleteObjectRequest{
			ObjectType: "user",
			ObjectId:   "test-user-2@acmecorp.com",
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.DeleteObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "get deleted test-obj-2",
		Req: &dsr.GetObjectRequest{
			ObjectType: "user",
			ObjectId:   "test-user-2@acmecorp.com",
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			assert.Error(t, tErr)
			assert.Contains(t, tErr.Error(), "key not found")
			assert.Nil(t, msg)
			return func(req proto.Message) {}
		},
	},
}

var objectTestCasesStreamMode = []*TestCase{}
