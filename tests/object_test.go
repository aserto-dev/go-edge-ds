package tests_test

import (
	"testing"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"github.com/aserto-dev/go-directory/pkg/pb"
	"github.com/aserto-dev/go-edge-ds/pkg/types"

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
				Id:          "fd20c049-ea32-42b3-9ced-c4037a33026b",
				Type:        "user",
				Key:         "test-user@acmecorp.com",
				DisplayName: "test obj 1",
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
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "fd20c049-ea32-42b3-9ced-c4037a33026b", resp.Result.Id)
				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user@acmecorp.com", resp.Result.Key)
				assert.Equal(t, "test obj 1", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "8123438213397438108", resp.Result.Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "get test-obj-1",
		Req: &dsr.GetObjectRequest{
			Param: &dsc.ObjectIdentifier{
				Id: proto.String("fd20c049-ea32-42b3-9ced-c4037a33026b"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "fd20c049-ea32-42b3-9ced-c4037a33026b", resp.Result.Id)
				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user@acmecorp.com", resp.Result.Key)
				assert.Equal(t, "test obj 1", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "8123438213397438108", resp.Result.Hash)
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "update test-obj-1",
		Req: &dsw.SetObjectRequest{
			Object: &dsc.Object{
				Id:          "fd20c049-ea32-42b3-9ced-c4037a33026b",
				Type:        "user",
				Key:         "test-user-11@acmecorp.com",
				DisplayName: "test obj 11",
				Hash:        "8123438213397438108",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "fd20c049-ea32-42b3-9ced-c4037a33026b", resp.Result.Id)
				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user-11@acmecorp.com", resp.Result.Key)
				assert.Equal(t, "test obj 11", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "1525294859288182108", resp.Result.Hash)
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "get updated test-obj-11",
		Req: &dsr.GetObjectRequest{
			Param: &dsc.ObjectIdentifier{
				Type: proto.String("user"),
				Key:  proto.String("test-user-11@acmecorp.com"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "fd20c049-ea32-42b3-9ced-c4037a33026b", resp.Result.Id)
				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user-11@acmecorp.com", resp.Result.Key)
				assert.Equal(t, "test obj 11", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "1525294859288182108", resp.Result.Hash)
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "delete test-obj-11",
		Req: &dsw.DeleteObjectRequest{
			Param: &dsc.ObjectIdentifier{
				Type: proto.String("user"),
				Key:  proto.String("test-user-11@acmecorp.com"),
			},
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
			Param: &dsc.ObjectIdentifier{
				Type: proto.String("user"),
				Key:  proto.String("test-user-11@acmecorp.com"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			assert.Error(t, tErr)
			assert.Nil(t, msg)
			return func(req proto.Message) {}
		},
	},
	{
		Name: "delete deleted test-obj-11 by id",
		Req: &dsw.DeleteObjectRequest{
			Param: &dsc.ObjectIdentifier{
				Id: proto.String("fd20c049-ea32-42b3-9ced-c4037a33026b"),
			},
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
				Key:         "test-user-2@acmecorp.com",
				DisplayName: "test obj 2",
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
				t.Logf("resp hash:%s", resp.Result.Hash)
				t.Logf("resp id:%s", resp.Result.Id)

				assert.True(t, types.ID.IsValid(resp.Result.Id))
				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user-2@acmecorp.com", resp.Result.Key)
				assert.Equal(t, "test obj 2", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.True(t, len(resp.Result.Hash) > 4)

				return func(req proto.Message) {
					lastHash := resp.Result.Hash

					switch r := req.(type) {
					case *dsw.SetObjectRequest:
						r.Object.Hash = lastHash
					}
				}
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "get test-obj-2",
		Req: &dsr.GetObjectRequest{
			Param: &dsc.ObjectIdentifier{
				Type: proto.String("user"),
				Key:  proto.String("test-user-2@acmecorp.com"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)
				t.Logf("resp id:%s", resp.Result.Id)

				assert.True(t, types.ID.IsValid(resp.Result.Id))
				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user-2@acmecorp.com", resp.Result.Key)
				assert.Equal(t, "test obj 2", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.True(t, len(resp.Result.Hash) > 4)

				return func(req proto.Message) {
					lastHash := resp.Result.Hash

					switch r := req.(type) {
					case *dsw.SetObjectRequest:
						r.Object.Hash = lastHash
					}
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
				Key:         "test-user-2@acmecorp.com",
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
				t.Logf("resp hash:%s", resp.Result.Hash)
				t.Logf("resp id:%s", resp.Result.Id)

				assert.True(t, types.ID.IsValid(resp.Result.Id))
				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user-2@acmecorp.com", resp.Result.Key)
				assert.Equal(t, "test obj 22", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.True(t, len(resp.Result.Hash) > 4)
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "get updated test-obj-2",
		Req: &dsr.GetObjectRequest{
			Param: &dsc.ObjectIdentifier{
				Type: proto.String("user"),
				Key:  proto.String("test-user-2@acmecorp.com"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetObjectResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)
				t.Logf("resp id:%s", resp.Result.Id)

				assert.True(t, types.ID.IsValid(resp.Result.Id))
				assert.Equal(t, "user", resp.Result.Type)
				assert.Equal(t, "test-user-2@acmecorp.com", resp.Result.Key)
				assert.Equal(t, "test obj 22", resp.Result.DisplayName)
				assert.NotNil(t, resp.Result.Properties)
				assert.Len(t, resp.Result.Properties.Fields, 0)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.True(t, len(resp.Result.Hash) > 4)
			}
			return func(req proto.Message) {}
		},
	},
	{
		Name: "delete test-obj-2",
		Req: &dsw.DeleteObjectRequest{
			Param: &dsc.ObjectIdentifier{
				Type: proto.String("user"),
				Key:  proto.String("test-user-2@acmecorp.com"),
			},
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
			Param: &dsc.ObjectIdentifier{
				Type: proto.String("user"),
				Key:  proto.String("test-user-2@acmecorp.com"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			assert.Error(t, tErr)
			assert.Nil(t, msg)
			return func(req proto.Message) {}
		},
	},
}

var objectTestCasesStreamMode = []*TestCase{}
