package rid_test

// import (
// 	"bytes"
// 	"testing"

// 	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
// 	"github.com/aserto-dev/go-edge-ds/pkg/ds"

// 	"github.com/pkg/errors"
// 	"github.com/stretchr/testify/require"
// 	"google.golang.org/protobuf/proto"
// )

// func TestRelationIdentifierMarshal(t *testing.T) {}

// func TestRelationIdentifierUnmarshal(t *testing.T) {
// 	tcs := []struct {
// 		objKey []byte
// 		objRID *dsc3.RelationIdentifier
// 	}{
// 		{
// 			objKey: []byte("doc:groceries|owner|user:beth@the-smiths.com"),
// 			objRID: &dsc3.RelationIdentifier{
// 				ObjectType:      "doc",
// 				ObjectId:        "groceries",
// 				Relation:        "owner",
// 				SubjectType:     "user",
// 				SubjectId:       "beth@the-smiths.com",
// 				SubjectRelation: "",
// 			},
// 		},
// 		{
// 			objKey: []byte("folder:morty.shared|editor|group:editor|member:"),
// 			objRID: &dsc3.RelationIdentifier{
// 				ObjectType:      "folder",
// 				ObjectId:        "morty.shared",
// 				Relation:        "editor",
// 				SubjectType:     "group",
// 				SubjectId:       "editor",
// 				SubjectRelation: "member",
// 			},
// 		},
// 	}

// 	for id, tc := range tcs {
// 		{
// 			rid, err := ObjKeyToRID1(tc.objKey)
// 			require.NoError(t, err)

// 			require.True(t, proto.Equal(tc.objRID, rid), "tc %d", id)
// 		}
// 		{
// 			rid, err := ObjKeyToRID2(tc.objKey)
// 			require.NoError(t, err)

// 			require.True(t, proto.Equal(tc.objRID, rid), "tc %d", id)
// 		}
// 	}
// }

// func ObjKeyToRID1(key []byte) (*dsc3.RelationIdentifier, error) {
// 	s := bytes.Split(key, []byte{ds.InstanceSeparator})
// 	l := len(s)
// 	rid := &dsc3.RelationIdentifier{}

// 	if l == 4 {
// 		s4 := bytes.Split(s[3], []byte{ds.TypeIDSeparator})
// 		rid.SubjectRelation = string(s4[0])
// 	}
// 	if l == 3 || l == 4 {
// 		s3 := bytes.Split(s[2], []byte{ds.TypeIDSeparator})
// 		if len(s3) != 2 {
// 			return nil, errors.Errorf("parse failure (s3)")
// 		}

// 		rid.SubjectType = string(s3[0])
// 		rid.SubjectId = string(s3[1])

// 		rid.Relation = string(s[1])

// 		s1 := bytes.Split(s[0], []byte{ds.TypeIDSeparator})
// 		if len(s1) != 2 {
// 			return nil, errors.Errorf("parse failure (s1)")
// 		}

// 		rid.ObjectType = string(s1[0])
// 		rid.ObjectId = string(s1[1])

// 		return rid, nil
// 	}

// 	return nil, errors.Errorf("parse failure (s0)")
// }

// var isToken = func(r rune) bool {
// 	return r == rune(ds.InstanceSeparator) || r == rune(ds.TypeIDSeparator)
// }

// const (
// 	ObjectTypeField      int = 0
// 	ObjectIdField        int = 1
// 	RelationField        int = 2
// 	SubjectTypeField     int = 3
// 	SubjectIdField       int = 4
// 	SubjectRelationField int = 5
// )

// const (
// 	RelIdFieldCount               int = 5
// 	RelIdWithSubjectRelationCount int = 6
// )

// func ObjKeyToRID2(key []byte) (*dsc3.RelationIdentifier, error) {
// 	parts := bytes.FieldsFunc(key, isToken)

// 	l := len(parts)

// 	if l >= RelIdFieldCount && l <= RelIdWithSubjectRelationCount {
// 		rid := &dsc3.RelationIdentifier{
// 			ObjectType:  string(parts[ObjectTypeField]),
// 			ObjectId:    string(parts[ObjectIdField]),
// 			Relation:    string(parts[RelationField]),
// 			SubjectType: string(parts[SubjectTypeField]),
// 			SubjectId:   string(parts[SubjectIdField]),
// 		}
// 		if l == RelIdWithSubjectRelationCount {
// 			rid.SubjectRelation = string(parts[SubjectRelationField])
// 		}
// 		return rid, nil
// 	}

// 	return nil, errors.Errorf("key parse detected %d parts instead of (%d | %d)", l, RelIdFieldCount, RelIdWithSubjectRelationCount)
// }

// func BenchmarkObjKeyToRID1(b *testing.B) {
// 	assert := require.New(b)

// 	for _, key := range keys() {
// 		_, err := ObjKeyToRID1(key)
// 		assert.NoError(err)
// 	}
// }

// func BenchmarkObjKeyToRID2(b *testing.B) {
// 	assert := require.New(b)

// 	for _, key := range keys() {
// 		_, err := ObjKeyToRID2(key)
// 		assert.NoError(err)
// 	}
// }

// func keys() [][]byte {
// 	return [][]byte{
// 		[]byte("doc:groceries|owner|user:beth@the-smiths.com"),
// 		[]byte("doc:groceries|parent|folder:root"),
// 		[]byte("doc:groceries|viewer|user:*"),
// 		[]byte("doc:morty.journal|parent|folder:morty"),
// 		[]byte("doc:morty.shared.notes|owner|user:morty@the-citadel.com"),
// 		[]byte("doc:morty.shared.notes|parent|folder:morty.shared"),
// 		[]byte("doc:rick.inventions|owner|user:rick@the-citadel.com"),
// 		[]byte("doc:rick.inventions|parent|folder:rick"),
// 		[]byte("doc:secrets|owner|user:beth@the-smiths.com"),
// 		[]byte("doc:secrets|parent|folder:root"),
// 		[]byte("folder:beth|owner|user:beth@the-smiths.com"),
// 		[]byte("folder:beth|parent|folder:root"),
// 		[]byte("folder:jerry|owner|user:jerry@the-smiths.com"),
// 		[]byte("folder:jerry|parent|folder:root"),
// 		[]byte("folder:morty.shared|editor|group:editor|member"),
// 		[]byte("folder:morty.shared|parent|folder:morty"),
// 		[]byte("folder:morty.shared|viewer|group:viewer|member"),
// 		[]byte("folder:morty|owner|user:morty@the-citadel.com"),
// 		[]byte("folder:morty|parent|folder:root"),
// 		[]byte("folder:rick|owner|user:rick@the-citadel.com"),
// 		[]byte("folder:rick|parent|folder:root"),
// 		[]byte("folder:root|owner|user:beth@the-smiths.com"),
// 		[]byte("folder:summer|owner|user:summer@the-smiths.com"),
// 		[]byte("folder:summer|parent|folder:root"),
// 		[]byte("group:admin|member|user:rick@the-citadel.com"),
// 		[]byte("group:editor|member|group:admin|member"),
// 		[]byte("group:editor|member|user:morty@the-citadel.com"),
// 		[]byte("group:editor|member|user:summer@the-smiths.com"),
// 		[]byte("group:evil_genius|member|user:rick@the-citadel.com"),
// 		[]byte("group:viewer|member|group:editor|member"),
// 		[]byte("group:viewer|member|user:beth@the-smiths.com"),
// 		[]byte("group:viewer|member|user:jerry@the-smiths.com"),
// 		[]byte("identity:CiRmZDA2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs|identifier|user:rick@the-citadel.com"),
// 		[]byte("identity:CiRmZDE2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs|identifier|user:morty@the-citadel.com"),
// 		[]byte("identity:CiRmZDI2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs|identifier|user:summer@the-smiths.com"),
// 		[]byte("identity:CiRmZDM2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs|identifier|user:beth@the-smiths.com"),
// 		[]byte("identity:CiRmZDQ2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs|identifier|user:jerry@the-smiths.com"),
// 		[]byte("identity:beth@the-smiths.com|identifier|user:beth@the-smiths.com"),
// 		[]byte("identity:jerry@the-smiths.com|identifier|user:jerry@the-smiths.com"),
// 		[]byte("identity:morty@the-citadel.com|identifier|user:morty@the-citadel.com"),
// 		[]byte("identity:rick@the-citadel.com|identifier|user:rick@the-citadel.com"),
// 		[]byte("identity:summer@the-smiths.com|identifier|user:summer@the-smiths.com"),
// 		[]byte("user:beth@the-smiths.com|manager|user:rick@the-citadel.com"),
// 		[]byte("user:jerry@the-smiths.com|manager|user:beth@the-smiths.com"),
// 		[]byte("user:morty@the-citadel.com|manager|user:rick@the-citadel.com"),
// 		[]byte("user:summer@the-smiths.com|manager|user:rick@the-citadel.com"),
// 	}
// }
