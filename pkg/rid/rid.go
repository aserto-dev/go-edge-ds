// nolint: stylecheck
package rid

import (
	"bytes"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"

	"github.com/pkg/errors"
)

var isToken = func(r rune) bool {
	return r == InstanceSeparator || r == TypeIDSeparator
}

const (
	InstanceSeparator = rune('|')
	TypeIDSeparator   = rune(':')
)

const (
	ObjKeyObjectTypeField      int = 0
	ObjKeyObjectIdField        int = 1
	ObjKeyRelationField        int = 2
	ObjKeySubjectTypeField     int = 3
	ObjKeySubjectIdField       int = 4
	ObjKeySubjectRelationField int = 5
)

const (
	SubKeySubjectTypeField     int = 0
	SubKeySubjectIdField       int = 1
	SubKeyRelationField        int = 2
	SubKeyObjectTypeField      int = 3
	SubKeyObjectIdField        int = 4
	SubKeySubjectRelationField int = 5
)

const (
	RelIdFieldCount               int = 5
	RelIdWithSubjectRelationCount int = 6
)

func ObjKeyToRID(key []byte, rid *dsc3.RelationIdentifier) error {
	parts := bytes.FieldsFunc(key, isToken)

	l := len(parts)

	if l >= RelIdFieldCount && l <= RelIdWithSubjectRelationCount {
		rid.ObjectType = string(parts[ObjKeyObjectTypeField])
		rid.ObjectId = string(parts[ObjKeyObjectIdField])
		rid.Relation = string(parts[ObjKeyRelationField])
		rid.SubjectType = string(parts[ObjKeySubjectTypeField])
		rid.SubjectId = string(parts[ObjKeySubjectIdField])

		if l == RelIdWithSubjectRelationCount {
			rid.SubjectRelation = string(parts[ObjKeySubjectRelationField])
		} else {
			rid.SubjectRelation = ""
		}

		return nil
	}

	return errors.Errorf("key parse detected %d key parts instead of (%d | %d)", l, RelIdFieldCount, RelIdWithSubjectRelationCount)
}

func SubKeyToRID(key []byte, rid *dsc3.RelationIdentifier) error {
	parts := bytes.FieldsFunc(key, isToken)

	l := len(parts)

	if l >= RelIdFieldCount && l <= RelIdWithSubjectRelationCount {
		rid.SubjectType = string(parts[SubKeySubjectTypeField])
		rid.SubjectId = string(parts[SubKeySubjectIdField])
		rid.Relation = string(parts[SubKeyRelationField])
		rid.ObjectType = string(parts[SubKeyObjectTypeField])
		rid.ObjectId = string(parts[SubKeyObjectIdField])

		if l == RelIdWithSubjectRelationCount {
			rid.SubjectRelation = string(parts[SubKeySubjectRelationField])
		} else {
			rid.SubjectRelation = ""
		}

		return nil
	}

	return errors.Errorf("key parse detected %d key parts instead of (%d | %d)", l, RelIdFieldCount, RelIdWithSubjectRelationCount)
}
