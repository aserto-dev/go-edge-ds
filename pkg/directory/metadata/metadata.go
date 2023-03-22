package metadata

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
)

var (
	// base directory object types.
	ObjectTypes = []*dsc.ObjectType{
		{Name: "system", DisplayName: "System", IsSubject: false, Ordinal: 900, Status: uint32(dsc.Flag_FLAG_HIDDEN | dsc.Flag_FLAG_SYSTEM)},
		{Name: "user", DisplayName: "User", IsSubject: true, Ordinal: 100, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Name: "identity", DisplayName: "Identity", IsSubject: false, Ordinal: 300, Status: uint32(dsc.Flag_FLAG_SYSTEM | dsc.Flag_FLAG_READONLY)},
		{Name: "group", DisplayName: "Group", IsSubject: true, Ordinal: 200, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Name: "application", DisplayName: "Application", IsSubject: false, Ordinal: 400, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Name: "resource", DisplayName: "Resource", IsSubject: false, Ordinal: 500, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Name: "user-v1", DisplayName: "UserV1", IsSubject: true, Ordinal: 1000, Status: uint32(dsc.Flag_FLAG_HIDDEN | dsc.Flag_FLAG_SYSTEM | dsc.Flag_FLAG_SHADOW | dsc.Flag_FLAG_READONLY)},
	}

	// base directory relation types.
	RelationTypes = []*dsc.RelationType{
		{ObjectType: "system", Name: "user", DisplayName: "system:user", Ordinal: 900, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{ObjectType: "identity", Name: "identifier", DisplayName: "identity:identifier", Ordinal: 200, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{ObjectType: "group", Name: "member", DisplayName: "group:member", Ordinal: 100, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{ObjectType: "application", Name: "user", DisplayName: "application:user", Ordinal: 400, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{ObjectType: "user", Name: "manager", DisplayName: "user:manager", Ordinal: 300, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
	}
)
