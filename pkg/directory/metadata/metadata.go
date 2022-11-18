package metadata

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
)

var (
	// base directory object types.
	ObjectTypes = []*dsc.ObjectType{
		{Id: 10000, Name: "system", DisplayName: "System", IsSubject: false, Ordinal: 900, Status: uint32(dsc.Flag_FLAG_HIDDEN | dsc.Flag_FLAG_SYSTEM)},
		{Id: 10001, Name: "user", DisplayName: "User", IsSubject: true, Ordinal: 100, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Id: 10002, Name: "identity", DisplayName: "Identity", IsSubject: false, Ordinal: 300, Status: uint32(dsc.Flag_FLAG_SYSTEM | dsc.Flag_FLAG_READONLY)},
		{Id: 10003, Name: "group", DisplayName: "Group", IsSubject: true, Ordinal: 200, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Id: 10004, Name: "application", DisplayName: "Application", IsSubject: false, Ordinal: 400, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Id: 10005, Name: "resource", DisplayName: "Resource", IsSubject: false, Ordinal: 500, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Id: 10006, Name: "user-v1", DisplayName: "UserV1", IsSubject: true, Ordinal: 1000, Status: uint32(dsc.Flag_FLAG_HIDDEN | dsc.Flag_FLAG_SYSTEM | dsc.Flag_FLAG_SHADOW | dsc.Flag_FLAG_READONLY)},
	}

	// base directory relation types.
	RelationTypes = []*dsc.RelationType{
		{Id: 90000, ObjectType: "system", Name: "user", DisplayName: "system:user", Ordinal: 900, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Id: 90001, ObjectType: "identity", Name: "identifier", DisplayName: "identity:identifier", Ordinal: 200, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Id: 90002, ObjectType: "group", Name: "member", DisplayName: "group:member", Ordinal: 100, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Id: 90003, ObjectType: "application", Name: "user", DisplayName: "application:user", Ordinal: 400, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Id: 90004, ObjectType: "user", Name: "manager", DisplayName: "user:manager", Ordinal: 300, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
	}
)
