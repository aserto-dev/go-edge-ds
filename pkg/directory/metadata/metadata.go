package metadata

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
)

var (
	// base directory object types
	ObjectTypes = []*dsc.ObjectType{
		{Id: 10000, Name: "system", DisplayName: "System", IsSubject: false, Ordinal: 900, Status: uint32(dsc.Flag_FLAG_HIDDEN | dsc.Flag_FLAG_SYSTEM)},
		{Id: 10001, Name: "user", DisplayName: "User", IsSubject: true, Ordinal: 100, Status: uint32(dsc.Flag_FLAG_UNKNOWN)},
		{Id: 10002, Name: "identity", DisplayName: "Identity", IsSubject: false, Ordinal: 300, Status: uint32(dsc.Flag_FLAG_READONLY)},
		{Id: 10003, Name: "group", DisplayName: "Group", IsSubject: false, Ordinal: 200, Status: uint32(dsc.Flag_FLAG_UNKNOWN)},
		{Id: 10004, Name: "application", DisplayName: "Application", IsSubject: false, Ordinal: 400, Status: uint32(dsc.Flag_FLAG_UNKNOWN)},
		{Id: 10005, Name: "resource", DisplayName: "Resource", IsSubject: false, Ordinal: 500, Status: uint32(dsc.Flag_FLAG_UNKNOWN)},
		{Id: 10006, Name: "user-v1", DisplayName: "UserV1", IsSubject: true, Ordinal: 1000, Status: uint32(dsc.Flag_FLAG_HIDDEN | dsc.Flag_FLAG_READONLY | dsc.Flag_FLAG_SHADOW)},
	}

	// additional DS0 object types
	RootObjectTypes = []*dsc.ObjectType{
		{Id: 10101, Name: "tenant", DisplayName: "Tenant", IsSubject: false, Ordinal: 0, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Id: 10103, Name: "machine", DisplayName: "Machine", IsSubject: true, Ordinal: 0, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
		{Id: 10104, Name: "service", DisplayName: "Service", IsSubject: false, Ordinal: 0, Status: uint32(dsc.Flag_FLAG_SYSTEM)},
	}

	// base directory relation types
	RelationTypes = []*dsc.RelationType{
		{Id: 90000, ObjectType: "system", Name: "user", DisplayName: "system:user", Ordinal: 900, Status: uint32(dsc.Flag_FLAG_UNKNOWN)},
		{Id: 90001, ObjectType: "identity", Name: "identifier", DisplayName: "identity:identifier", Ordinal: 200, Status: uint32(dsc.Flag_FLAG_UNKNOWN)},
		{Id: 90002, ObjectType: "group", Name: "member", DisplayName: "group:member", Ordinal: 100, Status: uint32(dsc.Flag_FLAG_UNKNOWN)},
		{Id: 90003, ObjectType: "application", Name: "user", DisplayName: "application:user", Ordinal: 400, Status: uint32(dsc.Flag_FLAG_UNKNOWN)},
		{Id: 90004, ObjectType: "user", Name: "manager", DisplayName: "user:manager", Ordinal: 300, Status: uint32(dsc.Flag_FLAG_UNKNOWN)},
	}

	// additional DS0 relation types
	RootRelationTypes = []*dsc.RelationType{
		{Id: 90100, ObjectType: "tenant", Name: "account", DisplayName: "tenant:account"},
		{Id: 90104, ObjectType: "tenant", Name: "viewer", DisplayName: "tenant:viewer"},
		{Id: 90103, ObjectType: "tenant", Name: "member", DisplayName: "tenant:member", Unions: []string{"viewer"}},
		{Id: 90102, ObjectType: "tenant", Name: "admin", DisplayName: "tenant:admin", Unions: []string{"member", "viewer"}},
		{Id: 90101, ObjectType: "tenant", Name: "owner", DisplayName: "tenant:owner", Unions: []string{"admin", "member", "viewer"}},
		{Id: 90105, ObjectType: "tenant", Name: "decision-log-reader", DisplayName: "tenant:decision-log-reader"},
		{Id: 90106, ObjectType: "tenant", Name: "discovery-client", DisplayName: "tenant:discovery-client"},
		{Id: 90107, ObjectType: "tenant", Name: "edge-authorizer", DisplayName: "tenant:edge-authorizer"},
		{Id: 90108, ObjectType: "tenant", Name: "directory-client-reader", DisplayName: "tenant:directory-client-reader"},
		{Id: 90109, ObjectType: "tenant", Name: "directory-client-writer", DisplayName: "tenant:directory-client-writer"},
		{Id: 90200, ObjectType: "system", Name: "admin", DisplayName: "system:admin"},
		{Id: 90201, ObjectType: "system", Name: "task-handler", DisplayName: "task:handler"},
		{Id: 90202, ObjectType: "system", Name: "task-manager", DisplayName: "task:manager"},
	}
)
