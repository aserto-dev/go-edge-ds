package ds

import (
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
)

type objectDependency struct {
	*dsc2.ObjectDependency
}

func ObjectDependency(i *dsc2.ObjectDependency) *objectDependency { return &objectDependency{i} }
