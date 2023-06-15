package ds

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
)

type objectDependency struct {
	*dsc.ObjectDependency
}

func ObjectDependency(i *dsc.ObjectDependency) *objectDependency { return &objectDependency{i} }
