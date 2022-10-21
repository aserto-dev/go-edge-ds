package types

import (
	"strings"

	"github.com/google/uuid"
)

type id struct{}

var ID = id{}

func (id) IsValid(id string) bool {
	if _, err := uuid.Parse(id); err == nil {
		return true
	}
	return false
}

func (id) IsValidIfSet(id string) bool {
	if strings.TrimSpace(id) == "" {
		return true
	}
	if _, err := uuid.Parse(id); err == nil {
		return true
	}
	return false
}

func (id) New() string {
	return strings.ToLower(uuid.NewString())
}
