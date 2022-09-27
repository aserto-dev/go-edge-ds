package types

import "github.com/google/uuid"

type id struct{}

var ID = id{}

func (id) IsValid(id string) bool {
	if _, err := uuid.Parse(id); err == nil {
		return true
	}
	return false
}
