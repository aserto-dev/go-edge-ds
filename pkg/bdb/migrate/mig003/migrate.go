package mig003

import (
	bolt "go.etcd.io/bbolt"
)

// mig003
//
// backup current database file
// mount current database file as read-only
// add _manifest bucket
// convert object_types, relation_types, permissions to annotated v3 manifest
// set manifest
// set model
// copy object (with schema check)
// copy relations (with schema check)
// set schema to 3

const (
	Version string = "0.0.3"
)

var fnMap = []func(*bolt.DB, *bolt.DB) error{}

func Migrate(roDB, rwDB *bolt.DB) error {
	for _, fn := range fnMap {
		if err := fn(roDB, rwDB); err != nil {
			return err
		}
	}
	return nil
}
