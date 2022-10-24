//go:build mage
// +build mage

package main

import (
	"runtime"

	"github.com/aserto-dev/mage-loot/common"
	"github.com/aserto-dev/mage-loot/deps"
	"github.com/magefile/mage/sh"
)

// Lint runs linting for the entire project.
func Lint() error {
	return common.Lint()
}

// Test runs all tests and generates a code coverage report.
func Test() error {
	return common.Test()
}

func Deps() {
	deps.GetAllDeps()
}

func Build() error {
	return common.Build()
}

func Run() error {
	return sh.RunV(
		"./bin/"+runtime.GOOS+"-"+runtime.GOARCH+"/server",
		"run",
		"--db_path", ".db/acmecorp.db",
		"--seed",
	)
}
