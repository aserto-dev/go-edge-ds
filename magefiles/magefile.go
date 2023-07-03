//go:build mage
// +build mage

package main

import (
	"os"
	"runtime"

	"github.com/aserto-dev/mage-loot/common"
	"github.com/aserto-dev/mage-loot/deps"
	"github.com/magefile/mage/sh"
)

func init() {
	os.Setenv("GO_VERSION", "1.19")
	os.Setenv("DOCKER_BUILDKIT", "1")
}

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
	return sh.RunV("go", []string{
		"build",
		"-o", "./bin/" + runtime.GOOS + "-" + runtime.GOARCH + "/server",
		"./internal/cmd/server"}...)
}

func Run() error {
	return sh.RunV(
		"./bin/"+runtime.GOOS+"-"+runtime.GOARCH+"/server",
		"run",
		"--db_path", ".db/acmecorp.db",
		"--seed",
	)
}
