package fs

import (
	"os"
)

const (
	FileMode0644 = 0o644
	FileMode0700 = 0o700
)

func FileExists(path string) bool {
	fsInfo, err := os.Stat(path)
	if err == nil && !fsInfo.IsDir() {
		return true
	}

	return false
}

func DirExists(path string) bool {
	fsInfo, err := os.Stat(path)
	if err == nil && fsInfo.IsDir() {
		return true
	}

	return false
}

func EnsureDirPath(path string) error {
	if !DirExists(path) {
		return os.MkdirAll(path, FileMode0700)
	}

	return nil
}
