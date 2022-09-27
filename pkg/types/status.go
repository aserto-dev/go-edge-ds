package types

import dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"

type StatusFlag uint32

const (
	Default  StatusFlag = 0
	Hidden   StatusFlag = 1 << dsc.Flag_FLAG_HIDDEN
	ReadOnly StatusFlag = 1 << dsc.Flag_FLAG_READONLY
	System   StatusFlag = 1 << dsc.Flag_FLAG_SYSTEM
	Shadow   StatusFlag = 1 << dsc.Flag_FLAG_SHADOW
)

func Status(i uint32) StatusFlag {
	return StatusFlag(i)
}

func (f StatusFlag) Validate() bool {
	return true
}
