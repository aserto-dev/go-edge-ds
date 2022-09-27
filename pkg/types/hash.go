package types

import "encoding/binary"

const DefaultHash string = `0`

func IsDefaultHash(h string) bool {
	return h == DefaultHash
}

func Int32ToByte(i int32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(i))
	return buf
}

func Int64ToByte(i int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(i))
	return buf
}

func Uint32ToByte(i uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, i)
	return buf
}

func Uint64ToByte(i uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, i)
	return buf
}

func BoolToByte(b bool) []byte {
	if b {
		return []byte{0x1}
	}
	return []byte{0x0}
}
