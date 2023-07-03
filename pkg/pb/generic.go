package pb

import "google.golang.org/protobuf/proto"

func Contains[T proto.Message](collection []T, element T) bool {
	for _, item := range collection {
		if proto.Equal(item, element) {
			return true
		}
	}
	return false
}
