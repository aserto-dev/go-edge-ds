module github.com/aserto-dev/edge-ds

go 1.17

// replace github.com/aserto-dev/go-directory => ../go-directory

require (
	github.com/aserto-dev/certs v0.0.2
	github.com/aserto-dev/go-directory v0.0.14
	github.com/google/uuid v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/magefile/mage v1.14.0
	github.com/mitchellh/hashstructure/v2 v2.0.2
	github.com/pkg/errors v0.9.1
	github.com/rs/zerolog v1.28.0
	github.com/spf13/pflag v1.0.5
	go.etcd.io/bbolt v1.3.6
)

require (
	github.com/aserto-dev/errors v0.0.1 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
)

require (
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.11.3 // indirect
	golang.org/x/net v0.0.0-20220909164309-bea034e7d591 // indirect
	golang.org/x/sys v0.0.0-20220907062415-87db552b00fd // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220927151529-dcaddaf36704 // indirect
	google.golang.org/grpc v1.50.1
	google.golang.org/protobuf v1.28.1
)
