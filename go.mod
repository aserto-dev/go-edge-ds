module github.com/aserto-dev/go-edge-ds

go 1.19

replace github.com/aserto-dev/go-directory => ../go-directory

require (
	github.com/Masterminds/semver v1.5.0
	github.com/aserto-dev/errors v0.0.6
	github.com/aserto-dev/go-aserto v0.20.3
	github.com/aserto-dev/go-directory v0.21.7-0.20230811200833-ff67a9fd261e
	github.com/bufbuild/protovalidate-go v0.2.1
	github.com/google/uuid v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	github.com/magefile/mage v1.15.0
	github.com/mitchellh/hashstructure/v2 v2.0.2
	github.com/pkg/errors v0.9.1
	github.com/rs/zerolog v1.30.0
	github.com/samber/lo v1.38.1
	github.com/stretchr/testify v1.8.4
	go.etcd.io/bbolt v1.3.7
	google.golang.org/grpc v1.57.0
	google.golang.org/protobuf v1.31.0
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.31.0-20230721003620-2341cbb21958.1 // indirect
	github.com/antlr/antlr4/runtime/Go/antlr/v4 v4.0.0-20230512164433-5d1fd1a340c9 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/cel-go v0.17.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.16.2 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stoewer/go-strcase v1.3.0 // indirect
	golang.org/x/exp v0.0.0-20230817173708-d852ddb80c63 // indirect
	golang.org/x/net v0.14.0 // indirect
	golang.org/x/sys v0.11.0 // indirect
	golang.org/x/text v0.12.0 // indirect
	google.golang.org/genproto v0.0.0-20230706204954-ccb25ca9f130 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230726155614-23370e0ffb3e // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230726155614-23370e0ffb3e // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
