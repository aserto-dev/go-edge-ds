module github.com/aserto-dev/go-edge-ds

go 1.23.5

// replace github.com/aserto-dev/azm => ../azm
// replace github.com/aserto-dev/go-directory => ../go-directory

replace github.com/bufbuild/protovalidate-go => github.com/bufbuild/protovalidate-go v0.7.3

require (
	github.com/Masterminds/semver/v3 v3.3.1
	github.com/aserto-dev/aserto-grpc v0.2.9
	github.com/aserto-dev/azm v0.2.9-0.20250220002245-5ec95be39106
	github.com/aserto-dev/errors v0.0.13
	github.com/aserto-dev/go-directory v0.33.5
	github.com/authzen/access.go v0.0.0-20250123041208-d58afed67b50
	github.com/bufbuild/protovalidate-go v0.8.2
	github.com/go-http-utils/headers v0.0.0-20181008091004-fed159eddc2a
	github.com/gonvenience/ytbx v1.4.6
	github.com/google/uuid v1.6.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	github.com/homeport/dyff v1.9.4
	github.com/panmari/cuckoofilter v1.0.6
	github.com/pkg/errors v0.9.1
	github.com/rs/zerolog v1.33.0
	github.com/samber/lo v1.49.0
	github.com/stretchr/testify v1.10.0
	go.etcd.io/bbolt v1.3.11
	golang.org/x/sync v0.11.0
	google.golang.org/grpc v1.70.0
	google.golang.org/protobuf v1.36.5
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.3-20241127180247-a33202765966.1 // indirect
	cel.dev/expr v0.19.1 // indirect
	github.com/BurntSushi/toml v1.4.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/deckarep/golang-set/v2 v2.7.0 // indirect
	github.com/dgryski/go-metro v0.0.0-20250106013310-edb8663e5e33 // indirect
	github.com/gonvenience/bunt v1.4.0 // indirect
	github.com/gonvenience/neat v1.3.15 // indirect
	github.com/gonvenience/term v1.0.3 // indirect
	github.com/gonvenience/text v1.0.8 // indirect
	github.com/google/cel-go v0.22.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.1 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/mattn/go-ciede2000 v0.0.0-20170301095244-782e8c62fec3 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mitchellh/go-ps v1.0.0 // indirect
	github.com/mitchellh/hashstructure v1.1.0 // indirect
	github.com/mitchellh/hashstructure/v2 v2.0.2 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sergi/go-diff v1.3.2-0.20230802210424-5b0b94c5c0d3 // indirect
	github.com/stoewer/go-strcase v1.3.0 // indirect
	github.com/texttheater/golang-levenshtein v1.0.1 // indirect
	github.com/virtuald/go-ordered-json v0.0.0-20170621173500-b18e6e673d74 // indirect
	golang.org/x/exp v0.0.0-20250106191152-7588d65b2ba8 // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/term v0.29.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250212204824-5a70512c5d8b // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250212204824-5a70512c5d8b // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
