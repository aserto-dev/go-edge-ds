package ds

import (
	"net/http"

	cerr "github.com/aserto-dev/errors"

	"google.golang.org/grpc/codes"
)

var (
	// TODO renumber errors.
	ErrObjectTypeNotFound                    = cerr.NewAsertoError("E20031", codes.NotFound, http.StatusNotFound, "object type not found")
	ErrObjectNotFound                        = cerr.NewAsertoError("E20032", codes.NotFound, http.StatusNotFound, "object not found")
	ErrRelationNotFound                      = cerr.NewAsertoError("E20033", codes.NotFound, http.StatusNotFound, "relation not found")
	ErrInvalidArgumentObjectTypeIdentifier   = cerr.NewAsertoError("E20034", codes.InvalidArgument, http.StatusBadRequest, "object type identifier invalid argument")
	ErrInvalidArgumentRelationTypeIdentifier = cerr.NewAsertoError("E20034", codes.InvalidArgument, http.StatusBadRequest, "relation type identifier invalid argument")
	ErrInvalidArgumentObjectIdentifier       = cerr.NewAsertoError("E20035", codes.InvalidArgument, http.StatusBadRequest, "object identifier invalid argument")
	ErrInvalidArgumentRelationIdentifier     = cerr.NewAsertoError("E20036", codes.InvalidArgument, http.StatusBadRequest, "relation identifier invalid argument")
	ErrInvalidArgumentObjectTypeSelector     = cerr.NewAsertoError("E20037", codes.InvalidArgument, http.StatusBadRequest, "object type selector invalid argument")
	ErrNoCompleteObjectIdentifier            = cerr.NewAsertoError("E20038", codes.FailedPrecondition, http.StatusPreconditionFailed, "relation identifier no complete object identifier")
)
