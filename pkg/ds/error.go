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
	ErrInvalidArgumentPermissionIdentifier   = cerr.NewAsertoError("E20039", codes.InvalidArgument, http.StatusBadRequest, "permission identifier invalid argument")
	ErrInvalidArgumentObjectType             = cerr.NewAsertoError("E20040", codes.InvalidArgument, http.StatusBadRequest, "object type invalid argument")
	ErrInvalidArgumentRelationType           = cerr.NewAsertoError("E20041", codes.InvalidArgument, http.StatusBadRequest, "relation type invalid argument")
	ErrInvalidArgumentPermission             = cerr.NewAsertoError("E20042", codes.InvalidArgument, http.StatusBadRequest, "permission invalid argument")
	ErrInvalidArgumentObject                 = cerr.NewAsertoError("E20042", codes.InvalidArgument, http.StatusBadRequest, "object invalid argument")
	ErrInvalidArgumentRelation               = cerr.NewAsertoError("E20042", codes.InvalidArgument, http.StatusBadRequest, "relation invalid argument")
	ErrGraphDirectionality                   = cerr.NewAsertoError("E20043", codes.InvalidArgument, http.StatusPreconditionFailed, "unable to determine graph directionality")
)
