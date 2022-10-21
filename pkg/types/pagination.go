package types

import dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"

type PaginationRequest struct {
	*dsc.PaginationRequest
}

func NewPaginationRequest(i *dsc.PaginationRequest) *PaginationRequest {
	if i == nil {
		return &PaginationRequest{PaginationRequest: &dsc.PaginationRequest{}}
	}
	return &PaginationRequest{PaginationRequest: i}
}

type PaginationResponse struct {
	*dsc.PaginationResponse
}
