package ds

import (
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
)

func PaginationRequest2(r *dsc3.PaginationRequest) *dsc2.PaginationRequest {
	if r == nil {
		return &dsc2.PaginationRequest{
			Size:  100,
			Token: "",
		}
	}
	return &dsc2.PaginationRequest{
		Size:  r.GetSize(),
		Token: r.GetToken(),
	}
}

func PaginationResponse3(r *dsc2.PaginationResponse) *dsc3.PaginationResponse {
	if r == nil {
		return &dsc3.PaginationResponse{
			NextToken: "",
		}
	}
	return &dsc3.PaginationResponse{
		NextToken: r.GetNextToken(),
	}
}
