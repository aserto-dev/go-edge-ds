package v3

import (
	"context"

	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	dsa1 "github.com/authzen/access.go/api/access/v1"
	"github.com/rs/zerolog"
)

type Access struct {
	dsa1.UnimplementedAccessServer
	logger *zerolog.Logger
	reader *Reader
}

func NewAccess(logger *zerolog.Logger, reader *Reader) *Access {
	return &Access{
		logger: logger,
		reader: reader,
	}
}

// Evaluation access check.
func (s *Access) Evaluation(ctx context.Context, req *dsa1.EvaluationRequest) (*dsa1.EvaluationResponse, error) {
	resp, err := s.reader.Check(ctx, &dsr3.CheckRequest{
		ObjectType:  req.Resource.Type,
		ObjectId:    req.Resource.Id,
		Relation:    req.Action.Name,
		SubjectType: req.Subject.Type,
		SubjectId:   req.Subject.Id,
	})
	if err != nil {
		return &dsa1.EvaluationResponse{}, err
	}

	return &dsa1.EvaluationResponse{
		Decision: resp.Check,
		Context:  resp.Context,
	}, nil
}

// Evaluations access check.
func (s *Access) Evaluations(ctx context.Context, req *dsa1.EvaluationsRequest) (*dsa1.EvaluationsResponse, error) {
	def := extractCheck(&dsa1.EvaluationRequest{
		Subject:  req.Subject,
		Action:   req.GetAction(),
		Resource: req.GetResource(),
		Context:  req.GetContext(),
	})

	checks := &dsr3.ChecksRequest{
		Default: def,
		Checks:  extractChecks(req),
	}

	checksResp, err := s.reader.Checks(ctx, checks)
	if err != nil {
		return &dsa1.EvaluationsResponse{}, err
	}

	resp := &dsa1.EvaluationsResponse{
		Decisions: extractDecisions(checksResp),
	}
	return resp, nil
}

func extractChecks(req *dsa1.EvaluationsRequest) []*dsr3.CheckRequest {
	checks := make([]*dsr3.CheckRequest, len(req.Evaluations))
	for k, v := range req.Evaluations {
		c := extractCheck(v)
		checks[k] = c
	}
	return checks
}

func extractCheck(req *dsa1.EvaluationRequest) *dsr3.CheckRequest {
	c := &dsr3.CheckRequest{}
	if req.Resource != nil {
		c.ObjectType = req.Resource.GetType()
		c.ObjectId = req.Resource.GetId()
	}
	if req.Action != nil {
		c.Relation = req.Action.GetName()
	}
	if req.Subject != nil {
		c.SubjectType = req.Subject.GetType()
		c.SubjectId = req.Subject.GetId()
	}
	return c
}

func extractDecisions(resp *dsr3.ChecksResponse) []*dsa1.EvaluationResponse {
	evaluations := make([]*dsa1.EvaluationResponse, len(resp.Checks))
	for k, v := range resp.Checks {
		e := &dsa1.EvaluationResponse{}
		e.Decision = v.GetCheck()
		if v.Context != nil {
			e.Context = v.GetContext()
		}
		evaluations[k] = e
	}
	return evaluations
}
