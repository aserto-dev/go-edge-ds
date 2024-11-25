package ds

import (
	"context"
	"runtime"
	"sync"

	"github.com/aserto-dev/azm/cache"

	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"

	bolt "go.etcd.io/bbolt"
)

type checks struct {
	*dsr3.ChecksRequest
}

func Checks(i *dsr3.ChecksRequest) *checks {
	return &checks{i}
}

func (i *checks) Validate(mc *cache.Cache) error {
	return nil
}

func (i *checks) Exec(ctx context.Context, tx *bolt.Tx, mc *cache.Cache) (*dsr3.ChecksResponse, error) {
	inbox := make(chan *checksReq, len(i.Checks))
	outbox := make(chan *checksResp, len(i.Checks))

	// setup Check workers.
	var wg sync.WaitGroup
	dop := min(runtime.GOMAXPROCS(0), len(i.Checks))
	for wc := 0; wc < dop; wc++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// handle Check request.
			for req := range inbox {

				check := Check(req.CheckRequest)

				// validate check request, on error, deny access, set context and return early.
				if err := check.Validate(mc); err != nil {
					outbox <- &checksResp{
						Index: req.Index,
						CheckResponse: &dsr3.CheckResponse{
							Check:   false,
							Context: SetContextWithReason(err),
						},
					}
					return
				}

				resp, err := check.Exec(ctx, tx, mc)
				if err != nil {
					// TODO log err
					_ = err
				}

				outbox <- &checksResp{Index: req.Index, CheckResponse: resp}
			}
		}()
	}

	// substitute defaults.
	for index := 0; index < len(i.Checks); index++ {
		if i.Checks[index].GetObjectType() == "" {
			i.Checks[index].ObjectType = i.Default.GetObjectType()
		}
		if i.Checks[index].GetObjectId() == "" {
			i.Checks[index].ObjectId = i.Default.GetObjectId()
		}
		if i.Checks[index].GetRelation() == "" {
			i.Checks[index].Relation = i.Default.GetRelation()
		}
		if i.Checks[index].GetSubjectType() == "" {
			i.Checks[index].SubjectType = i.Default.GetSubjectType()
		}
		if i.Checks[index].GetSubjectId() == "" {
			i.Checks[index].SubjectId = i.Default.GetSubjectId()
		}
		if i.Default.GetTrace() {
			i.Checks[index].Trace = true
		}

		// send request to inbox
		inbox <- &checksReq{Index: index, CheckRequest: i.Checks[index]}
	}

	close(inbox)

	go func() {
		wg.Wait()
		close(outbox)
	}()

	resp := &dsr3.ChecksResponse{Checks: make([]*dsr3.CheckResponse, len(i.Checks))}

	for result := range outbox {
		resp.Checks[result.Index] = result.CheckResponse
	}

	return resp, nil
}

type checksReq struct {
	Index int
	*dsr3.CheckRequest
}

type checksResp struct {
	Index int
	*dsr3.CheckResponse
}
