package session

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aserto-dev/go-directory/pkg/derr"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var ErrMissingHeader = errors.New("missing required header")

type HeaderMiddleware struct {
	DisableValidation bool
}

func (m *HeaderMiddleware) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		newCtx, err := m.fromMetadata(ctx)
		if err != nil {
			return nil, err
		}

		return handler(newCtx, req)
	}
}

func (m *HeaderMiddleware) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()

		newCtx, err := m.fromMetadata(ctx)
		if err != nil {
			return err
		}

		wrapped := grpc_middleware.WrapServerStream(stream)
		wrapped.WrappedContext = newCtx
		return handler(srv, wrapped)
	}
}

func (m *HeaderMiddleware) HTTP(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := m.fromHeader(r)
		if err != nil {
			http.Error(w, fmt.Sprintf("%q", err.Error()), http.StatusBadRequest)
			return

		}

		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (m *HeaderMiddleware) fromMetadata(ctx context.Context) (context.Context, error) {
	md := metautils.ExtractIncoming(ctx)

	sessionID := md.Get(string(HeaderAsertoSessionID))
	if sessionID == "" {
		return ctx, nil
	}

	return m.withSessionID(ctx, sessionID)
}

func (m *HeaderMiddleware) fromHeader(r *http.Request) (context.Context, error) {
	sessionID := r.Header.Get(string(HeaderAsertoSessionID))
	if sessionID == "" {
		return r.Context(), nil
	}

	return m.withSessionID(r.Context(), sessionID)
}

func (m *HeaderMiddleware) withSessionID(ctx context.Context, sessionID string) (context.Context, error) {
	if sessionID == "" {
		return ctx, nil
	}
	if err := CheckSessionID(sessionID); err != nil {
		return ctx, derr.ErrInvalidID
	}
	return ContextWithSessionID(ctx, sessionID), nil
}
