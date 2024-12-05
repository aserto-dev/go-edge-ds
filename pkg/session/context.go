package session

// import (
// 	"context"
// 	"net/textproto"
// 	"strings"

// 	"github.com/google/uuid"
// )

// type CtxKey string

// var (
// 	HeaderAsertoSessionID          = CtxKey(textproto.CanonicalMIMEHeaderKey("Aserto-Session-Id"))
// 	HeaderAsertoSessionIDLowercase = CtxKey(strings.ToLower(string(HeaderAsertoSessionID)))
// )

// // NewSessionID creates a new session ID.
// func NewSessionID() (string, error) {
// 	id, err := uuid.NewUUID()
// 	return id.String(), err
// }

// // CheckSessionID returns an error if the id doesn't look like an session ID.
// func CheckSessionID(id string) error {
// 	_, err := uuid.Parse(id)
// 	return err
// }

// // ExtractSessionID extracts a session id from a context.
// func ExtractSessionID(ctx context.Context) string {
// 	id, ok := ctx.Value(HeaderAsertoSessionID).(string)
// 	if !ok {
// 		return ""
// 	}

// 	return id
// }

// func ContextWithSessionID(ctx context.Context, sessionID string) context.Context {
// 	return context.WithValue(ctx, HeaderAsertoSessionID, sessionID)
// }
