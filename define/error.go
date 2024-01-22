package define

import "errors"

var (
	ErrorReachedMaxConnNumLimit = errors.New("unable to create a new gRPC connection, the maximum number of connections has already been reached")
	ErrorConnectionTimedOut     = errors.New("connection timed out, context deadline exceeded")
)
