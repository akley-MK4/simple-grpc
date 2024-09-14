package define

import "errors"

var (
	ErrorReachedMaxConnNumLimit  = errors.New("unable to create a new gRPC connection, the maximum number of connections has already been reached")
	ErrorConnectionTimedOut      = errors.New("connection timed out, context deadline exceeded")
	ErrReachedUpdateChanCapacity = errors.New("the capacity of the update event channel is insufficient")
	ErrTrcReconnect              = errors.New("a gRPC connection has been disconnected and attempted to reconnect, but still failed to connect")
)
