package example

import (
	"fmt"
	"github.com/akley-MK4/simple-grpc/logger"
	"google.golang.org/grpc"
	"net"
)

const (
	listenPort = 8080
)

func RunBasicServerExample() error {
	if err := logger.SetLoggerInstance(newExample("[Server]")); err != nil {
		return err
	}

	listener, listenErr := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", listenPort))
	if listenErr != nil {
		return listenErr
	}

	svr := grpc.NewServer()
	if err := svr.Serve(listener); err != nil {
		return err
	}

	return nil
}
