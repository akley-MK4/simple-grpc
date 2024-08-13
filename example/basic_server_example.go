package example

import (
	"fmt"
	"github.com/akley-MK4/simple-grpc/logger"
	"google.golang.org/grpc"
	"net"
	"sync"
)

const (
	listenPort  = 8080
	listenPort2 = 8081
)

var (
	logInitOnce = sync.Once{}
)

func GetPortList() []int {
	return []int{listenPort, listenPort2}
}

func RunBasicServerExample(port int) (retErr error) {
	logInitOnce.Do(func() {
		if err := logger.SetLoggerInstance(newExample("[Server]")); err != nil {
			retErr = err
		}
	})
	if retErr != nil {
		return
	}

	listener, listenErr := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if listenErr != nil {
		return listenErr
	}

	svr := grpc.NewServer()
	if err := svr.Serve(listener); err != nil {
		return err
	}

	return nil
}
