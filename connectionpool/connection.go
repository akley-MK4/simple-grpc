package connectionpool

import (
	"context"
	"errors"
	"github.com/akley-MK4/simple-grpc/define"
	"github.com/akley-MK4/simple-grpc/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"sync/atomic"
	"time"
)

const (
	maxRetryConnectNum = 3

	onceAttemptWaitGRPCConnStatusSec = 3
	maxAttemptWaitGRPCConnStatusNum  = 2
)

func newConnection(id int, target string, opts []grpc.DialOption, connectTimeout time.Duration) *Connection {
	conn := &Connection{
		target:         target,
		connectTimeout: connectTimeout,
		opts:           opts,
		id:             id,
		status:         define.ConnStatusNotCreate,
		grpcConn:       nil,
	}

	return conn
}

type Connection struct {
	target         string
	connectTimeout time.Duration
	opts           []grpc.DialOption

	id            int
	status        uint32
	idleSettingTp int64
	grpcConn      *grpc.ClientConn
}

func (t *Connection) create() error {
	ctx, cancel := context.WithTimeout(context.Background(), t.connectTimeout)
	defer cancel()

	grpcConn, err := grpc.DialContext(ctx, t.target, append(t.opts, grpc.WithBlock())...)
	if err != nil {
		return err
	}

	t.grpcConn = grpcConn
	return nil
}

func (t *Connection) connect() (retSuccess bool, retErr error) {
	if t.grpcConn == nil {
		retErr = errors.New("the GRPC connection is nil")
		return
	}

	for i := 0; i < maxRetryConnectNum; i++ {
		t.grpcConn.Connect()
		time.Sleep(time.Second)
		if t.grpcConn.GetState() == connectivity.Ready {
			retSuccess = true
			return
		}
	}

	return
}

func (t *Connection) GetStatus() uint32 {
	return t.status
}

func (t *Connection) setStatusStatus(status uint32) {
	if status == define.ConnStatusIdle {
		t.idleSettingTp = time.Now().Unix()
	}
	t.status = status

}

func (t *Connection) IsIdleStatus() bool {
	return t.grpcConn != nil && t.status == define.ConnStatusIdle
}

func (t *Connection) GetGRPCConn() *grpc.ClientConn {
	return t.grpcConn
}

func (t *Connection) GetIdleSettingTimestamp() int64 {
	return t.idleSettingTp
}

func (t *Connection) updateStatus() {
	grpcConn := t.grpcConn
	if grpcConn == nil {
		t.setStatusStatus(define.ConnStatusNotCreate)
		return
	}

	if grpcConn.GetState() == connectivity.Ready {
		t.setStatusStatus(define.ConnStatusIdle)
		return
	}
	t.setStatusStatus(define.ConnStatusCreated)
}

func (t *Connection) checkAndWaitForGRPCConnReady() bool {
	switch t.grpcConn.GetState() {
	case connectivity.Ready:
		return true
	case connectivity.Idle:
		t.grpcConn.Connect()
		break
	}

	for i := 0; i < maxAttemptWaitGRPCConnStatusNum; i++ {
		if t.waitForGrpcConnReady() {
			return true
		}
	}

	return false
}

func (t *Connection) waitForGrpcConnReady() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*onceAttemptWaitGRPCConnStatusSec)
	defer cancel()

	_ = t.grpcConn.WaitForStateChange(ctx, t.grpcConn.GetState())
	return t.grpcConn.GetState() == connectivity.Ready
}

func (t *Connection) switchFromIdleToBusyStatus() (retSuccess bool, retErr error) {
	if !atomic.CompareAndSwapUint32(&t.status, define.ConnStatusIdle, define.ConnStatusBusy) {
		return
	}

	if t.checkAndWaitForGRPCConnReady() {
		retSuccess = true
		return
	}

	t.status = define.ConnStatusCreated
	retSuccess = false
	return
}

func (t *Connection) switchFromNotCreateToBusyStatus() (retSuccess bool, retErr error) {
	if !atomic.CompareAndSwapUint32(&t.status, define.ConnStatusNotCreate, define.ConnStatusBusy) {
		return
	}

	if err := t.create(); err != nil {
		t.status = define.ConnStatusNotCreate
		retErr = err
		return
	}

	if t.checkAndWaitForGRPCConnReady() {
		retSuccess = true
		return
	}

	t.status = define.ConnStatusCreated
	return
}

func (t *Connection) switchFromCreatedToBusyStatus() (retSuccess bool, retErr error) {
	if !atomic.CompareAndSwapUint32(&t.status, define.ConnStatusCreated, define.ConnStatusBusy) {
		return
	}

	if t.checkAndWaitForGRPCConnReady() {
		retSuccess = true
		return
	}

	t.status = define.ConnStatusCreated
	return
}

func (t *Connection) switchFromIdleToNotCreateStatus() {
	if !atomic.CompareAndSwapUint32(&t.status, define.ConnStatusIdle, define.ConnStatusClosing) {
		return
	}

	go func() {
		if err := t.grpcConn.Close(); err != nil {
			logger.GetLoggerInstance().WarningF("Failed to close GRPC connection, %v", err)
		}
		t.grpcConn = nil
		t.status = define.ConnStatusNotCreate
	}()

	return
}
