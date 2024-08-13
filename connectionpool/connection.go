package connectionpool

import (
	"context"
	"errors"
	"fmt"
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

	checkStatusTimeAfterConnect = time.Millisecond * 200
)

var (
	incConnId uint64
)

type connSwitchToBusyUsingStatusFunc func() (switched bool, retErr error)

func newConnection(target string, opts []grpc.DialOption, connectTimeout time.Duration) *Connection {
	conn := &Connection{
		target:         target,
		connectTimeout: connectTimeout,
		opts:           opts,
		id:             atomic.AddUint64(&incConnId, 1),
		usingStatus:    define.NotOpenUsingStatus,
		grpcConn:       nil,
	}

	return conn
}

type Connection struct {
	target         string
	connectTimeout time.Duration
	opts           []grpc.DialOption

	id           uint64
	usingStatus  uintptr
	idledMilliTp int64
	grpcConn     *grpc.ClientConn
}

func (t *Connection) GetId() uint64 {
	return t.id
}

func (t *Connection) GetUsingStatus() uintptr {
	return t.usingStatus
}

func (t *Connection) GetUsingStatusDesc() string {
	return define.GetUsingStatusDesc(t.usingStatus)
}

func (t *Connection) setUsingStatus(status uintptr) {
	if status == define.IdledUsingStatus {
		t.idledMilliTp = time.Now().UnixMilli()
	}
	t.usingStatus = status
}

func (t *Connection) GetTarget() string {
	return t.target
}

func (t *Connection) GetGRPCConn() *grpc.ClientConn {
	return t.grpcConn
}

func (t *Connection) GetIdledMilliTimestamp() int64 {
	return t.idledMilliTp
}

func (t *Connection) GetConnStatus() connectivity.State {
	if t.grpcConn == nil {
		return connectivity.Shutdown
	}

	return t.grpcConn.GetState()
}

func (t *Connection) stop(force bool) error {
	switchedStatus := false
	for _, oldStatus := range prioritySwitchFromStatusListToStopping {
		if atomic.CompareAndSwapUintptr(&t.usingStatus, oldStatus, define.StoppingUsingStatus) {
			switchedStatus = true
			break
		}
	}

	if !switchedStatus && !force {
		return fmt.Errorf("current using status is %v and cannot be closed", t.usingStatus)
	}

	grpcConn := t.grpcConn
	if grpcConn != nil {
		if err := grpcConn.Close(); err != nil {
			logger.GetLoggerInstance().WarningF("Failed to close a gRPC connection, Id: %v, Target %v, UsingStatus: %v, ConnStatus: %v, Err: %v",
				t.GetId(), t.GetTarget(), t.GetUsingStatusDesc(), grpcConn.GetState(), err)
		}
	}

	t.usingStatus = define.StoppedUsingStatus
	t.opts = []grpc.DialOption{}
	return nil
}

func (t *Connection) open() error {
	ctx, cancel := context.WithTimeout(context.Background(), t.connectTimeout)
	defer cancel()

	grpcConn, err := grpc.DialContext(ctx, t.target, append(t.opts, grpc.WithReturnConnectionError())...)
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
		time.Sleep(checkStatusTimeAfterConnect)
		if t.grpcConn.GetState() == connectivity.Ready {
			retSuccess = true
			return
		}
	}

	return
}

func (t *Connection) IsIdleUsingStatus() bool {
	return t.grpcConn != nil && t.usingStatus == define.IdledUsingStatus
}

func (t *Connection) updateUsingStatus() (switchedIdledStatus bool) {
	grpcConn := t.grpcConn
	if grpcConn == nil {
		t.setUsingStatus(define.NotOpenUsingStatus)
		return
	}

	if grpcConn.GetState() == connectivity.Ready {
		t.setUsingStatus(define.IdledUsingStatus)
		switchedIdledStatus = true
		return
	}

	t.setUsingStatus(define.DisconnectedUsingStatus)
	return
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

func (t *Connection) switchFromIdleToBusyUsingStatus() (switched bool, retErr error) {
	if !atomic.CompareAndSwapUintptr(&t.usingStatus, define.IdledUsingStatus, define.BusyUsingStatus) {
		return
	}

	if t.grpcConn == nil {
		t.usingStatus = define.NotOpenUsingStatus
		retErr = errors.New("no gRPC connection opened")
		return
	}

	switched = t.checkAndWaitForGRPCConnReady()
	if !switched {
		t.usingStatus = define.DisconnectedUsingStatus
	}

	return
}

func (t *Connection) switchFromNotOpenToBusyUsingStatus() (switched bool, retErr error) {
	if !atomic.CompareAndSwapUintptr(&t.usingStatus, define.NotOpenUsingStatus, define.BusyUsingStatus) {
		return
	}

	if err := t.open(); err != nil {
		t.usingStatus = define.NotOpenUsingStatus
		retErr = err
		return
	}

	switched = t.checkAndWaitForGRPCConnReady()
	if !switched {
		t.usingStatus = define.DisconnectedUsingStatus
	}

	return
}

func (t *Connection) switchFromDisconnectedToBusyUsingStatus() (switched bool, retErr error) {
	if !atomic.CompareAndSwapUintptr(&t.usingStatus, define.DisconnectedUsingStatus, define.BusyUsingStatus) {
		return
	}

	if t.grpcConn == nil {
		t.usingStatus = define.NotOpenUsingStatus
		retErr = errors.New("no gRPC connection opened")
		return
	}

	switched = t.checkAndWaitForGRPCConnReady()
	if !switched {
		t.usingStatus = define.DisconnectedUsingStatus
	}

	return
}

func (t *Connection) switchFromIdledToNotOpenUsingStatus() bool {
	if !atomic.CompareAndSwapUintptr(&t.usingStatus, define.IdledUsingStatus, define.ClosingUsingStatus) {
		return false
	}

	if t.grpcConn == nil {
		t.usingStatus = define.NotOpenUsingStatus
		return true
	}

	if err := t.grpcConn.Close(); err != nil {
		logger.GetLoggerInstance().WarningF("Failed to close a GRPC connection, Id: %v, Target %v, UsingStatus: %v, ConnStatus: %v",
			t.GetId(), t.GetTarget(), t.GetUsingStatusDesc(), t.grpcConn.GetState(), err)
	}

	t.grpcConn = nil
	t.usingStatus = define.NotOpenUsingStatus

	return true
}
