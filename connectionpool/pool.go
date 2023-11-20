package connectionpool

import (
	"container/list"
	"context"
	"errors"
	"github.com/akley-MK4/simple-grpc/define"
	"github.com/akley-MK4/simple-grpc/logger"
	"google.golang.org/grpc"
	"time"
)

type PreemptFuncType func() (retConn *Connection, retErr error)

type KwArgsNewConnPool struct {
	Target   string
	DialOpts []grpc.DialOption

	NewConnTimeout time.Duration

	MaxConnNum           uint16
	MinIdleConnNum       uint16
	MaxIdleDurationMilli uint64
}

func NewConnectionPool(kw KwArgsNewConnPool) (*ConnectionPool, error) {
	// check parameters
	if kw.Target == "" || len(kw.DialOpts) <= 0 || kw.MaxConnNum <= 0 || kw.MinIdleConnNum > kw.MaxConnNum ||
		kw.MaxIdleDurationMilli <= 0 || kw.NewConnTimeout <= 0 {
		return nil, errors.New("there are invalid parameters")
	}

	var connList []*Connection
	for i := 0; i < int(kw.MaxConnNum); i++ {
		conn := newConnection(i, kw.Target, kw.DialOpts, kw.NewConnTimeout)
		connList = append(connList, conn)
	}

	for i := 0; i < int(kw.MinIdleConnNum); i++ {
		conn := connList[i]
		if err := conn.create(); err != nil {
			logger.GetLoggerInstance().WarningF("Failed to create connection. Id: %v, Target %v, Status: %v, Err: %v",
				conn.GetId(), conn.GetTarget(), conn.GetStatusDesc(), err)
			continue
		}
		conn.checkAndWaitForGRPCConnReady()
		conn.updateStatus()
		logger.GetLoggerInstance().DebugF("Successfully created connection. Id: %v, Target %v, Status: %v, "+
			"GRPCConnStatus: %v",
			conn.GetId(), conn.GetTarget(), conn.GetStatusDesc(), conn.GetGRPCConnStatus())
	}

	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	connPool := &ConnectionPool{
		kw:         &kw,
		connList:   connList,
		cancelCtx:  cancelCtx,
		cancelFunc: cancelFunc,
	}
	connPool.preemptFns = []PreemptFuncType{
		connPool.preemptIdleConn,
		connPool.preemptCreatedConn,
		connPool.preemptNotCreateConn,
	}

	return connPool, nil
}

type ConnectionPool struct {
	kw *KwArgsNewConnPool

	connList []*Connection
	//rwMutex  sync.RWMutex
	//idleConnList list.List

	preemptFns []PreemptFuncType

	cancelFunc context.CancelFunc
	cancelCtx  context.Context
	started    bool
}

func (t *ConnectionPool) Start() error {
	t.started = true
	go t.checkAndShrinkPeriodically()
	return nil
}

func (t *ConnectionPool) Stop() {
	if !t.started {
		return
	}

	t.cancelFunc()

	for _, conn := range t.GetConnections() {
		conn.stop()
	}

	t.connList = []*Connection{}
}

func (t *ConnectionPool) GetConnections() []*Connection {
	return t.connList
}

func (t *ConnectionPool) checkAndShrinkPeriodically() {
	logger.GetLoggerInstance().Debug("Start periodic check and shrink GRPC connection pool")
	timer := time.NewTicker(time.Millisecond * time.Duration(t.kw.MaxIdleDurationMilli))

loopEnd:
	for {
		select {
		case <-timer.C:
			if !t.checkAndShrink() {
				break loopEnd
			}
			break
		case <-t.cancelCtx.Done():
			break loopEnd
		}
	}

	timer.Stop()
	logger.GetLoggerInstance().Debug("Stop periodic checks and shrink GRPC connection pool")
}

func (t *ConnectionPool) checkAndShrink() bool {
	nowTp := time.Now().UnixMilli()
	connSpace := list.List{}

	for _, conn := range t.GetConnections() {
		connTp := conn.GetIdleSettingMilliTimestamp()
		if conn.GetStatus() != define.ConnStatusIdle || connTp <= 0 || nowTp <= connTp {
			continue
		}

		if uint64(nowTp-connTp) < t.kw.MaxIdleDurationMilli {
			continue
		}
		connSpace.PushBack(conn)
	}

	excessNum := connSpace.Len() - int(t.kw.MinIdleConnNum)
	if excessNum <= 0 {
		return true
	}

	for i := 0; i < excessNum; i++ {
		elem := connSpace.Back()
		connSpace.Remove(elem)
		conn := elem.Value.(*Connection)
		conn.switchFromIdleToNotCreateStatus()
	}

	if excessNum > 0 {
		logger.GetLoggerInstance().DebugF("In order to shrink the connection pool, %d idle connections were closed", excessNum)
	}

	return true
}

func (t *ConnectionPool) RecycleConnection(conn *Connection) error {
	if conn == nil {
		return errors.New("the parameter conn is a nil point")
	}

	conn.updateStatus()
	return nil
}

func (t *ConnectionPool) PreemptConnection() (retConn *Connection, retFnIdx int, retErr error) {
	for idx, fn := range t.preemptFns {
		retConn, retErr = fn()
		retFnIdx = idx
		if retErr == nil && retConn != nil {
			return
		}
	}

	if retConn == nil && retErr == nil {
		retErr = errors.New("did not preempt any connection")
	}

	return
}

func (t *ConnectionPool) preemptIdleConn() (retConn *Connection, retErr error) {
	for _, conn := range t.GetConnections() {
		success, err := conn.switchFromIdleToBusyStatus()
		if err != nil {
			retErr = err
			return
		}
		if success {
			retConn = conn
			return
		}
	}

	return
}

func (t *ConnectionPool) preemptNotCreateConn() (retConn *Connection, retErr error) {
	for _, conn := range t.GetConnections() {
		success, err := conn.switchFromNotCreateToBusyStatus()
		if err != nil {
			retErr = err
			return
		}
		if success {
			retConn = conn
			return
		}
	}

	return
}

func (t *ConnectionPool) preemptCreatedConn() (retConn *Connection, retErr error) {
	for _, conn := range t.GetConnections() {
		success, err := conn.switchFromCreatedToBusyStatus()
		if err != nil {
			retErr = err
			return
		}
		if success {
			retConn = conn
			return
		}
	}

	return
}
