package connectionpool

import (
	"container/list"
	"context"
	"errors"
	"github.com/akley-MK4/simple-grpc/define"
	"github.com/akley-MK4/simple-grpc/logger"
	"sync"
	"time"
)

type PreemptFuncType func() (retConn *Connection, retErr error)

type KwArgsNewConnPool struct {
	Addr               string
	Port               uint16
	CaFilePath         string
	AdditionalDialOpts []grpc.DialOption

	NewConnTimeout time.Duration

	MaxConnNum             uint16
	MinIdleConnNum         uint16
	MaxIdleDurationSeconds uint64
}

func NewConnectionPool(kw KwArgsNewConnPool) (*ConnectionPool, error) {
	// check parameters
	if kw.Addr == "" || kw.Port <= 0 || kw.MaxConnNum <= 0 || kw.MinIdleConnNum > kw.MaxConnNum ||
		kw.MaxIdleDurationSeconds <= 0 || kw.NewConnTimeout <= 0 {
		return nil, errors.New("there are invalid parameters")
	}

	dialOpts, buildOptsErr := buildDialOptions(&kw)
	if buildOptsErr != nil {
		return nil, buildOptsErr
	}

	target := fmtTargetAddr(kw.Addr, kw.Port)

	var connList []*Connection
	for i := 0; i < int(kw.MaxConnNum); i++ {
		conn := newConnection(i, target, dialOpts, kw.NewConnTimeout)
		connList = append(connList, conn)
	}

	for i := 0; i < int(kw.MinIdleConnNum); i++ {
		conn := connList[i]
		if err := conn.create(); err != nil {
			logger.GetLoggerInstance().WarningF("Failed to create GRPC connection.target %v, %v", target, err)
			continue
		}
		conn.checkAndWaitForGRPCConnReady()
	}

	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	connPool := &ConnectionPool{
		kw:         &kw,
		target:     target,
		dialOpts:   dialOpts,
		connList:   connList,
		cancelCtx:  cancelCtx,
		cancelFunc: cancelFunc,
	}
	connPool.preemptFns = []PreemptFuncType{
		connPool.preemptIdleConn,
		connPool.preemptCreatedConn,
		connPool.preemptNotActiveConn,
	}

	return connPool, nil
}

type ConnectionPool struct {
	kw     *KwArgsNewConnPool
	target string

	dialOpts []grpc.DialOption

	connList []*Connection
	rwMutex  sync.RWMutex
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
}

func (t *ConnectionPool) checkAndShrinkPeriodically() {
	logger.GetLoggerInstance().Info("Start periodic check and shrink GRPC connection pool")
	interval := time.Second * time.Duration(t.kw.MaxIdleDurationSeconds)
	timer := time.NewTicker(interval)

Loop:
	for {
		select {
		case <-timer.C:
			if !t.checkAndShrink() {
				break Loop
			}
			break
		case <-t.cancelCtx.Done():
			break Loop
		}
	}

	timer.Stop()
	logger.GetLoggerInstance().Info("Stop periodic checks and shrink GRPC connection pool")
}

func (t *ConnectionPool) checkAndShrink() bool {
	nowTp := time.Now().Unix()
	connSpace := list.List{}

	for _, conn := range t.connList {
		connTp := conn.GetIdleSettingTimestamp()
		if conn.GetStatus() != define.ConnStatusIdle || connTp <= 0 || nowTp <= connTp {
			continue
		}
		if uint64(nowTp-connTp) < t.kw.MaxIdleDurationSeconds {
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

	return true
}

func (t *ConnectionPool) RecycleConnection(conn *Connection) error {
	if conn == nil {
		return errors.New("the parameter conn is a nil point")
	}

	conn.updateStatus()
	return nil
}

func (t *ConnectionPool) PreemptConnection() (retConn *Connection, retErr error) {
	for _, fn := range t.preemptFns {
		retConn, retErr = fn()
		if retErr == nil && retConn != nil {
			return
		}
	}

	return
}

func (t *ConnectionPool) preemptIdleConn() (retConn *Connection, retErr error) {
	for _, conn := range t.connList {
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

func (t *ConnectionPool) preemptNotActiveConn() (retConn *Connection, retErr error) {
	for _, conn := range t.connList {
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
	for _, conn := range t.connList {
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
