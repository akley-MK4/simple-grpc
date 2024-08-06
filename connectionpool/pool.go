package connectionpool

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/akley-MK4/simple-grpc/define"
	"github.com/akley-MK4/simple-grpc/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"sync"
	"sync/atomic"
	"time"
)

var (
	prioritySwitchFromStatusListToBusy = []uintptr{
		define.IdledUsingStatus,
		define.DisconnectedUsingStatus,
		define.NotOpenUsingStatus,
	}

	prioritySwitchFromStatusListToIdle = []uintptr{
		define.DisconnectedUsingStatus,
		define.NotOpenUsingStatus,
	}

	prioritySwitchFromStatusListToStopping = []uintptr{
		define.IdledUsingStatus,
		define.DisconnectedUsingStatus,
		define.NotOpenUsingStatus,
	}
)

type KwArgsConnPool struct {
	Target   string
	DialOpts []grpc.DialOption

	NewConnTimeout time.Duration

	MaxConnNum                   int
	MinIdledConnNum              int
	MaxIdledDurationMilliseconds uint64
	CheckIdledConnNumInterval    time.Duration
}

func NewConnectionPool(kw KwArgsConnPool) (*ConnectionPool, error) {
	// check parameters
	if kw.Target == "" || len(kw.DialOpts) <= 0 || kw.MaxConnNum <= 0 || kw.MinIdledConnNum > kw.MaxConnNum ||
		kw.MaxIdledDurationMilliseconds <= 0 || kw.NewConnTimeout <= 0 || kw.CheckIdledConnNumInterval <= 0 {
		return nil, errors.New("invalid parameters")
	}

	var connList []*Connection
	readyConnCount := 0
	for i := 0; i < kw.MinIdledConnNum; i++ {
		conn := newConnection(kw.Target, kw.DialOpts, kw.NewConnTimeout)
		connList = append(connList, conn)

		// Open the gRPC connection
		if err := conn.open(); err != nil {
			if err.Error() == "context deadline exceeded" {
				err = define.ErrorConnectionTimedOut
			}

			logger.GetLoggerInstance().WarningF("Failed to create a gRPC connection, Id: %v, Target %v, UsingStatus: %v, Err: %v",
				conn.GetId(), conn.GetTarget(), conn.GetUsingStatusDesc(), err)
			continue
		}
		conn.setUsingStatus(define.DisconnectedUsingStatus)

		// Check and wait for the status of the connection to be ready
		if !conn.checkAndWaitForGRPCConnReady() {
			logger.GetLoggerInstance().WarningF("Unable to switch a gRPC connection to Ready status, "+
				"Id: %v, Target %v, UsingStatus: %v, ConnStatus: %v",
				conn.GetId(), conn.GetTarget(), conn.GetUsingStatusDesc(), conn.GetConnStatus())
			continue
		}
		conn.setUsingStatus(define.IdledUsingStatus)
		readyConnCount++
		//logger.GetLoggerInstance().DebugF("Successfully created a gRPC connection, Id: %v, Target %v, UsingStatus: %v, "+
		//	"ConnStatus: %v",
		//	conn.GetId(), conn.GetTarget(), conn.GetUsingStatusDesc(), conn.GetConnStatus())
	}

	logger.GetLoggerInstance().DebugF("Successfully created %d ready gRPC connections", readyConnCount)
	connPool := &ConnectionPool{
		kw:             &kw,
		connList:       connList,
		status:         define.InitializedPoolStatus,
		idledConnCount: int32(readyConnCount),
	}
	connPool.shrinkCancelCtx, connPool.shrinkCancelFunc = context.WithCancel(context.Background())
	connPool.keepActiveCtx, connPool.keepActiveCancelFunc = context.WithCancel(context.Background())

	return connPool, nil
}

type ConnectionPool struct {
	kw *KwArgsConnPool

	status uintptr

	connList []*Connection
	rwMutex  sync.RWMutex

	shrinkCancelFunc     context.CancelFunc
	shrinkCancelCtx      context.Context
	keepActiveCancelFunc context.CancelFunc
	keepActiveCtx        context.Context

	idledConnCount int32
	busyConnCount  int32
}

func (t *ConnectionPool) GetStatus() uintptr {
	return t.status
}

func (t *ConnectionPool) GetIdledConnectionsCount() int32 {
	return t.idledConnCount
}

func (t *ConnectionPool) GetBusyConnectionsCount() int32 {
	return t.busyConnCount
}

func (t *ConnectionPool) Start() error {
	if !atomic.CompareAndSwapUintptr(&t.status, define.InitializedPoolStatus, define.StartingPoolStatus) {
		return fmt.Errorf("wrong old pool status %v", t.status)
	}

	go t.checkAndShrinkIdledConnectionsPeriodically()
	go t.checkAndAddIdledConnectionsPeriodically()

	t.status = define.StartedPoolStatus
	return nil
}

func (t *ConnectionPool) Stop(disableClean bool) error {
	if !atomic.CompareAndSwapUintptr(&t.status, define.StartedPoolStatus, define.StoppingPoolStatus) {
		return fmt.Errorf("wrong old pool status %v", t.status)
	}

	t.shrinkCancelFunc()
	t.keepActiveCancelFunc()

	for _, conn := range t.getConnections() {
		if err := conn.stop(); err != nil {
			logger.GetLoggerInstance().WarningF("Failed to close a gRPC connection, Id: %v, Target %v, UsingStatus: %v, ConnStatus: %v, Err: %v",
				conn.GetId(), conn.GetTarget(), conn.GetUsingStatusDesc(), conn.GetConnStatus(), err)
		}
	}

	if !disableClean {
		t.connList = []*Connection{}
	}

	t.status = define.StoppedPoolStatus
	return nil
}

func (t *ConnectionPool) AllocateConnection() (retConn *Connection, retErr error) {
	if t.status != define.StartedPoolStatus {
		retErr = fmt.Errorf("wrong pool status %v", t.status)
		return
	}

	if int(atomic.LoadInt32(&t.busyConnCount)) >= t.kw.MaxConnNum {
		retErr = define.ErrorReachedMaxConnNumLimit
		return
	}

	defer func() {
		if retErr == nil && retConn != nil {
			atomic.AddInt32(&t.busyConnCount, 1)
		}
	}()

	for _, oldStatus := range prioritySwitchFromStatusListToBusy {
		if oldStatus == define.IdledUsingStatus && atomic.LoadInt32(&t.idledConnCount) <= 0 {
			continue
		}

		retConn, retErr = t.allocateConnectionByUsingStatus(oldStatus, t.getConnections())
		if retErr != nil {
			return
		}
		if retConn == nil {
			continue
		}

		if oldStatus == define.IdledUsingStatus {
			atomic.AddInt32(&t.idledConnCount, -1)
		}
		return
	}

	retConn, retErr = t.allocateNewConnection()
	return
}

func (t *ConnectionPool) RecycleConnection(conn *Connection) error {
	if conn == nil {
		return errors.New("the parameter conn is a nil point")
	}
	if t.status != define.StartedPoolStatus {
		return fmt.Errorf("wrong pool status %v", t.status)
	}

	//if conn.GetUsingStatus() != define.BusyUsingStatus {
	//	return fmt.Errorf("wrong using status %v", conn.GetUsingStatus())
	//}

	if conn.updateUsingStatus() {
		atomic.AddInt32(&t.idledConnCount, 1)
	}
	atomic.AddInt32(&t.busyConnCount, -1)
	return nil
}

func (t *ConnectionPool) getConnections() []*Connection {
	return t.connList
}

func (t *ConnectionPool) GetConnectionsCount() int {
	return len(t.connList)
}

func (t *ConnectionPool) GetReadyConnectionsCount() (retCount int) {
	for _, conn := range t.getConnections() {
		if conn.GetConnStatus() == connectivity.Ready {
			retCount++
		}
	}
	return
}

func (t *ConnectionPool) GetUsingStatusConnectionsCount(usingStatus uintptr) (retCount int) {
	for _, conn := range t.getConnections() {
		if conn.GetUsingStatus() == usingStatus {
			retCount++
		}
	}
	return
}

func (t *ConnectionPool) checkAndAddIdledConnectionsPeriodically() {
	logger.GetLoggerInstance().Debug("Started periodic check and add the gRPC connections of the idled status")
	timer := time.NewTicker(t.kw.CheckIdledConnNumInterval)

loopEnd:
	for {
		select {
		case <-timer.C:
			if t.status == define.StoppedPoolStatus {
				break loopEnd
			}

			newActivatedCount := t.checkAndAddIdledConnections()
			if newActivatedCount > 0 {
				logger.GetLoggerInstance().DebugF("Successfully added %d gRPC connections of the idled status", newActivatedCount)
			}
			break
		case <-t.keepActiveCtx.Done():
			break loopEnd
		}
	}

	timer.Stop()
	logger.GetLoggerInstance().Debug("Exited periodic check and add the gRPC connections of the idled status")
}

func (t *ConnectionPool) checkAndAddIdledConnections() (newIdledConnCount int) {
	idledCount := t.GetUsingStatusConnectionsCount(define.IdledUsingStatus)
	if idledCount >= t.kw.MinIdledConnNum {
		return
	}

	for i := 0; i < t.kw.MinIdledConnNum-idledCount; i++ {
		for _, oldStatus := range prioritySwitchFromStatusListToIdle {
			conn, errSwitch := t.allocateConnectionByUsingStatus(oldStatus, t.getConnections())
			if errSwitch != nil {
				logger.GetLoggerInstance().WarningF("Failed to add an idled gRPC connection, %v", errSwitch)
				continue
			}
			if conn == nil {
				continue
			}

			atomic.AddInt32(&t.busyConnCount, 1)
			// switch to idle status
			if err := t.RecycleConnection(conn); err != nil {
				logger.GetLoggerInstance().WarningF("Failed to add an idled gRPC connection, unable to recycle the connection, "+
					"Id: %v, Target %v, UsingStatus: %v, ConnStatus: %v, Err: %v",
					conn.GetId(), conn.GetTarget(), conn.GetUsingStatusDesc(), conn.GetConnStatus(), err)
				continue
			}

			newIdledConnCount++
		}
	}

	return
}

func (t *ConnectionPool) checkAndShrinkIdledConnectionsPeriodically() {
	logger.GetLoggerInstance().Debug("Started periodic check and shrink the gRPC connections of the idled status")
	timer := time.NewTicker(time.Millisecond * time.Duration(t.kw.MaxIdledDurationMilliseconds))

loopEnd:
	for {
		select {
		case <-timer.C:
			if t.status == define.StoppedPoolStatus {
				break loopEnd
			}
			closedCount := t.checkAndShrinkIdledConnections()
			if closedCount > 0 {
				logger.GetLoggerInstance().DebugF("Shrinked %d gRPC connections of the idled status", closedCount)
			}
			break
		case <-t.shrinkCancelCtx.Done():
			break loopEnd
		}
	}

	timer.Stop()
	logger.GetLoggerInstance().Debug("Exited periodic check and shrink the gRPC connections of the idle status")
}

func (t *ConnectionPool) checkAndShrinkIdledConnections() (retClosedCount int) {
	closedCount := uint64(0)

	for _, conn := range t.getConnections() {
		if conn.GetUsingStatus() != define.IdledUsingStatus || conn.GetConnStatus() == connectivity.Ready {
			continue
		}
		if conn.switchFromIdledToNotOpenUsingStatus() {
			closedCount++
			atomic.AddInt32(&t.idledConnCount, -1)
		}
	}

	nowTp := time.Now().UnixMilli()
	var expiredConnList list.List

	for _, conn := range t.getConnections() {
		idledTp := conn.GetIdledMilliTimestamp()
		if conn.GetUsingStatus() != define.IdledUsingStatus || idledTp <= 0 || nowTp <= idledTp {
			continue
		}

		if uint64(nowTp-idledTp) >= t.kw.MaxIdledDurationMilliseconds {
			expiredConnList.PushBack(conn)
		}
	}

	if expiredConnList.Len() <= t.kw.MinIdledConnNum {
		return
	}

	// Prioritize closing connections at the back of the queue
	needClosedCount := expiredConnList.Len() - t.kw.MinIdledConnNum
	var wg sync.WaitGroup
	wg.Add(needClosedCount)

	for i := 0; i < needClosedCount; i++ {
		elem := expiredConnList.Back()
		expiredConnList.Remove(elem)
		conn := elem.Value.(*Connection)
		go func(inConn *Connection, inWg *sync.WaitGroup) {
			defer wg.Done()

			if conn.switchFromIdledToNotOpenUsingStatus() {
				atomic.AddInt32(&t.idledConnCount, -1)
				atomic.AddUint64(&closedCount, 1)
			}
		}(conn, &wg)
	}
	wg.Wait()

	retClosedCount = int(closedCount)
	return
}

func (t *ConnectionPool) allocateConnectionByUsingStatus(oldStatus uintptr, connList []*Connection) (retConn *Connection, retErr error) {
	for _, conn := range connList {
		switched := false
		var errSwitch error = nil
		var switchUsingStatusFunc connSwitchToBusyUsingStatusFunc

		switch oldStatus {
		case define.IdledUsingStatus:
			switchUsingStatusFunc = conn.switchFromIdleToBusyUsingStatus
			break
		case define.DisconnectedUsingStatus:
			switchUsingStatusFunc = conn.switchFromDisconnectedToBusyUsingStatus
			break
		case define.NotOpenUsingStatus:
			switchUsingStatusFunc = conn.switchFromNotOpenToBusyUsingStatus
			break
		}

		if switchUsingStatusFunc == nil {
			retErr = fmt.Errorf("old status %v cannot switch to busy using status", define.GetUsingStatusDesc(oldStatus))
			return
		}

		switched, errSwitch = switchUsingStatusFunc()
		if errSwitch != nil {
			if errSwitch.Error() == "context deadline exceeded" {
				errSwitch = define.ErrorConnectionTimedOut
			}

			retErr = fmt.Errorf("an error occurs when switching the using status of a gRPC connection to busy using status, "+
				"Id: %v, Target %v, UsingStatus: %v, ConnStatus: %v, Err: %v",
				conn.GetId(), conn.GetTarget(), conn.GetUsingStatusDesc(), conn.GetConnStatus(), errSwitch)
			return
		}

		if switched {
			retConn = conn
			return
		}
	}

	return
}

func (t *ConnectionPool) allocateNewConnection() (retConn *Connection, retErr error) {
	t.rwMutex.Lock()
	if len(t.connList) >= t.kw.MaxConnNum {
		t.rwMutex.Unlock()
		retErr = define.ErrorReachedMaxConnNumLimit
		return
	}

	newConn := newConnection(t.kw.Target, t.kw.DialOpts, t.kw.NewConnTimeout)
	newConn.setUsingStatus(define.WaitConnectUsingStatus)
	t.connList = append([]*Connection{newConn}, t.connList...)
	t.rwMutex.Unlock()

	if err := newConn.open(); err != nil {
		newConn.setUsingStatus(define.NotOpenUsingStatus)
		if err.Error() == "context deadline exceeded" {
			err = define.ErrorConnectionTimedOut
		}
		retErr = err
		return
	}

	if newConn.checkAndWaitForGRPCConnReady() {
		newConn.setUsingStatus(define.BusyUsingStatus)
		retConn = newConn
		return
	}

	newConn.setUsingStatus(define.DisconnectedUsingStatus)
	retErr = errors.New("unable to switch to Ready status")
	return
}
