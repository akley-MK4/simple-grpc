package connectionpool

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akley-MK4/simple-grpc/define"
	"github.com/akley-MK4/simple-grpc/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const (
	MaxCheckBusyConnNum   = 30
	MaxUpdateChanCapacity = 5
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
	MinReadyConnsNum             int
	MaxIdledDurationMilliseconds uint64
	ForceCloseExpiredIdledConns  bool
	CheckAndUpdateInterval       time.Duration
	OnAddReadyConnections        OnAddReadyConnectionsFunc
}

func NewConnectionPool(kw KwArgsConnPool) (*ConnectionPool, error) {
	// check parameters
	if kw.Target == "" || len(kw.DialOpts) <= 0 || kw.MaxConnNum <= 0 || kw.MinReadyConnsNum > kw.MaxConnNum ||
		kw.MaxIdledDurationMilliseconds <= 0 || kw.NewConnTimeout <= 0 || kw.CheckAndUpdateInterval <= 0 {
		return nil, errors.New("invalid parameters")
	}

	var connList []*Connection
	readyConnCount := 0
	for i := 0; i < kw.MinReadyConnsNum; i++ {
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
		kw:                    &kw,
		connList:              connList,
		status:                define.InitializedPoolStatus,
		idledConnCount:        int32(readyConnCount),
		updateChan:            make(chan UpdateEventContext, MaxUpdateChanCapacity),
		onAddReadyConnections: kw.OnAddReadyConnections,
	}

	connPool.checkCancelCtx, connPool.checkCancelFunc = context.WithCancel(context.Background())
	return connPool, nil
}

type UpdateEventContext struct {
	Kw *KwArgsConnPool
}

type OnAddReadyConnectionsFunc func(addedReadyConnsNum int)

type ConnectionPool struct {
	kw *KwArgsConnPool

	status uintptr

	connList []*Connection
	rwMutex  sync.RWMutex

	checkCancelFunc context.CancelFunc
	checkCancelCtx  context.Context

	idledConnCount int32
	busyConnCount  int32

	updateChan chan UpdateEventContext

	onAddReadyConnections OnAddReadyConnectionsFunc
}

func (t *ConnectionPool) GetStatus() uintptr {
	return t.status
}

func (t *ConnectionPool) GetStatusDesc() string {
	return define.GetPoolStatusDesc(t.status)
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

	go t.checkAndUpdateConnectionsPeriodically()

	t.status = define.RunningPoolStatus
	return nil
}

func (t *ConnectionPool) Stop(disableClean bool) error {
	if !atomic.CompareAndSwapUintptr(&t.status, define.RunningPoolStatus, define.StoppingPoolStatus) {
		return fmt.Errorf("wrong old pool status %v", t.status)
	}

	t.checkCancelFunc()

	for _, conn := range t.getConnections() {
		if err := conn.stop(false); err != nil {
			logger.GetLoggerInstance().WarningF("Failed to close a gRPC connection, Id: %v, Target %v, UsingStatus: %v, ConnStatus: %v, Err: %v",
				conn.GetId(), conn.GetTarget(), conn.GetUsingStatusDesc(), conn.GetConnStatus(), err)
		}
	}

	if !disableClean {
		t.rwMutex.Lock()
		t.connList = []*Connection{}
		t.rwMutex.Unlock()
	}

	t.status = define.StoppedPoolStatus
	return nil
}

func (t *ConnectionPool) AllocateConnection() (retConn *Connection, retErr error) {
	if t.status != define.RunningPoolStatus {
		retErr = fmt.Errorf("wrong pool status %v", define.GetPoolStatusDesc(t.status))
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

		return
	}

	retConn, retErr = t.allocateNewConnection()
	return
}

func (t *ConnectionPool) RecycleConnection(conn *Connection) error {
	if conn == nil {
		return errors.New("the parameter conn is a nil point")
	}
	if t.status != define.RunningPoolStatus && t.status != define.UpdatingPoolStatus {
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
	t.rwMutex.RLock()
	defer t.rwMutex.RUnlock()

	return t.connList
}

func (t *ConnectionPool) GetConnectionsCount() int {
	t.rwMutex.RLock()
	defer t.rwMutex.RUnlock()

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

func (t *ConnectionPool) checkAndAddReadyConnections() (retCount int) {
	readyConnsCount := t.GetReadyConnectionsCount()
	if readyConnsCount >= t.kw.MinReadyConnsNum {
		return
	}

	for i := 0; i < t.kw.MinReadyConnsNum-readyConnsCount; i++ {
		for _, oldStatus := range prioritySwitchFromStatusListToIdle {
			conn, errSwitch := t.allocateConnectionByUsingStatus(oldStatus, t.getConnections())
			if errSwitch != nil {
				logger.GetLoggerInstance().WarningF("Failed to add a ready gRPC connection, %v", errSwitch)
				continue
			}
			if conn == nil {
				continue
			}

			atomic.AddInt32(&t.busyConnCount, 1)
			// switch to idle status
			if err := t.RecycleConnection(conn); err != nil {
				logger.GetLoggerInstance().WarningF("Failed to add a ready gRPC connection, unable to recycle the connection, "+
					"Id: %v, Target %v, UsingStatus: %v, ConnStatus: %v, Err: %v",
					conn.GetId(), conn.GetTarget(), conn.GetUsingStatusDesc(), conn.GetConnStatus(), err)
				continue
			}

			retCount++
		}
	}

	return
}

func (t *ConnectionPool) checkAndUpdateConnectionsPeriodically() {
	logger.GetLoggerInstance().Debug("Started periodic check and update the gRPC connections")
	timer := time.NewTicker(t.kw.CheckAndUpdateInterval)

loopEnd:
	for {
		select {
		case <-timer.C:
			if t.status == define.StoppedPoolStatus {
				break loopEnd
			}
			if t.status == define.UpdatingPoolStatus {
				continue
			}

			if retCount := t.checkAndUpdateUnpreparedConnections(); retCount > 0 {
				logger.GetLoggerInstance().DebugF("Updated %d unprepared connections to %v status", retCount,
					define.GetUsingStatusDesc(define.NotOpenUsingStatus))
			}

			if retCount := t.checkAndAddReadyConnections(); retCount > 0 {
				if t.onAddReadyConnections != nil {
					t.onAddReadyConnections(retCount)
				}
			}

			if retCount := t.checkAndCloseExpiredIdledConnections(); retCount > 0 {
				logger.GetLoggerInstance().DebugF("Closed %d expired gRPC connections", retCount)
			}
		case <-t.checkCancelCtx.Done():
			break loopEnd
		case ctxt, ok := <-t.updateChan:
			if !ok {
				break loopEnd
			}

			t.status = define.UpdatingPoolStatus
			logger.GetLoggerInstance().Debug("Start updating the gRPC connection pool")
			if ctxt.Kw != nil {
				t.kw = ctxt.Kw
			} else {
				logger.GetLoggerInstance().Warning("The kw field of UpdateEvent Context is nil, only resetting is performed during updates the gRPC Connections pool")
			}
			ctxt.Kw = nil
			cleanConnNum, err := t.resetConnections()
			if err != nil {
				logger.GetLoggerInstance().WarningF("Failed to update the gRPC connection pool, unable to clear old connections, %v", err)
			}

			t.status = define.RunningPoolStatus
			logger.GetLoggerInstance().DebugF("Completed the update of the gRPC connection pool, cleared %d old connections", cleanConnNum)
		}
	}

	timer.Stop()
	logger.GetLoggerInstance().Debug("Exited periodic check and update the gRPC connections")
}

func (t *ConnectionPool) checkAndUpdateUnpreparedConnections() (retCount int) {
	for _, conn := range t.getConnections() {
		if conn.GetConnStatus() == connectivity.Ready {
			continue
		}

		if conn.GetUsingStatus() == define.IdledUsingStatus && conn.switchFromIdledToNotOpenUsingStatus() {
			retCount++
			atomic.AddInt32(&t.idledConnCount, -1)
		}
	}

	return
}

func (t *ConnectionPool) checkAndCloseExpiredIdledConnections() (retClosedCount int) {
	nowTp := time.Now().UnixMilli()
	var expiredConnList list.List

	closedCount := uint64(0)
	for _, conn := range t.getConnections() {
		idledTp := conn.GetIdledMilliTimestamp()
		if conn.GetUsingStatus() != define.IdledUsingStatus || idledTp <= 0 || nowTp <= idledTp {
			continue
		}

		if uint64(nowTp-idledTp) >= t.kw.MaxIdledDurationMilliseconds {
			expiredConnList.PushBack(conn)
		}
	}

	var expiredConnsNum int
	if t.kw.ForceCloseExpiredIdledConns {
		expiredConnsNum = expiredConnList.Len()
	} else if expiredConnList.Len() > t.kw.MinReadyConnsNum {
		expiredConnsNum = expiredConnList.Len() - t.kw.MinReadyConnsNum
	}

	// Prioritize closing connections at the back of the queue
	for i := 0; i < expiredConnsNum; i++ {
		elem := expiredConnList.Back()
		expiredConnList.Remove(elem)
		conn := elem.Value.(*Connection)
		if conn.switchFromIdledToNotOpenUsingStatus() {
			atomic.AddInt32(&t.idledConnCount, -1)
			closedCount++
		}
	}

	retClosedCount = int(closedCount)
	return
}

func (t *ConnectionPool) allocateConnectionByUsingStatus(oldStatus uintptr, connList []*Connection) (retConn *Connection, retErr error) {
	for _, conn := range connList {
		switched := false
		chgStatus := false
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

		chgStatus, switched, errSwitch = switchUsingStatusFunc()
		if oldStatus == define.IdledUsingStatus && chgStatus {
			atomic.AddInt32(&t.idledConnCount, -1)
		}

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

func (t *ConnectionPool) resetConnections() (int, error) {
	for i := 0; i < MaxCheckBusyConnNum; i++ {
		time.Sleep(time.Second)
		if atomic.LoadInt32(&t.busyConnCount) <= 0 {
			break
		}
	}

	if atomic.LoadInt32(&t.busyConnCount) > 0 {
		return 0, errors.New("there are busy connections that cannot be cleared")
	}

	newConnList := []*Connection{}
	for i := 0; i < t.kw.MinReadyConnsNum; i++ {
		conn := newConnection(t.kw.Target, t.kw.DialOpts, t.kw.NewConnTimeout)
		newConnList = append(newConnList, conn)
	}

	t.rwMutex.Lock()
	connList := t.connList
	t.connList = newConnList
	t.rwMutex.Unlock()
	atomic.StoreInt32(&t.idledConnCount, 0)
	for _, conn := range connList {
		_ = conn.stop(true)
	}

	return len(connList), nil
}

func (t *ConnectionPool) PubUpdateEvent(ctxt UpdateEventContext) error {
	if len(t.updateChan) >= MaxUpdateChanCapacity {
		return define.ErrReachedUpdateChanCapacity
	}

	t.status = define.UpdatingPoolStatus
	t.updateChan <- ctxt

	return nil
}
