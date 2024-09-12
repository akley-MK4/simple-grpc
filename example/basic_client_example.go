package example

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akley-MK4/simple-grpc/connectionpool"
	"github.com/akley-MK4/simple-grpc/define"
	"github.com/akley-MK4/simple-grpc/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	maxWorkerNum                = 10
	maxConnNum                  = 50
	minReadyConnsNum            = 20
	idledConnExpireMilliseconds = 1000 * 20
	readyConnNumIntervalSec     = 5
)

var (
	connPoolInst *connectionpool.ConnectionPool
)

func getConnPoolInst() *connectionpool.ConnectionPool {
	return connPoolInst
}

func RunBasicClientExample() error {
	if err := logger.SetLoggerInstance(newExample("[Client]")); err != nil {
		return err
	}

	// Create connection pool
	kw := connectionpool.KwArgsConnPool{
		Target:           fmt.Sprintf("0.0.0.0:%d", listenPort),
		NewConnTimeout:   time.Second * time.Duration(3),
		MaxConnNum:       maxConnNum,
		MinReadyConnsNum: minReadyConnsNum,
		//MaxIdleDurationMilli: 100 * 60,
		MaxIdledDurationMilliseconds: uint64(idledConnExpireMilliseconds),
		CheckAndUpdateInterval:       time.Second * time.Duration(readyConnNumIntervalSec),
		ForceCloseExpiredIdledConns:  false,
	}

	kw.DialOpts = append(kw.DialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	kw.OnAddReadyConnections = func(addedConnsNum, currentReadyConnsNum int) {
		logger.GetLoggerInstance().InfoF("\n\n\nSuccessfully switched %d gRPC connections to the ready state, the current number of ready gRPC connections is %d\n\n\n",
			addedConnsNum, currentReadyConnsNum)
	}

	connPool, newConnPoolErr := connectionpool.NewConnectionPool(kw)
	if newConnPoolErr != nil {
		return newConnPoolErr
	}

	if err := connPool.Start(); err != nil {
		return err
	}

	connPoolInst = connPool

	printStats := func() {
		if connPoolInst != nil {
			logger.GetLoggerInstance().InfoF("PoolStatus: %v, ConnectionsNum: %v, ReadyConnectionsNum: %v, BusyConnectionsNum: %v, "+
				"IdledConnectionsNum: %v, NotOpenUsingStatusNum: %v, DisconnectedUsingStatusNum: %v, StoppedUsingStatus: %v",
				connPoolInst.GetStatusDesc(),
				connPoolInst.GetConnectionsCount(), connPoolInst.GetReadyConnectionsCount(), connPoolInst.GetBusyConnectionsCount(),
				connPoolInst.GetIdledConnectionsCount(), connPoolInst.GetUsingStatusConnectionsCount(define.NotOpenUsingStatus),
				connPoolInst.GetUsingStatusConnectionsCount(define.DisconnectedUsingStatus),
				connPoolInst.GetUsingStatusConnectionsCount(define.StoppedUsingStatus))
		}
	}

	go func() {
		for {
			time.Sleep(time.Second * 5)
			printStats()
		}
	}()

	//time.Sleep(time.Second * 1000)

	logger.GetLoggerInstance().Info("\n\n\n===============Starting the stage 1 for testing=================")
	if err := testMaxAllocateAndRecycleConnections(); err != nil {
		printStats()
		return fmt.Errorf("failed to test the testMaxAllocateAndRecycleConnections function, %v", err)
	}
	logger.GetLoggerInstance().Info("Successfully tested the testMaxAllocateAndRecycleConnections function")

	if err := testCloseExpiredUnusedConnections(); err != nil {
		printStats()
		return fmt.Errorf("failed to test the testCloseExpiredUnusedConnections function, %v", err)
	}
	logger.GetLoggerInstance().Info("Successfully tested the testCloseExpiredUnusedConnections function")

	if err := testCheckAndAddReadyConnections(); err != nil {
		printStats()
		return fmt.Errorf("failed to test the testCheckAndAddReadyConnections function, %v", err)
	}
	logger.GetLoggerInstance().Info("Successfully tested the testCheckAndAddReadyConnections function")

	logger.GetLoggerInstance().Info("\n\n\n===============Starting the stage 2 for testing=================")
	if err := testForceCloseExpiredAddIdleConnections(); err != nil {
		printStats()
		return fmt.Errorf("failed to test the testForceCloseExpiredAddIdleConnections function, %v", err)
	}
	logger.GetLoggerInstance().Info("Successfully tested the testForceCloseExpiredAddIdleConnections function")

	if err := connPool.Stop(true); err != nil {
		return err
	}

	logger.GetLoggerInstance().Info("\n\n\n===============Starting the stage 3 for testing=================")
	connPool, newConnPoolErr = connectionpool.NewConnectionPool(kw)
	if newConnPoolErr != nil {
		return newConnPoolErr
	}

	if err := connPool.Start(); err != nil {
		return err
	}
	connPoolInst = connPool

	if err := testCheckAndAddReadyConnections(); err != nil {
		printStats()
		return fmt.Errorf("failed to test the testCheckAndAddReadyConnections function, %v", err)
	}
	logger.GetLoggerInstance().Info("Successfully tested the testCheckAndAddReadyConnections function")
	if err := testCloseExpiredUnusedConnections(); err != nil {
		printStats()
		return fmt.Errorf("failed to test the testCloseExpiredUnusedConnections function, %v", err)
	}
	logger.GetLoggerInstance().Info("Successfully tested the testCloseExpiredUnusedConnections function")

	logger.GetLoggerInstance().Info("\n\n\n===============Starting the stage 4 for testing=================")
	kw.Target = fmt.Sprintf("0.0.0.0:%d", listenPort2)
	minReadyConnsNum = 25
	logger.GetLoggerInstance().InfoF("Set the MinReadyConnsNum to %v from %v", minReadyConnsNum, kw.MinReadyConnsNum)
	kw.MinReadyConnsNum = minReadyConnsNum
	maxUpdateChanCapacity := connectionpool.MaxUpdateChanCapacity + 5
	for i := 0; i < maxUpdateChanCapacity; i++ {
		err := connPoolInst.PubUpdateEvent(connectionpool.UpdateEventContext{
			Kw: &kw,
		})
		if (i+1) >= connectionpool.MaxUpdateChanCapacity && err == define.ErrReachedUpdateChanCapacity {
			continue
		}
		if err != nil {
			return err
		}
	}

	time.Sleep(time.Second * 2)
	for i := 0; i < 20; i++ {
		if connPoolInst.GetStatus() == define.RunningPoolStatus {
			break
		}
		logger.GetLoggerInstance().Info("Waiting for the connection pool status to switch to RunningPoolStatus")
		time.Sleep(time.Second * 2)
	}
	if connPoolInst.GetStatus() != define.RunningPoolStatus {
		return errors.New("waiting for the connection pool status to switch to RunningPoolStatus timed out")
	}

	logger.GetLoggerInstance().InfoF("Successfully switched connection to target %v\n\n", kw.Target)
	time.Sleep(time.Second * 20)

	if err := testMaxAllocateAndRecycleConnections(); err != nil {
		printStats()
		return fmt.Errorf("failed to test the testMaxAllocateAndRecycleConnections function, %v", err)
	}
	logger.GetLoggerInstance().Info("Successfully tested the testMaxAllocateAndRecycleConnections function")

	if err := testCloseExpiredUnusedConnections(); err != nil {
		printStats()
		return fmt.Errorf("failed to test the testShrinkAndKeepMinIdledConnections function, %v", err)
	}
	logger.GetLoggerInstance().Info("Successfully tested the testShrinkAndKeepMinIdledConnections function")

	if err := testCheckAndAddReadyConnections(); err != nil {
		printStats()
		return fmt.Errorf("failed to test the testCheckAndAddReadyConnections function, %v", err)
	}
	logger.GetLoggerInstance().Info("Successfully tested the testCheckAndAddReadyConnections function")

	if err := testStopPool(); err != nil {
		printStats()
		return fmt.Errorf("failed to test the testStopPool function, %v", err)
	}
	logger.GetLoggerInstance().Info("Successfully tested the testStopPool function")

	return nil
}

func testMaxAllocateAndRecycleConnections() error {
	var rwMutex sync.RWMutex
	var wg sync.WaitGroup
	connList := make([]*connectionpool.Connection, 0, maxConnNum)

	wg.Add(maxConnNum)
	for i := 0; i < maxConnNum; i++ {
		go func(inIdx int, inWg *sync.WaitGroup) {
			defer inWg.Done()

			conn, errAllocate := getConnPoolInst().AllocateConnection()
			if errAllocate != nil {
				logger.GetLoggerInstance().WarningF("Failed to allocate conn, Idx: %d, Err: %v",
					inIdx, errAllocate)
				return
			}

			rwMutex.Lock()
			connList = append(connList, conn)
			rwMutex.Unlock()
		}(i, &wg)
	}
	wg.Wait()

	if len(connList) != maxConnNum {
		return fmt.Errorf("connections assignment failed, %d != %d", len(connList), maxConnNum)
	}
	logger.GetLoggerInstance().Info("Successfully allocated the maximum number of connections")

	count := getConnPoolInst().GetConnectionsCount()
	if count != maxConnNum {
		return fmt.Errorf("current number of connections is incorrect, %d != %d", count, maxConnNum)
	}

	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.BusyUsingStatus)
	if count != maxConnNum || getConnPoolInst().GetBusyConnectionsCount() != int32(maxConnNum) || getConnPoolInst().GetIdledConnectionsCount() != 0 {
		return fmt.Errorf("current number of busy using status connections is incorrect, %d != %d", count, maxConnNum)
	}

	count = getConnPoolInst().GetReadyConnectionsCount()
	if count != maxConnNum {
		return fmt.Errorf("current number of ready connections is incorrect, %d != %d", count, maxConnNum)
	}

	_, errMaxConnLimit := getConnPoolInst().AllocateConnection()
	if errMaxConnLimit == nil || errMaxConnLimit != define.ErrorReachedMaxConnNumLimit {
		return errors.New("the maximum number of connections limit has failed")
	}
	logger.GetLoggerInstance().Info("Successfully limited the maximum number of connections")

	var wg2 sync.WaitGroup
	wg2.Add(len(connList))
	errRecycleCount := uint64(0)
	for idx, conn := range connList {
		go func(inIdx int, inConn *connectionpool.Connection, inWg *sync.WaitGroup) {
			defer inWg.Done()
			if err := getConnPoolInst().RecycleConnection(inConn); err != nil {
				atomic.AddUint64(&errRecycleCount, 1)
				logger.GetLoggerInstance().WarningF("Failed to recycle connection, Id: %d, Err: %v",
					inConn.GetId(), err)
				return
			}

		}(idx, conn, &wg2)
	}
	wg2.Wait()

	if errRecycleCount > 0 {
		return fmt.Errorf("%d errors occurred while recycling connections", errRecycleCount)
	}
	logger.GetLoggerInstance().Info("Successfully recycled the maximum number of connections")

	count = getConnPoolInst().GetConnectionsCount()
	if count != maxConnNum {
		return fmt.Errorf("current number of connections is incorrect, %d != %d", count, maxConnNum)
	}

	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.IdledUsingStatus)
	if count != maxConnNum || getConnPoolInst().GetIdledConnectionsCount() != int32(maxConnNum) || getConnPoolInst().GetBusyConnectionsCount() != 0 {
		return fmt.Errorf("current number of idled using status connections is incorrect, %d != %d", count, maxConnNum)
	}

	return nil
}

func testCheckAndAddReadyConnections() error {
	time.Sleep(time.Duration(readyConnNumIntervalSec+5) * time.Second)

	if connsNum := getConnPoolInst().GetReadyConnectionsCount(); connsNum != minReadyConnsNum {
		return fmt.Errorf("[2] current number of ready connections is incorrect, %d != %d", connsNum, minReadyConnsNum)
	}
	if connsNum := getConnPoolInst().GetBusyConnectionsCount(); int(connsNum) != 0 {
		return fmt.Errorf("[3] current number of busy using status connections is incorrect, %d != %d", connsNum, 0)
	}
	if connsNum := getConnPoolInst().GetIdledConnectionsCount(); int(connsNum) != minReadyConnsNum {
		return fmt.Errorf("[4] current number of ready connections is incorrect, %d != %d", connsNum, minReadyConnsNum)
	}

	return nil
}

func testForceCloseExpiredAddIdleConnections() error {
	connPoolInst.PubUpdateEvent(connectionpool.UpdateEventContext{})
	return testCheckAndAddReadyConnections()
}

func testCloseExpiredUnusedConnections() error {

	var newConns []*connectionpool.Connection
	for i := 0; i < int(maxConnNum); i++ {
		conn, errNewConn := getConnPoolInst().AllocateConnection()
		if errNewConn != nil {
			return fmt.Errorf("[0] failed to allocate connection, %v", errNewConn)
		}
		newConns = append(newConns, conn)
	}

	count := getConnPoolInst().GetConnectionsCount()
	if count != maxConnNum {
		return fmt.Errorf("[1] current number of connections is incorrect, %d != %d", count, maxConnNum)
	}

	shoudReadyConnsNum := maxConnNum
	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.IdledUsingStatus)
	if count != 0 || getConnPoolInst().GetIdledConnectionsCount() != 0 {
		return fmt.Errorf("[2] current number of unused using status connections is incorrect, %d != %d", count, 0)
	}
	count = getConnPoolInst().GetReadyConnectionsCount()
	if count != shoudReadyConnsNum {
		return fmt.Errorf("[3] current number of ready connections is incorrect, %d != %d", count, shoudReadyConnsNum)
	}
	if connsNum := getConnPoolInst().GetBusyConnectionsCount(); int(connsNum) != shoudReadyConnsNum {
		return fmt.Errorf("[4] current number of busy using status connections is incorrect, %d != %d", connsNum, shoudReadyConnsNum)
	}

	if connsNum := getConnPoolInst().GetReadyConnectionsCount(); connsNum != shoudReadyConnsNum {
		return fmt.Errorf("[5] current number of ready connections is incorrect, %d != %d", connsNum, shoudReadyConnsNum)
	}

	for _, conn := range newConns {
		if err := getConnPoolInst().RecycleConnection(conn); err != nil {
			return fmt.Errorf("[6] Failed to recycle connection, Id: %d, Err: %v", conn.GetId(), err)
		}
	}

	if connsNum := getConnPoolInst().GetIdledConnectionsCount(); int(connsNum) != shoudReadyConnsNum {
		return fmt.Errorf("[7] current number of ready connections is incorrect, %d != %d", connsNum, shoudReadyConnsNum)
	}

	if connsNum := getConnPoolInst().GetBusyConnectionsCount(); int(connsNum) != 0 {
		return fmt.Errorf("[8] current number of busy using status connections is incorrect, %d != %d", connsNum, 0)
	}

	logger.GetLoggerInstance().Info("Wait for shrink")
	time.Sleep(time.Millisecond*time.Duration(idledConnExpireMilliseconds) + time.Second*60)

	if connsNum := getConnPoolInst().GetReadyConnectionsCount(); connsNum != minReadyConnsNum {
		return fmt.Errorf("[9] current number of ready connections is incorrect, %d != %d", connsNum, minReadyConnsNum)
	}
	if connsNum := getConnPoolInst().GetIdledConnectionsCount(); int(connsNum) != minReadyConnsNum {
		return fmt.Errorf("[10] current number of ready connections is incorrect, %d != %d", connsNum, minReadyConnsNum)
	}
	if connsNum := getConnPoolInst().GetBusyConnectionsCount(); int(connsNum) != 0 {
		return fmt.Errorf("[11] current number of busy using status connections is incorrect, %d != %d", connsNum, 0)
	}

	return nil
}

func testStopPool() error {
	if err := getConnPoolInst().Stop(true); err != nil {
		return err
	}

	count := getConnPoolInst().GetConnectionsCount()
	if count != maxConnNum {
		return fmt.Errorf("current number of connections is incorrect, %d != %d", count, maxConnNum)
	}

	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.StoppedUsingStatus)
	if count != maxConnNum {
		return fmt.Errorf("current number of stopped using status connections is incorrect, %d != %d", count, maxConnNum)
	}

	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.IdledUsingStatus)
	if count != 0 {
		return fmt.Errorf("current number of idle using status connections is incorrect, %d != %d", count, 0)
	}

	return nil
}
