package example

import (
	"errors"
	"fmt"
	"github.com/akley-MK4/simple-grpc/connectionpool"
	"github.com/akley-MK4/simple-grpc/define"
	"github.com/akley-MK4/simple-grpc/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxWorkerNum            = 10
	maxConnNum              = 100
	minIdleConnNum          = 20
	maxIdleDurationMilli    = 1000 * 10
	idledConnNumIntervalSec = 5
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
		Target:          fmt.Sprintf("0.0.0.0:%d", listenPort),
		NewConnTimeout:  time.Second * time.Duration(3),
		MaxConnNum:      maxConnNum,
		MinIdledConnNum: minIdleConnNum,
		//MaxIdleDurationMilli: 100 * 60,
		MaxIdledDurationMilli: maxIdleDurationMilli,
		IdledConnNumInterval:  time.Second * time.Duration(idledConnNumIntervalSec),
	}

	kw.DialOpts = append(kw.DialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	connPool, newConnPoolErr := connectionpool.NewConnectionPool(kw)
	if newConnPoolErr != nil {
		return newConnPoolErr
	}

	if err := connPool.Start(); err != nil {
		return err
	}

	connPoolInst = connPool
	time.Sleep(time.Second * 1)

	if err := testMaxAllocateAndRecycleConnections(); err != nil {
		return fmt.Errorf("failed to test the testMaxAllocateAndRecycleConnections function, %v", err)
	}
	logger.GetLoggerInstance().Info("Successfully tested the testMaxAllocateAndRecycleConnections function")

	if err := testShrinkAndKeepMinIdledConnections(); err != nil {
		return fmt.Errorf("failed to test the testShrinkAndKeepMinIdledConnections function, %v", err)
	}
	logger.GetLoggerInstance().Info("Successfully tested the testShrinkAndKeepMinIdledConnections function")

	if err := testStopPool(); err != nil {
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
	if count != maxConnNum {
		return fmt.Errorf("current number of busy using status connections is incorrect, %d != %d", count, maxConnNum)
	}

	count = getConnPoolInst().GetReadyConnectionsCount()
	if count != maxConnNum {
		return fmt.Errorf("current number of ready connections is incorrect, %d != %d", count, maxConnNum)
	}

	_, errMaxConnLimit := getConnPoolInst().AllocateConnection()
	if errMaxConnLimit == nil {
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
	if count != maxConnNum {
		return fmt.Errorf("current number of idled using status connections is incorrect, %d != %d", count, maxConnNum)
	}

	return nil
}

func testShrinkAndKeepMinIdledConnections() error {
	logger.GetLoggerInstance().Info("Wait for shrink")
	time.Sleep(time.Millisecond*maxIdleDurationMilli + time.Second*10)

	count := getConnPoolInst().GetConnectionsCount()
	if count != maxConnNum {
		return fmt.Errorf("[1] current number of connections is incorrect, %d != %d", count, maxConnNum)
	}

	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.IdledUsingStatus)
	if count != minIdleConnNum {
		return fmt.Errorf("[2] current number of idled using status connections is incorrect, %d != %d", count, minIdleConnNum)
	}
	count = getConnPoolInst().GetReadyConnectionsCount()
	if count != minIdleConnNum {
		return fmt.Errorf("[3] current number of ready connections is incorrect, %d != %d", count, minIdleConnNum)
	}

	logger.GetLoggerInstance().Info("Check for added idle connections")
	newConn, errNewConn := getConnPoolInst().AllocateConnection()
	if errNewConn != nil {
		return errNewConn
	}
	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.IdledUsingStatus)
	if count != (minIdleConnNum - 1) {
		return fmt.Errorf("[4] current number of idle using status connections is incorrect, %d != %d", count, minIdleConnNum-1)
	}
	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.BusyUsingStatus)
	if count != 1 {
		return fmt.Errorf("[5] current number of busy using status connections is incorrect, %d != %d", count, 1)
	}

	time.Sleep(time.Second*idledConnNumIntervalSec + 5)
	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.IdledUsingStatus)
	if count != minIdleConnNum {
		return fmt.Errorf("[6] current number of idle using status connections is incorrect, %d != %d", count, minIdleConnNum)
	}
	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.BusyUsingStatus)
	if count != 1 {
		return fmt.Errorf("[7] current number of busy using status connections is incorrect, %d != %d", count, 1)
	}

	if err := getConnPoolInst().RecycleConnection(newConn); err != nil {
		return err
	}

	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.BusyUsingStatus)
	if count != 0 {
		return fmt.Errorf("[8] current number of busy using status connections is incorrect, %d != %d", count, 0)
	}
	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.IdledUsingStatus)
	if count != (minIdleConnNum + 1) {
		return fmt.Errorf("[9] current number of idle using status connections is incorrect, %d != %d", count, (minIdleConnNum + 1))
	}

	logger.GetLoggerInstance().Info("Wait for shrink")
	time.Sleep(time.Millisecond*maxIdleDurationMilli + time.Second*10)
	count = getConnPoolInst().GetUsingStatusConnectionsCount(define.IdledUsingStatus)
	if count != minIdleConnNum {
		return fmt.Errorf("[10] current number of idled using status connections is incorrect, %d != %d", count, minIdleConnNum)
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
