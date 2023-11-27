package example

import (
	"fmt"
	"github.com/akley-MK4/simple-grpc/connectionpool"
	"github.com/akley-MK4/simple-grpc/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math/rand"
	"time"
)

const (
	maxWorkerNum         = 10
	maxConnNum           = 10
	minIdleConnNum       = 5
	maxIdleDurationMilli = 1000 * 10
)

var (
	clientConnPool *connectionpool.ConnectionPool
)

func getClientConnPool() *connectionpool.ConnectionPool {
	return clientConnPool
}

func RunBasicClientExample() error {
	if err := logger.SetLoggerInstance(newExample("[Client]")); err != nil {
		return err
	}

	// Create connection pool
	kw := connectionpool.KwArgsNewConnPool{
		Target:         fmt.Sprintf("0.0.0.0:%d", listenPort),
		NewConnTimeout: time.Second * time.Duration(3),
		MaxConnNum:     maxConnNum,
		MinIdleConnNum: minIdleConnNum,
		//MaxIdleDurationMilli: 100 * 60,
		MaxIdleDurationMilli: maxIdleDurationMilli,
		KeepActiveInterval:   time.Second * time.Duration(5),
	}

	kw.DialOpts = append(kw.DialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	connPool, newConnPoolErr := connectionpool.NewConnectionPool(kw)
	if newConnPoolErr != nil {
		return newConnPoolErr
	}

	if err := connPool.Start(); err != nil {
		return err
	}

	clientConnPool = connPool
	go func() {
		for {
			time.Sleep(time.Second * 20)
			touchConnPoolShrink()
		}
	}()

	go func() {
		for {
			time.Sleep(time.Second * 3)
			logger.GetLoggerInstance().DebugF("Available Connections Num: %d", connPool.GetAvailableConnectionsNum())
		}
	}()
	time.Sleep(time.Hour)

	go func() {
		for {
			time.Sleep(time.Second * 5)
			outputStats()
		}
	}()

	go func() {
		time.Sleep(time.Second * 40)
		getClientConnPool().Stop()
		logger.GetLoggerInstance().Warning("Stopped conn pool")
	}()

	for {
		touchConnPoolShrink()
		time.Sleep(time.Second * 25)
	}

	time.Sleep(time.Hour)
	return nil

	initAllClientWorkers(maxWorkerNum, 3)

	time.Sleep(time.Second * 30)
	stopAllClientWorkers()

	time.Sleep(time.Minute * 5)
	connPool.Stop()

	outputStats()
	return nil
}

func touchConnPoolShrink() {
	var connList []*connectionpool.Connection
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	num := r.Intn(maxConnNum) + 1
	for i := 0; i < num; i++ {
		conn, _, preemptErr := getClientConnPool().PreemptConnection()
		if preemptErr != nil {
			logger.GetLoggerInstance().WarningF("Failed to preempt connection, Err: %v", preemptErr)
			continue
		}
		if conn == nil {
			continue
		}
		connList = append(connList, conn)
	}

	for _, conn := range connList {
		//time.Sleep(time.Millisecond * time.Duration(rVal))
		if err := getClientConnPool().RecycleConnection(conn); err != nil {
			logger.GetLoggerInstance().ErrorF("Recycling connection failed, Err: %v", err)
			return
		}
	}

	if len(connList) > 0 {
		logger.GetLoggerInstance().DebugF("Successfully preempted %d connections", len(connList))
	}
}
