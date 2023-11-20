package example

import (
	"context"
	"github.com/akley-MK4/simple-grpc/logger"
	"math/rand"
	"time"
)

var (
	workerList []*clientWorker
)

func getAllClientWorkers() []*clientWorker {
	return workerList
}

func initAllClientWorkers(maxNum int, maxPolicyNum int) {
	for id := 0; id < maxNum; id++ {
		worker := newClientWorker(id, maxPolicyNum)
		workerList = append(workerList, worker)
		go worker.startPreemptPolicy()
	}
}

func stopAllClientWorkers() {
	for _, worker := range workerList {
		worker.stop()
	}
}

func newClientWorker(id int, maxPolicyNum int) *clientWorker {
	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	return &clientWorker{
		id:         id,
		randGen:    rand.New(rand.NewSource(time.Now().UnixNano())),
		stats:      newClientWorkerStats(maxPolicyNum),
		cancelFunc: cancelFunc,
		cancelCtx:  cancelCtx,
	}
}

type clientWorker struct {
	id      int
	randGen *rand.Rand
	stats   *clientWorkerStats

	stopped    bool
	cancelFunc context.CancelFunc
	cancelCtx  context.Context
}

func (t *clientWorker) GetStats() clientWorkerStats {
	return *t.stats
}

func (t *clientWorker) start() {
	go t.startPreemptPolicy()
}

func (t *clientWorker) stop() {
	t.stopped = true
	time.Sleep(time.Second * 1)
	t.cancelFunc()
}

func (t *clientWorker) startPreemptPolicy() {

loopEnd:
	for {
		rVal := t.randGen.Intn(5) + 1
		timer := time.NewTicker(time.Second * time.Duration(rVal))

		select {
		case <-timer.C:
			timer.Stop()
			if t.stopped {
				break loopEnd
			}
			t.preemptHandle()
			break
		case <-t.cancelCtx.Done():
			break loopEnd
		}
	}
}

func (t *clientWorker) preemptHandle() {
	t.stats.AddTotalPreemptConnNum()
	conn, policyId, preemptErr := getClientConnPool().PreemptConnection()
	if preemptErr != nil {
		t.stats.AddTotalPreemptConnFailNum()
		logger.GetLoggerInstance().WarningF("Failed to preempt connection, WorkerId: %v, Err: %v", t.id, preemptErr)
		return
	}

	//logger.GetLoggerInstance().DebugF("Successfully preempted the connection. WorkerId: %v, Id: %v, Target %v, Status: %v, GRPCConnStatus: %v",
	//	t.id, conn.GetId(), conn.GetTarget(), conn.GetStatus(), conn.GetGRPCConnStatus())

	t.stats.AddTotalPreemptConnSuccessNum()
	t.stats.UpdatePreemptPolicyStats(policyId, 1)
	rVal := t.randGen.Intn(500) + 1
	time.Sleep(time.Millisecond * time.Duration(rVal))

	if err := getClientConnPool().RecycleConnection(conn); err != nil {
		t.stats.AddTotalRecycleConnFailNum()
		logger.GetLoggerInstance().ErrorF("Recycling connection failed, WorkerId: %v, Err: %v", t.id, err)
		return
	}

	t.stats.AddTotalRecycleConnSuccessNum()
}
