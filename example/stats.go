package example

import (
	"encoding/json"
	"fmt"
	"github.com/akley-MK4/simple-grpc/logger"
	"sync"
	"sync/atomic"
)

func outputStats() {
	var statsList []clientWorkerStats
	for _, worker := range getAllClientWorkers() {
		statsList = append(statsList, worker.GetStats())
	}
	if len(statsList) <= 0 {
		return
	}

	d, _ := json.Marshal(statsList)
	logger.GetLoggerInstance().Info("STATS:")
	fmt.Println(string(d))
}

type preemptPolicyStats struct {
	Id              int
	TotalSuccessNum uint64
}

func newClientWorkerStats(maxPolicyNum int) *clientWorkerStats {
	stats := &clientWorkerStats{}
	for i := 0; i < maxPolicyNum; i++ {
		stats.PolicyStats = append(stats.PolicyStats, &preemptPolicyStats{Id: i})
	}

	return stats
}

type clientWorkerStats struct {
	TotalPreemptConnNum        uint64
	TotalPreemptConnSuccessNum uint64
	TotalPreemptConnFailNum    uint64
	TotalRecycleConnSuccessNum uint64
	TotalRecycleConnFailNum    uint64

	rwMutex     sync.RWMutex
	PolicyStats []*preemptPolicyStats
}

func (t *clientWorkerStats) AddTotalPreemptConnNum() {
	atomic.AddUint64(&t.TotalPreemptConnNum, 1)
}

func (t *clientWorkerStats) AddTotalPreemptConnSuccessNum() {
	atomic.AddUint64(&t.TotalPreemptConnSuccessNum, 1)
}

func (t *clientWorkerStats) AddTotalPreemptConnFailNum() {
	atomic.AddUint64(&t.TotalPreemptConnFailNum, 1)
}

func (t *clientWorkerStats) AddTotalRecycleConnSuccessNum() {
	atomic.AddUint64(&t.TotalRecycleConnSuccessNum, 1)
}
func (t *clientWorkerStats) AddTotalRecycleConnFailNum() {
	atomic.AddUint64(&t.TotalRecycleConnFailNum, 1)
}

func (t *clientWorkerStats) UpdatePreemptPolicyStats(policyId int, successNum uint64) {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	policyStats := t.PolicyStats[policyId]
	policyStats.TotalSuccessNum += successNum
}
