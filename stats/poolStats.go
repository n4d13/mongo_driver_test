package stats

import (
	"fmt"
	"sync"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/event"
)

type PoolStats struct {
	Created    int64
	Closed     int64
	InUse      int64
	Returned   int64
	GetsOK     int64
	GetsFailed int64
	Reasons    map[string]int64
	mutex      sync.RWMutex
}

func NewPoolStats() *PoolStats {
	return &PoolStats{
		Reasons: make(map[string]int64),
	}
}

func (p *PoolStats) MonitorFunc(poolEvent *event.PoolEvent) {
	switch poolEvent.Type {
	case event.ConnectionCreated:
		atomic.AddInt64(&p.Created, 1)
	case event.ConnectionClosed:
		atomic.AddInt64(&p.Closed, 1)
	case event.ConnectionReturned:
		atomic.AddInt64(&p.Returned, 1)
		atomic.AddInt64(&p.InUse, -1)
	case event.GetSucceeded:
		atomic.AddInt64(&p.GetsOK, 1)
		atomic.AddInt64(&p.InUse, 1)
	case event.GetFailed:
		atomic.AddInt64(&p.GetsFailed, 1)
		_, ok := p.Reasons[poolEvent.Reason]
		p.mutex.Lock()
		if ok {
			p.Reasons[poolEvent.Reason] = p.Reasons[poolEvent.Reason] + 1
		} else {
			p.Reasons[poolEvent.Reason] = 1
		}
		p.mutex.Unlock()
	}
}

func (p *PoolStats) String() string {
	return fmt.Sprintf("{"+
		"created=%d, "+
		"closed=%d, "+
		"in_use=%d, "+
		"returned=%d, "+
		"gets_OK=%d, "+
		"gets_failed=%d, "+
		"failures=%v"+
		"}", p.Created, p.Closed, p.InUse, p.Returned, p.GetsOK, p.GetsFailed, p.Reasons)
}
