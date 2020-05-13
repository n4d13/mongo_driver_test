package stage

import (
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/n4d13/mongo_driver_test/repositories"
	"github.com/n4d13/mongo_driver_test/stats"
	"github.com/sirupsen/logrus"
)

type Config struct {
	WorkersCount     uint
	WorkersToAdd     uint
	IncrementLoad    uint
	ProducersCount   uint
	MsgBySec         uint
	TimeToSleepSecs  uint
	TimeToFinishSecs uint
	ContextTimeMs    uint
	QueryTimeoutMs   uint
}

type Stage struct {
	dbConfig      repositories.MongoDBConfiguration
	stageConfig   Config
	timeSpentByOp []int64
	errorCount    int64
}

func New(
	dbConfig repositories.MongoDBConfiguration,
	stageConfig Config) *Stage {
	return &Stage{
		dbConfig:    dbConfig,
		stageConfig: stageConfig,
	}
}

func (s *Stage) Run() {

	statsMonitor := stats.NewPoolStats()

	config := &repositories.MongoDBConfiguration{
		DbName:         s.dbConfig.DbName,
		CollectionName: s.dbConfig.CollectionName,
		ConnString:     s.dbConfig.ConnString,
		MinPool:        s.dbConfig.MinPool,
		MaxPool:        s.dbConfig.MaxPool,
		IdleTimeout:    s.dbConfig.IdleTimeout,
		SocketTimeout:  s.dbConfig.SocketTimeout,
	}
	repo, err := repositories.NewMongodbRepository(config, statsMonitor.MonitorFunc)
	if err != nil {
		logrus.Fatal(err)
	}

	spentFunc := func(timeSpent int64) {
		s.timeSpentByOp = append(s.timeSpentByOp, timeSpent)
	}

	errorFunc := func() {
		atomic.AddInt64(&s.errorCount, 1)
	}

	storeIds, err := ensureData(repo)
	if err != nil {
		return
	}

	repo.SetValidIds(storeIds)

	eventChannel := make(chan struct{}, 1000)

	wgP := &sync.WaitGroup{}

	producers := addProducers(int(s.stageConfig.ProducersCount), eventChannel, int(s.stageConfig.MsgBySec), wgP)

	workers := addWorkers(int(s.stageConfig.WorkersCount), repo, eventChannel,
		s.stageConfig.QueryTimeoutMs, s.stageConfig.ContextTimeMs, spentFunc, errorFunc)

	intLoad := int(s.stageConfig.IncrementLoad)
	intTimeToSleep := int(s.stageConfig.TimeToSleepSecs)
	for n := 0; n < intLoad; n++ {
		logrus.Printf("Waiting %d seconds to add %d workers. Current count: %d",
			s.stageConfig.TimeToSleepSecs, s.stageConfig.WorkersToAdd, len(workers))
		for i := 0; i < intTimeToSleep; i++ {
			logrus.WithField("executed", repo.QueryCount()).Infof("%v", statsMonitor)
			time.Sleep(1 * time.Second)
		}
		workers = append(workers, addWorkers(int(s.stageConfig.WorkersToAdd), repo, eventChannel,
			s.stageConfig.QueryTimeoutMs, s.stageConfig.ContextTimeMs, spentFunc, errorFunc)...)
		logrus.Printf("%d workers added. Using %d in total", s.stageConfig.WorkersToAdd, len(workers))
	}

	logrus.Printf("Waiting %d seconds to finish", s.stageConfig.TimeToFinishSecs)
	intTimeToFinish := int(s.stageConfig.TimeToFinishSecs)
	for i := 0; i < intTimeToFinish; i++ {
		logrus.WithField("executed", repo.QueryCount()).Infof("%+v", statsMonitor)
		time.Sleep(1 * time.Second)
	}

	for _, producer := range producers {
		producer.stop()
	}
	wgP.Wait()
	logrus.Println("Producers stopped.")

	for len(eventChannel) > 0 {
		logrus.WithField("executed", repo.QueryCount()).Infof("%+v", statsMonitor)
		time.Sleep(1 * time.Second)
	}

	repo.Close()

	time.Sleep(1 * time.Second)
	logrus.Printf("Total query count: %d", repo.QueryCount())
	logrus.Printf("%+v", statsMonitor)

	var total int64
	var max int64
	var min int64 = math.MaxInt64
	for _, spent := range s.timeSpentByOp {
		total += spent
		if spent > max {
			max = spent
		} else {
			if spent < min {
				min = spent
			}
		}
	}

	logrus.Printf("Errors = %d. Median = %d, min = %d, max = %d", s.errorCount, total/int64(len(s.timeSpentByOp)), min, max)

}

func addWorkers(
	workersCount int,
	repo repositories.TestRepository,
	evChan chan struct{},
	queryTimeout uint,
	contextTimeout uint,
	spentFunc func(int64),
	errorFunc func(),
) []*consumer {
	var consumers []*consumer
	for i := 0; i < workersCount; i++ {
		consumer := &consumer{
			repository:     repo,
			eventChannel:   evChan,
			queryTimeout:   queryTimeout,
			contextTimeout: contextTimeout,
			spentFunc:      spentFunc,
			errorFunc:      errorFunc,
		}
		consumers = append(consumers, consumer)
		go consumer.start()
	}
	return consumers
}

func addProducers(producersCount int, eventChannel chan struct{}, msgBySec int, wg *sync.WaitGroup) []*producer {
	var producers []*producer

	wg.Add(producersCount)

	for i := 0; i < producersCount; i++ {
		producer := &producer{
			eventChannel: eventChannel,
			wg:           wg,
		}
		producers = append(producers, producer)

		go producer.start(time.Duration(1000/msgBySec) * time.Millisecond)
	}

	return producers
}

type producer struct {
	eventChannel chan<- struct{}
	tm           *time.Ticker
	wg           *sync.WaitGroup
}

func (p *producer) start(sendEvery time.Duration) {
	p.tm = time.NewTicker(sendEvery)
	for range p.tm.C {
		p.eventChannel <- struct{}{}
	}
}

func (p *producer) stop() {
	p.tm.Stop()
	time.Sleep(100 * time.Millisecond)
	p.wg.Done()
}

type consumer struct {
	repository     repositories.TestRepository
	queryTimeout   uint
	contextTimeout uint
	eventChannel   <-chan struct{}
	spentFunc      func(int64)
	errorFunc      func()
}

func (c *consumer) start() {

	for range c.eventChannel {
		size := rand.Intn(400-100) + 100 //pseudo random it's ok
		start := time.Now()
		_, err := c.repository.GetStores(uint(size), c.queryTimeout, c.contextTimeout)
		spent := time.Since(start).Milliseconds()
		c.spentFunc(spent)
		if err != nil {
			c.errorFunc()
			logrus.Error(err)
		}
	}
}

const loremp = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
	"Praesent in lacinia magna. Aenean vitae maximus sem. " +
	"Quisque pharetra augue et mollis sollicitudin. " +
	"Mauris vehicula eros lorem. Donec non sodales neque. " +
	"Nullam malesuada ligula vel enim mattis tincidunt. " +
	"Praesent non ornare nunc, at vehicula leo. " +
	"Aenean et placerat orci. Nullam faucibus sodales diam vel volutpat. " +
	"Nulla tempor quis quam in ullamcorper."

func ensureData(repository repositories.TestRepository) ([]string, error) {

	count, err := repository.Count()
	if err != nil {
		return nil, err
	}

	if count > 0 {
		repository.Clear()
	}

	var storeIds []string
	var data []repositories.Store
	for i := 0; i < 10000; i++ {
		storeId := GenerateId()
		storeIds = append(storeIds, storeId)
		data = append(data, repositories.Store{
			StoreId:   storeId,
			Name:      "name: " + strconv.Itoa(i),
			HugeValue: loremp,
		})
	}
	err = repository.Insert(data)
	return storeIds, err
}
