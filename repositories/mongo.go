package repositories

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoDBConfiguration struct {
	DbName         string
	CollectionName string
	ConnString     string
	MinPool        uint64
	MaxPool        uint64
	IdleTimeout    time.Duration
	SocketTimeout  time.Duration
}

type mongoRepository struct {
	client           *mongo.Client
	storesCollection *mongo.Collection
	queryCount       int64
	validIds         []string
}

type TestRepository interface {
	GetStores(uint, uint, uint) ([]Store, error)
	Insert([]Store) error
	Count() (int64, error)
	QueryCount() int64
	Close()
	Clear()
	SetValidIds([]string)
}

func NewMongodbRepository(config *MongoDBConfiguration, monitorFunc func(*event.PoolEvent)) (TestRepository, error) {

	client, err := CreateClient(config, monitorFunc)

	if err != nil {
		return nil, err
	}

	database := client.Database(config.DbName)

	repository := &mongoRepository{
		client:           client,
		storesCollection: database.Collection(config.CollectionName),
	}

	logrus.Info("A MongoDBRepository was initialized")
	return repository, nil
}

func CreateClient(config *MongoDBConfiguration, monitorFunc func(*event.PoolEvent)) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	clientOptions := options.Client().ApplyURI(config.ConnString).
		SetReadPreference(readpref.SecondaryPreferred()).
		SetMaxConnIdleTime(config.IdleTimeout).
		SetMaxPoolSize(config.MaxPool).
		SetMinPoolSize(config.MinPool).
		SetSocketTimeout(config.SocketTimeout).
		SetPoolMonitor(
			&event.PoolMonitor{
				Event: monitorFunc,
			})

	db, err := mongo.Connect(ctx, clientOptions)

	if err != nil {
		return nil, err
	}
	er := db.Ping(ctx, readpref.SecondaryPreferred())
	if clientOptions.Auth != nil {
		if clientOptions.Auth.AuthSource != "" {
			config.DbName = clientOptions.Auth.AuthSource
		}
	}

	if er != nil {
		return nil, er
	}

	logrus.Info(fmt.Sprintf("Database's connection done. URL: %v - Database: %v", config.ConnString, config.DbName))

	_ = ensureIndex(db.Database(config.DbName).Collection(config.CollectionName))

	return db, nil
}

func ensureIndex(col *mongo.Collection) error {
	idxs, err := col.Indexes().List(context.TODO())
	idxName := "store_id_ux"
	if err != nil {
		return err
	}
	var exists = false
	for idxs.Next(context.Background()) {
		name := idxs.Current.Lookup("name")
		if name.String() == idxName {
			exists = true
		}
	}
	if !exists {
		_, err = col.Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys:    bson.M{"store_id": 1},
			Options: options.Index().SetName(idxName).SetUnique(true),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *mongoRepository) GetStores(size uint, queryTimeout uint, contextTimeout uint) ([]Store, error) {

	idsList := bson.A{}
	maxPosition := len(m.validIds)

	for i := 0; i < int(size); i++ {
		var random = rand.New(rand.NewSource(time.Now().UnixNano()))
		current := random.Intn(maxPosition)
		idsList = append(idsList, m.validIds[current])
	}

	filter := bson.M{"store_id": bson.M{"$in": idsList}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(contextTimeout)*time.Millisecond)
	defer cancel()

	atomic.AddInt64(&m.queryCount, 1)

	fOptions := options.Find().
		SetMaxTime(time.Duration(queryTimeout) * time.Millisecond).
		SetBatchSize(int32(len(idsList)))

	records, err := m.storesCollection.Find(ctx, filter, fOptions)
	if records != nil {
		defer records.Close(ctx)
	}

	if err != nil {
		return nil, err
	}

	var stores []Store
	err = records.All(ctx, &stores)
	if err != nil {
		return nil, err
	}

	return stores, nil
}

func (m *mongoRepository) Insert(stores []Store) error {

	var operations []mongo.WriteModel

	for _, store := range stores {
		operations = append(operations, &mongo.InsertOneModel{
			Document: bson.M{"store_id": store.StoreId, "name": store.Name, "hugeValue": store.HugeValue},
		})
	}

	_, err := m.storesCollection.BulkWrite(context.Background(), operations)
	return err
}

func (m *mongoRepository) Count() (int64, error) {
	return m.storesCollection.CountDocuments(context.TODO(), bson.M{})
}

func (m *mongoRepository) QueryCount() int64 {
	return m.queryCount
}
func (m *mongoRepository) Close() {
	_ = m.client.Disconnect(context.TODO())
}

func (m *mongoRepository) Clear() {
	_ = m.storesCollection.Drop(context.Background())
	_ = ensureIndex(m.storesCollection)
}

func (m *mongoRepository) SetValidIds(ids []string) {
	m.validIds = ids
}
