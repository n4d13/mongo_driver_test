package http

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/n4d13/mongo_driver_test/repositories"
	"github.com/n4d13/mongo_driver_test/stage"
	"github.com/sirupsen/logrus"
)

type RequestHandler struct {
}

func NewRequestHandler() *RequestHandler {
	return &RequestHandler{}
}

func (r *RequestHandler) RunTest(c *gin.Context) {
	var requestBody TestConfig
	if err := c.ShouldBindJSON(&requestBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if result := validateConfig(&requestBody); len(result) > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"validations": fmt.Sprintf("%+v", result)})
		return
	}

	logrus.Infof("Running test stage with: %+v", requestBody)

	stageImpl := stage.New(
		repositories.MongoDBConfiguration{
			DbName:         requestBody.DBConfig.DbName,
			CollectionName: requestBody.DBConfig.CollectionName,
			ConnString:     requestBody.DBConfig.ConnString,
			MinPool:        uint64(requestBody.DBConfig.MinPoolSize),
			MaxPool:        uint64(requestBody.DBConfig.MaxPoolSize),
			IdleTimeout:    time.Duration(requestBody.DBConfig.IdleTimeout) * time.Second,
			SocketTimeout:  time.Duration(requestBody.DBConfig.SocketTimeout) * time.Second,
		}, stage.Config{
			WorkersCount:     requestBody.StageConfig.WorkersCount,
			WorkersToAdd:     requestBody.StageConfig.WorkersToAdd,
			IncrementLoad:    requestBody.StageConfig.IncrementLoad,
			ProducersCount:   requestBody.StageConfig.ProducersCount,
			MsgBySec:         requestBody.StageConfig.MsgBySec,
			TimeToSleepSecs:  requestBody.StageConfig.TimeToSleepSecs,
			TimeToFinishSecs: requestBody.StageConfig.TimeToFinishSecs,
			ContextTimeMs:    requestBody.StageConfig.ContextTimeOutMs,
			QueryTimeoutMs:   requestBody.StageConfig.QueryTimeoutMs,
		})

	go func() {
		stageImpl.Run()
	}()

	c.JSON(http.StatusCreated, gin.H{"status": "running"})
}

func validateConfig(requestBody *TestConfig) []string {
	var result []string

	if isEmpty(requestBody.DBConfig.DbName) {
		result = append(result, "Database' name is required")
	}
	if isEmpty(requestBody.DBConfig.ConnString) {
		result = append(result, "Connection string is required")
	}
	if isEmpty(requestBody.DBConfig.CollectionName) {
		result = append(result, "Collection name is required")
	}
	if isEmptyNumber(requestBody.DBConfig.MaxPoolSize) {
		result = append(result, "MaxPoolSize is required")
	}
	if isEmptyNumber(requestBody.DBConfig.SocketTimeout) {
		result = append(result, "Socket' timeout is required")
	}
	if isEmptyNumber(requestBody.StageConfig.WorkersCount) {
		result = append(result, "Workers count is required")
	}
	if isEmptyNumber(requestBody.StageConfig.WorkersToAdd) {
		result = append(result, "Workers to add is required")
	}
	if isEmptyNumber(requestBody.StageConfig.IncrementLoad) {
		result = append(result, "Increment load is required")
	}
	if isEmptyNumber(requestBody.StageConfig.MsgBySec) {
		result = append(result, "Messages per second is required")
	}
	if isEmptyNumber(requestBody.StageConfig.ProducersCount) {
		result = append(result, "Producers' count is required")
	}
	if isEmptyNumber(requestBody.StageConfig.TimeToSleepSecs) {
		result = append(result, "Time to sleep is required")
	}
	if isEmptyNumber(requestBody.StageConfig.TimeToFinishSecs) {
		result = append(result, "Time to finish is required")
	}
	if isEmptyNumber(requestBody.StageConfig.ContextTimeOutMs) {
		result = append(result, "Context timeout is required")
	}
	if isEmptyNumber(requestBody.StageConfig.QueryTimeoutMs) {
		result = append(result, "Query' timeout is required")
	}

	return result
}

func isEmpty(value string) bool {
	return strings.TrimSpace(value) == ""
}

func isEmptyNumber(value uint) bool {
	return value == 0
}

type TestConfig struct {
	DBConfig    DBConfig    `json:"db_config"`
	StageConfig StageConfig `json:"stage_config"`
}

type DBConfig struct {
	DbName         string `json:"db_name"`
	CollectionName string `json:"collection_name"`
	ConnString     string `json:"conn_string"`
	MinPoolSize    uint   `json:"min_pool_size"`
	MaxPoolSize    uint   `json:"max_pool_size"`
	IdleTimeout    uint   `json:"idle_timeout"`
	SocketTimeout  uint   `json:"socket_timeout"`
}

type StageConfig struct {
	WorkersCount     uint `json:"workers_count"`
	WorkersToAdd     uint `json:"workers_to_add"`
	IncrementLoad    uint `json:"increment_load"`
	ProducersCount   uint `json:"producers_count"`
	MsgBySec         uint `json:"msg_by_sec"`
	TimeToSleepSecs  uint `json:"time_to_sleep_secs"`
	TimeToFinishSecs uint `json:"time_to_finish_secs"`
	ContextTimeOutMs uint `json:"context_time_out_ms"`
	QueryTimeoutMs   uint `json:"query_timeout_ms"`
}
