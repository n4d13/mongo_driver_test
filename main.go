package main

import (
	"strconv"

	"github.com/n4d13/mongo_driver_test/config"
	"github.com/n4d13/mongo_driver_test/http"
	"github.com/sirupsen/logrus"
)

func main() {

	appConfig := config.LoadConfig()

	handler := http.NewRequestHandler()

	server, err := http.ConfigureRoutes(handler, appConfig)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.SetFormatter(&logrus.JSONFormatter{})

	server.Run(":" + strconv.Itoa(appConfig.Port))

}
