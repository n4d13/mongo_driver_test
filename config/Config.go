package config

import (
	"log"
	"os"
	"strconv"
	"strings"
)

type AppConfig struct {
	Port     int
	BasePath string
}

func LoadConfig() AppConfig {
	return AppConfig{
		Port:     getIntEnvOrDefault("SERVER_PORT", 8090),
		BasePath: getEnvOrDefault("SERVER_BASE_PATH", "/api/v1"),
	}
}

func getEnvOrDefault(envName string, defaultValue string) string {
	value := os.Getenv(envName)
	if value == "" {
		return defaultValue
	}

	return value
}

func getIntEnvOrDefault(envName string, defaultValue int) int {
	value := os.Getenv(envName)
	if strings.TrimSpace(value) == "" {
		return defaultValue
	}
	result, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("Wrong environment variable type. Expected '%s' of type int", envName)
	}
	return result
}
