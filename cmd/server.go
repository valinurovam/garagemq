package main

import (
	"github.com/valinurovam/garagemq/server"
	log "github.com/sirupsen/logrus"
	"os"
	"github.com/valinurovam/garagemq/amqp"
)

func init() {
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	srv := server.NewServer("localhost", "5672", amqp.ProtoRabbit)
	srv.Start()
	defer srv.Stop()
}
