package main

import (
	"github.com/valinurovam/garagemq/server"
	log "github.com/sirupsen/logrus"
	"os"
	"github.com/valinurovam/garagemq/amqp"
	"io/ioutil"
	"gopkg.in/yaml.v2"
)

func init() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	config := server.ServerConfig{}
	file, _ := ioutil.ReadFile("etc/config.yaml")
	yaml.Unmarshal(file, &config)

	srv := server.NewServer("", "5672", amqp.ProtoRabbit, &config)
	srv.Start()
	defer srv.Stop()
}
