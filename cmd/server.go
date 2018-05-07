package main

import (
	"github.com/valinurovam/garagemq/server"
	log "github.com/sirupsen/logrus"
	"os"
)

func init() {
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main()  {
	srv := server.NewServer("localhost", "5672")
	srv.Start()
	defer srv.Stop()
}
