package main

import (
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	log "github.com/sirupsen/logrus"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/config"
	"github.com/valinurovam/garagemq/server"
	"gopkg.in/yaml.v2"
)

func init() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	// for hprof debugging
	go http.ListenAndServe("0.0.0.0:8080", nil)

	//if n, _ := syscall.SysctlUint32("hw.ncpu"); n > 0 {
	//	runtime.GOMAXPROCS(int(n))
	//}
	runtime.GOMAXPROCS(8)

	config := config.Config{}
	file, _ := ioutil.ReadFile("etc/config.yaml")
	yaml.Unmarshal(file, &config)

	srv := server.NewServer("localhost", "5672", amqp.ProtoRabbit, &config)
	srv.Start()
	defer srv.Stop()
}
