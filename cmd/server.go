package main

import (
	"github.com/valinurovam/garagemq/server"
	log "github.com/sirupsen/logrus"
	"os"
	"github.com/valinurovam/garagemq/amqp"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"syscall"
	"runtime"
	"fmt"
	_ "net/http/pprof"
	"net/http"
)

func init() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	// for hprof debugging
	go http.ListenAndServe("0.0.0.0:8080", nil)

	if n, _ := syscall.SysctlUint32("hw.ncpu"); n > 0 {
		fmt.Println("Set GOMAXPROCS", n)
		runtime.GOMAXPROCS(int(n))
	}

	config := server.ServerConfig{}
	file, _ := ioutil.ReadFile("etc/config.yaml")
	yaml.Unmarshal(file, &config)

	srv := server.NewServer("localhost", "5672", amqp.ProtoRabbit, &config)
	srv.Start()
	defer srv.Stop()
}
