package main

import (
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/valinurovam/garagemq/admin"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/config"
	"github.com/valinurovam/garagemq/metrics"
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
	//go http.ListenAndServe("0.0.0.0:8080", nil)

	//if n, _ := syscall.SysctlUint32("hw.ncpu"); n > 0 {
	//	runtime.GOMAXPROCS(int(n))
	//}
	runtime.GOMAXPROCS(8)

	metrics.NewTrackRegistry(15, time.Second)

	cfg := config.Config{}
	file, _ := ioutil.ReadFile("etc/config.yaml")
	yaml.Unmarshal(file, &cfg)

	srv := server.NewServer(cfg.TCP.IP, cfg.TCP.Port, amqp.ProtoRabbit, &cfg)
	adminServer := admin.NewAdminServer(srv)
	go adminServer.Start()
	srv.Start()
	defer srv.Stop()
}
