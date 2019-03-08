package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/valinurovam/garagemq/admin"
	"github.com/valinurovam/garagemq/config"
	"github.com/valinurovam/garagemq/metrics"
	"github.com/valinurovam/garagemq/server"
)

func init() {
	viper.SetEnvPrefix("gmq")
	viper.AutomaticEnv()
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)

	flag.Bool("help", false, "Shows the help message")
	flag.String("config", "", "The configuration file to use for the GarageMQ.")

	var levels []string
	for _, l := range logrus.AllLevels {
		levels = append(levels, l.String())
	}
	flag.String("log-file", "stdout", "Log file")
	flag.String("log-level", "info", fmt.Sprintf("Log level (%s)", strings.Join(levels, ", ")))
	flag.Bool("hprof", false, "Starts server with hprof profiler.")
	flag.String("hprof-host", "0.0.0.0", "hprof profiler host.")
	flag.String("hprof-port", "8080", "hprof profiler port.")

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
}

func main() {
	if viper.GetBool("help") {
		flag.Usage()
		os.Exit(0)
	}

	initLogger(viper.GetString("log-level"), viper.GetString("log-file"))
	var cfg *config.Config
	var err error
	if viper.GetString("config") != "" {
		if cfg, err = config.CreateFromFile(viper.GetString("config")); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	} else {
		cfg, _ = config.CreateDefault()
	}

	if viper.GetBool("hprof") {
		// for hprof debugging
		go http.ListenAndServe(fmt.Sprintf("%s:%s", viper.GetString("hprof-host"), viper.GetString("hprof-port")), nil)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	metrics.NewTrackRegistry(15, time.Second, false)

	srv := server.NewServer(cfg.TCP.IP, cfg.TCP.Port, cfg.Proto, cfg)
	adminServer := admin.NewAdminServer(srv, cfg.Admin.IP, cfg.Admin.Port)

	// Start admin server
	go func() {
		if err := adminServer.Start(); err != nil {
			panic("Failed to start adminServer - " + err.Error())
		}
	}()

	// Start GarageMQ broker
	srv.Start()
}

func initLogger(lvl string, path string) {
	level, err := logrus.ParseLevel(lvl)
	if err != nil {
		panic(err)
	}

	var output io.Writer
	switch path {
	case "stdout":
		output = os.Stdout
	case "stderr":
		output = os.Stderr
	default:
		output, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			panic(err)
		}
	}

	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	logrus.SetOutput(output)
	logrus.SetLevel(level)
}
