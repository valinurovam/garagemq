package server

import (
	"io/ioutil"
	"net"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	amqpclient "github.com/streadway/amqp"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/config"
	"github.com/valinurovam/garagemq/metrics"
)

var emptyTable = make(amqpclient.Table)

func init() {
	logrus.SetOutput(ioutil.Discard)
	//logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	//logrus.SetOutput(os.Stdout)
	//logrus.SetLevel(logrus.DebugLevel)
}

type ServerClient struct {
	server *Server
	client *amqpclient.Connection
	// we need second connection for exclusive locked tests
	clientEx *amqpclient.Connection
}

type TestConfig struct {
	srvConfig    config.Config
	clientConfig amqpclient.Config
}

func (sc *ServerClient) clean() {
	cfg := getDefaultTestConfig()
	// sc.server.Stop take a long time for tests and better just increase limits to open files
	// ulimit -n 1024
	//sc.server.Stop()
	os.RemoveAll(cfg.srvConfig.Db.DefaultPath)
	metrics.Destroy()
}

func getDefaultTestConfig() TestConfig {
	return TestConfig{
		srvConfig: config.Config{
			Users: []config.User{
				{
					Username: "test",
					Password: "084e0343a0486ff05530df6c705c8bb4", // guest md5 hash
				},
				{
					Username: "guest",
					Password: "084e0343a0486ff05530df6c705c8bb4", // guest md5 hash
				},
			},
			TCP: config.TCPConfig{
				Nodelay:      false,
				ReadBufSize:  0,
				WriteBufSize: 0,
			},
			Queue: config.Queue{
				ShardSize: 128,
			},
			Db: config.Db{
				DefaultPath: "db_test",
				Engine:      "badger",
			},
			Vhost: config.Vhost{
				DefaultPath: "/",
			},
			Security: config.Security{
				PasswordCheck: "md5",
			},
			Connection: config.Connection{
				ChannelsMax:  4096,
				FrameMaxSize: 65536,
			},
		},
		clientConfig: amqpclient.Config{},
	}
}
func getNewSC(config TestConfig) (*ServerClient, error) {
	metrics.NewTrackRegistry(15, time.Second, true)
	sc := &ServerClient{}
	sc.server = NewServer("localhost", "0", amqp.ProtoRabbit, &config.srvConfig)
	sc.server.initServerStorage()
	sc.server.initUsers()
	sc.server.initDefaultVirtualHosts()

	toServer, toServerEx, fromClient, fromClientEx, err := networkSim()
	if err != nil {
		return sc, err
	}
	sc.server.acceptConnection(fromClient)
	sc.server.acceptConnection(fromClientEx)

	clientConfig := config.clientConfig
	clientConfig.Dial = func(network, addr string) (net.Conn, error) {
		return toServer, nil
	}

	sc.client, err = amqpclient.DialConfig("amqp://localhost:0", clientConfig)
	if err != nil {
		return sc, err
	}

	clientConfigEx := config.clientConfig
	clientConfigEx.Dial = func(network, addr string) (net.Conn, error) {
		return toServerEx, nil
	}
	sc.clientEx, err = amqpclient.DialConfig("amqp://localhost:0", clientConfigEx)
	if err != nil {
		return sc, err
	}

	return sc, nil
}

func networkSim() (net.Conn, net.Conn, *net.TCPConn, *net.TCPConn, error) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	listener, err := net.ListenTCP("tcp", tcpAddr)
	defer listener.Close()
	toServer1, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		return nil, nil, nil, nil, err
	}

	toServer2, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		return nil, nil, nil, nil, err
	}

	fromClient1, err := listener.AcceptTCP()
	if err != nil {
		toServer1.Close()
		return nil, nil, nil, nil, err
	}

	fromClient2, err := listener.AcceptTCP()
	if err != nil {
		toServer2.Close()
		return nil, nil, nil, nil, err
	}

	return toServer1, toServer2, fromClient1, fromClient2, nil
}

func getServerChannel(sc *ServerClient, id uint16) *Channel {
	channels := sc.server.connections[sc.server.connSeq-1].channels
	return channels[id]
}
