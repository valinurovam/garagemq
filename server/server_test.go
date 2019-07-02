package server

import (
	"bytes"
	"io/ioutil"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	amqpclient "github.com/streadway/amqp"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/config"
	"github.com/valinurovam/garagemq/metrics"
)

var emptyTable = make(amqpclient.Table)
var proto = amqp.ProtoRabbit

func init() {
	logrus.SetOutput(ioutil.Discard)
	//logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	//logrus.SetOutput(os.Stdout)
	//logrus.SetLevel(logrus.InfoLevel)
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
	if sc.server != nil && sc.server.status == Running {
		sc.server.Stop()
	}
	if err:=os.RemoveAll(cfg.srvConfig.Db.DefaultPath); err != nil {
		panic(err)
	}
	metrics.Destroy()
}

func getDefaultTestConfig() TestConfig {
	return TestConfig{
		srvConfig: config.Config{
			Proto: "amqp-rabbit",
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
				ShardSize:        128,
				MaxMessagesInRAM: 4096,
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
	sc.server = NewServer("localhost", "0", proto, &config.srvConfig)
	sc.server.initServerStorage()
	sc.server.initUsers()
	sc.server.initDefaultVirtualHosts()
	sc.server.status = Running

	// the only chance to disable badger logger
	log.SetOutput(ioutil.Discard)

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

func TestServer_GetConnections(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()

	conns := sc.server.GetConnections()
	if len(conns) == 0 {
		t.Error("Expected at least test connections")
	}
}

func TestServer_GetProtoVersion(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()

	version := sc.server.GetProtoVersion()
	if version != proto {
		t.Errorf("Expected %s, actual %s", proto, version)
	}
}

func TestServer_GetVhosts(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()

	vhosts := sc.server.GetVhosts()
	if len(vhosts) == 0 {
		t.Error("Expected at least default vhost")
	}
	for _, vhost := range vhosts {
		if sc.server.GetVhost(vhost.GetName()) != vhost {
			t.Error("Expected equal vhosts")
		}
	}
}

func TestServer_RealStart(t *testing.T) {
	defer (&ServerClient{}).clean()
	cfg := getDefaultTestConfig()
	metrics.NewTrackRegistry(15, time.Second, true)
	server := NewServer("localhost", "55672", proto, &cfg.srvConfig)
	go server.Start()
	time.Sleep(2 * time.Second)
	defer server.Stop()

	conn, err := amqpclient.Dial("amqp://guest:guest@localhost:55672/")
	if err != nil {
		t.Error("Could not connect to real server", err)
		return
	}

	if len(server.connections) == 0 {
		t.Error("Expected connected client")
	}

	if len(server.connections[1].channels) == 0 {
		t.Error("Expected channels on connections")
	}
	defer conn.Close()
}

func TestServer_WrongProtocol(t *testing.T) {
	defer (&ServerClient{}).clean()
	cfg := getDefaultTestConfig()
	metrics.NewTrackRegistry(15, time.Second, true)
	server := NewServer("localhost", "55672", proto, &cfg.srvConfig)
	go server.Start()
	time.Sleep(2 * time.Second)
	defer server.Stop()

	amqpAddr, err := net.ResolveTCPAddr("tcp", "localhost:55672")
	connDial, err := net.DialTCP("tcp", nil, amqpAddr)
	if err != nil {
		t.Error("AMQP Dial failed:", err.Error())
	}

	_, err = connDial.Write([]byte{'A', 'M', 'Q', 'P', 0, 0, 0, 0})
	if err != nil {
		t.Error("Failed to send AMQP header", err.Error())
	}

	supported := make([]byte, 8)

	_, err = connDial.Read(supported)
	if err != nil {
		t.Error("Failed to read response from server")
	}

	if !bytes.Equal(supported, amqp.AmqpHeader) {
		t.Errorf("Expected %v, actual %v", amqp.AmqpHeader, supported)
	}
}
