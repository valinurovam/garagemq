package server

import (
	"testing"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/sirupsen/logrus"
	amqpclient "github.com/streadway/amqp"
	"io/ioutil"
	"net"
)

func init() {
	logrus.SetOutput(ioutil.Discard)
}

type ServerClient struct {
	server *Server
	client *amqpclient.Connection
}

func getDefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Users: []ConfigUser{
			{
				Username: "guest",
				Password: "$2a$14$OR8Od7QJ4yjck89RNWM0TeYJrQSIZLQ13ptktd3n.bStXuhZTcnuq", // guest hash
			},
		},
		Tcp: TcpConfig{
			Nodelay:      false,
			ReadBufSize:  0,
			WriteBufSize: 0,
		},
		Queue: Queue{
			ShardSize: 128,
		},
	}
}
func getNewSC(config *ServerConfig) (*ServerClient, error) {
	sc := &ServerClient{}
	sc.server = NewServer("localhost", "0", amqp.ProtoRabbit, config)
	clientNet, serverNet, err := networkSim()
	if err != nil {
		return nil, err
	}
	sc.server.acceptConnection(serverNet)

	clientConfig := amqpclient.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return clientNet, nil
		},
	}
	sc.client, err = amqpclient.DialConfig("amqp://localhost:0", clientConfig)
	if err != nil {
		return nil, err
	}

	return sc, nil
}

func networkSim() (net.Conn, *net.TCPConn, error) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	listener, err := net.ListenTCP("tcp", tcpAddr)
	defer listener.Close()
	c1, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		return nil, nil, err
	}

	c2, err := listener.AcceptTCP()
	if err != nil {
		c1.Close()
		return nil, nil, err
	}

	return c1, c2, nil
}

func TestConnectionSuccess(t *testing.T) {
	_, err := getNewSC(getDefaultServerConfig())
	if err != nil {
		t.Fatal(err)
	}
}

func TestConnectionFailed_WhenWrongAuth(t *testing.T) {
	config := getDefaultServerConfig()
	config.Users = []ConfigUser{
		{
			Username: "guest",
			Password: "guest?",
		},
	}
	_, err := getNewSC(config)
	if err == nil {
		t.Fatal("Expected auth error, nil given")
	}
}
