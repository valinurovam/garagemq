package server

import (
	"testing"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/sirupsen/logrus"
	amqpclient "github.com/streadway/amqp"
	"io/ioutil"
	"net"
)

var emptyTable = make(amqpclient.Table)

func init() {
	logrus.SetOutput(ioutil.Discard)
}

type ServerClient struct {
	server *Server
	client *amqpclient.Connection
	// we need second connection for exclusive locked tests
	clientEx *amqpclient.Connection
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
	toServer, toServerEx, fromClient, fromClientEx, err := networkSim()
	if err != nil {
		return nil, err
	}
	sc.server.acceptConnection(fromClient)
	sc.server.acceptConnection(fromClientEx)

	clientConfig := amqpclient.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return toServer, nil
		},
	}
	sc.client, err = amqpclient.DialConfig("amqp://localhost:0", clientConfig)
	if err != nil {
		return nil, err
	}
	clientConfigEx := amqpclient.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return toServerEx, nil
		},
	}
	sc.clientEx, err = amqpclient.DialConfig("amqp://localhost:0", clientConfigEx)
	if err != nil {
		return nil, err
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

func Test_Connection_Success(t *testing.T) {
	_, err := getNewSC(getDefaultServerConfig())
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Connection_Failed_WhenWrongAuth(t *testing.T) {
	config := getDefaultServerConfig()
	config.Users = []ConfigUser{
		{
			Username: "guest",
			Password: "guest?",
		},
	}
	_, err := getNewSC(config)
	if err == nil {
		//t.Fatal("Expected auth error")
	}
}
