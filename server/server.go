package server

import (
	"net"
	"os"
	log "github.com/sirupsen/logrus"
	"github.com/valinurovam/garagemq/auth"
	"github.com/valinurovam/garagemq/vhost"
	"sync"
)

type Server struct {
	host         string
	port         string
	protoVersion string
	listener     *net.TCPListener
	connSeq      int64
	connLock     sync.Mutex
	connections  map[int64]*Connection
	config       *ServerConfig
	users        map[string]string
	vhosts       map[string]*vhost.VirtualHost
}

func NewServer(host string, port string, protoVersion string, config *ServerConfig) (server *Server) {
	server = &Server{
		host:         host,
		port:         port,
		connections:  make(map[int64]*Connection),
		protoVersion: protoVersion,
		config:       config,
		users:        make(map[string]string),
		vhosts:       make(map[string]*vhost.VirtualHost),
	}

	server.initUsers()
	server.initVirtualHosts()
	return
}

func (srv *Server) Start() (err error) {
	address := srv.host + ":" + srv.port
	tcpAddr, err := net.ResolveTCPAddr("tcp4", address)
	srv.listener, err = net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"address": address,
		}).Error("Error on listener start")
		os.Exit(1)
	}

	log.WithFields(log.Fields{
		"address": address,
	}).Info("Server start")

	for {
		conn, err := srv.listener.AcceptTCP()
		conn.SetReadBuffer(srv.config.Tcp.ReadBufSize)
		conn.SetWriteBuffer(srv.config.Tcp.WriteBufSize)
		conn.SetNoDelay(srv.config.Tcp.Nodelay)

		if err != nil {
			log.WithError(err).Error("accepting connection")
			os.Exit(1)
		}
		log.WithFields(log.Fields{
			"from": conn.RemoteAddr().String(),
			"to":   conn.LocalAddr().String(),
		}).Info("accepting connection")
		srv.acceptConnection(conn)
	}
	return
}

func (srv *Server) Stop() {
	srv.listener.Close()
}

func (srv *Server) acceptConnection(conn *net.TCPConn) {
	srv.connLock.Lock()
	connection := NewConnection(srv, conn)
	srv.connections[connection.id] = connection
	srv.connLock.Unlock()
	go connection.handleConnection()
}

func (srv *Server) removeConnection(connId int64) {
	srv.connLock.Lock()
	delete(srv.connections, connId)
	srv.connLock.Unlock()
}

func (srv *Server) checkAuth(saslData auth.SaslData) bool {
	for userName, passwordHash := range srv.users {
		if userName != saslData.Username {
			continue
		}

		return auth.CheckPasswordHash(saslData.Password, passwordHash);
	}
	return false
}

func (srv *Server) initUsers() {
	for _, user := range srv.config.Users {
		srv.users[user.Username] = user.Password
	}
}

func (srv *Server) initVirtualHosts() {
	srv.vhosts["/"] = vhost.New("/", true)
}
