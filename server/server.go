package server

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/valinurovam/garagemq/auth"
	"github.com/valinurovam/garagemq/config"
	"github.com/valinurovam/garagemq/interfaces"
	"github.com/valinurovam/garagemq/msgstorage"
	"github.com/valinurovam/garagemq/srvstorage"
	"github.com/valinurovam/garagemq/storage"
	"github.com/valinurovam/garagemq/vhost"
)

const (
	Started  = iota
	Stopping
)

type Server struct {
	host         string
	port         string
	protoVersion string
	listener     *net.TCPListener
	connSeq      uint64
	connLock     sync.Mutex
	connections  map[uint64]*Connection
	config       *config.Config
	users        map[string]string
	vhostsLock   sync.Mutex
	vhosts       map[string]*vhost.VirtualHost
	status       int
	storage      *srvstorage.SrvStorage
}

func NewServer(host string, port string, protoVersion string, config *config.Config) (server *Server) {
	server = &Server{
		host:         host,
		port:         port,
		connections:  make(map[uint64]*Connection),
		protoVersion: protoVersion,
		config:       config,
		users:        make(map[string]string),
		vhosts:       make(map[string]*vhost.VirtualHost),
		connSeq:      1,
	}
	return
}

func (srv *Server) Start() {
	log.WithFields(log.Fields{
		"pid": os.Getpid(),
	}).Info("Server starting")

	go srv.hookSignals()

	srv.initServerStorage()
	srv.initUsers()
	if srv.storage.IsFirstStart() {
		srv.initDefaultVirtualHosts()
	} else {
		srv.initVirtualHostsFromStorage()
	}

	go srv.listen()

	srv.storage.UpdateLastStart()
	srv.status = Started
	select {}
}

func (srv *Server) Stop() {
	srv.vhostsLock.Lock()
	defer srv.vhostsLock.Unlock()
	srv.status = Stopping

	// stop accept new connections
	srv.listener.Close()

	// TODO Critical: close connections or just stop receive data!

	// stop exchanges and queues
	// cancel consumers
	// close connections
	// stop vhosts
	for _, virtualHost := range srv.vhosts {
		virtualHost.Stop()
	}
	srv.storage.Close()
}

func (srv *Server) GetVhost(name string) *vhost.VirtualHost {
	srv.vhostsLock.Lock()
	defer srv.vhostsLock.Unlock()

	return srv.vhosts[name]
}

func (srv *Server) listen() {
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
	}).Info("Server started")

	for {
		conn, err := srv.listener.AcceptTCP()
		if err != nil {
			if srv.status == Stopping {
				return
			}
			srv.stopWithError(err, "accepting connection")
		}
		log.WithFields(log.Fields{
			"from": conn.RemoteAddr().String(),
			"to":   conn.LocalAddr().String(),
		}).Info("accepting connection")

		conn.SetReadBuffer(srv.config.Tcp.ReadBufSize)
		conn.SetWriteBuffer(srv.config.Tcp.WriteBufSize)
		conn.SetNoDelay(srv.config.Tcp.Nodelay)

		srv.acceptConnection(conn)
	}
}

func (srv *Server) stopWithError(err error, msg string) {
	log.WithError(err).Error(msg)
	srv.Stop()
	os.Exit(1)
}

func (srv *Server) acceptConnection(conn *net.TCPConn) {
	srv.connLock.Lock()
	defer srv.connLock.Unlock()

	connection := NewConnection(srv, conn)
	srv.connections[connection.id] = connection
	go connection.handleConnection()
}

func (srv *Server) removeConnection(connId uint64) {
	srv.connLock.Lock()
	defer srv.connLock.Unlock()

	delete(srv.connections, connId)
}

func (srv *Server) checkAuth(saslData auth.SaslData) bool {
	for userName, passwordHash := range srv.users {
		if userName != saslData.Username {
			continue
		}

		return auth.CheckPasswordHash(
			saslData.Password,
			passwordHash,
			srv.config.Security.PasswordCheck == "md5",
		);
	}
	return false
}

func (srv *Server) initUsers() {
	for _, user := range srv.config.Users {
		srv.users[user.Username] = user.Password
	}
}

func (srv *Server) initServerStorage() {
	srv.storage = srvstorage.New(srv.getStorageInstance("server"), srv.protoVersion)
}

func (srv *Server) initDefaultVirtualHosts() {
	log.WithFields(log.Fields{
		"vhost": srv.config.Vhost.DefaultPath,
	}).Info("Initialize default vhost")

	log.Info("Initialize host message msgStorage")
	msgStorage := msgstorage.New(srv.getStorageInstance("vhost_default"), srv.protoVersion)

	srv.vhostsLock.Lock()
	defer srv.vhostsLock.Unlock()
	srv.vhosts[srv.config.Vhost.DefaultPath] = vhost.New(srv.config.Vhost.DefaultPath, true, msgStorage, srv.storage, srv.config)
	srv.storage.AddVhost(srv.config.Vhost.DefaultPath, true)
}

func (srv *Server) initVirtualHostsFromStorage() {
	log.Info("Initialize vhosts")

	vhosts := srv.storage.GetVhosts()
	for host, system := range vhosts {
		log.WithFields(log.Fields{
			"vhost": srv.config.Vhost.DefaultPath,
		}).Info("Initialize host message msgStorage")

		var storageName string
		if host == srv.config.Vhost.DefaultPath {
			storageName = "vhost_default"
		} else {
			storageName = host
		}
		msgStorage := msgstorage.New(srv.getStorageInstance(storageName), srv.protoVersion)
		srv.vhosts[host] = vhost.New(host, system, msgStorage, srv.storage, srv.config)
	}


	srv.vhostsLock.Lock()
	defer srv.vhostsLock.Unlock()
}

func (srv *Server) getStorageInstance(name string) interfaces.DbStorage {
	// very ugly solution, but don't know how to deal with "/" vhost for example
	// rabbitmq generate random uniq id for msgstore and touch .vhost file with vhost name into folder

	h := md5.New()
	h.Write([]byte(name))
	name = hex.EncodeToString(h.Sum(nil))

	stPath := fmt.Sprintf("%s/%s/%s", srv.config.Db.DefaultPath, srv.config.Db.Engine, name)

	if err := os.MkdirAll(stPath, 0777); err != nil {
		// TODO is here true way to handle error?
		panic(err)
	}

	log.WithFields(log.Fields{
		"path":   stPath,
		"engine": srv.config.Db.Engine,
	}).Info("Open db storage")

	switch srv.config.Db.Engine {
	case "badger":
		return storage.NewBadger(stPath)
	case "bunt":
		return storage.NewBunt(stPath)
	default:
		srv.stopWithError(nil, fmt.Sprintf("Unknown db engine '%s'", srv.config.Db.Engine))
	}
	return nil
}

func (srv *Server) onSignal(sig os.Signal) {
	switch sig {
	case syscall.SIGTERM:
		fallthrough
	case syscall.SIGINT:
		srv.Stop()
		os.Exit(0)
	}
}

func (srv *Server) hookSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for sig := range c {
			log.Infof("Received [%d:%s] signal from OS", sig, sig.String())
			srv.onSignal(sig)
		}
	}()
}
