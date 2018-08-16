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
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/auth"
	"github.com/valinurovam/garagemq/config"
	"github.com/valinurovam/garagemq/interfaces"
	"github.com/valinurovam/garagemq/msgstorage"
	"github.com/valinurovam/garagemq/srvstorage"
	"github.com/valinurovam/garagemq/storage"
)

// server state statuses
const (
	Started  = iota
	Stopping
)

// Server implements AMQP server
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
	vhosts       map[string]*VirtualHost
	status       int
	storage      *srvstorage.SrvStorage
}

// NewServer returns new instance of AMQP Server
func NewServer(host string, port string, protoVersion string, config *config.Config) (server *Server) {
	server = &Server{
		host:         host,
		port:         port,
		connections:  make(map[uint64]*Connection),
		protoVersion: protoVersion,
		config:       config,
		users:        make(map[string]string),
		vhosts:       make(map[string]*VirtualHost),
		connSeq:      1,
	}
	return
}

// Start start main server loop
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

// Stop stop server and all vhosts
func (srv *Server) Stop() {
	srv.vhostsLock.Lock()
	defer srv.vhostsLock.Unlock()
	srv.status = Stopping

	// stop accept new connections
	srv.listener.Close()

	var wg sync.WaitGroup
	for _, conn := range srv.connections {
		wg.Add(1)
		go conn.safeClose(&wg)
	}
	wg.Wait()

	// stop exchanges and queues
	for _, virtualHost := range srv.vhosts {
		virtualHost.Stop()
	}

	if srv.storage != nil {
		srv.storage.Close()
	}
}

func (srv *Server) getVhost(name string) *VirtualHost {
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

		conn.SetReadBuffer(srv.config.TCP.ReadBufSize)
		conn.SetWriteBuffer(srv.config.TCP.WriteBufSize)
		conn.SetNoDelay(srv.config.TCP.Nodelay)

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

func (srv *Server) removeConnection(connID uint64) {
	srv.connLock.Lock()
	defer srv.connLock.Unlock()

	delete(srv.connections, connID)
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
	srv.storage = srvstorage.NewSrvStorage(srv.getStorageInstance("server"), srv.protoVersion)
}

func (srv *Server) initDefaultVirtualHosts() {
	log.WithFields(log.Fields{
		"vhost": srv.config.Vhost.DefaultPath,
	}).Info("Initialize default vhost")

	log.Info("Initialize host message msgStorage")
	msgStorage := msgstorage.NewMsgStorage(srv.getStorageInstance("vhost_default"), srv.protoVersion)

	srv.vhostsLock.Lock()
	defer srv.vhostsLock.Unlock()
	srv.vhosts[srv.config.Vhost.DefaultPath] = NewVhost(srv.config.Vhost.DefaultPath, true, msgStorage, srv)
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
		msgStorage := msgstorage.NewMsgStorage(srv.getStorageInstance(storageName), srv.protoVersion)
		srv.vhosts[host] = NewVhost(host, system, msgStorage, srv)
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
	case "buntdb":
		return storage.NewBuntDB(stPath)
	default:
		srv.stopWithError(nil, fmt.Sprintf("Unknown db engine '%s'", srv.config.Db.Engine))
	}
	return nil
}

func (srv *Server) onSignal(sig os.Signal) {
	switch sig {
	case syscall.SIGTERM, syscall.SIGINT:
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

func (srv *Server) getConfirmChannel(meta *amqp.ConfirmMeta) *Channel {
	srv.connLock.Lock()
	defer srv.connLock.Unlock()
	conn := srv.connections[meta.ConnID]
	if conn == nil {
		return nil
	}

	return conn.getChannel(meta.ChanID)
}
