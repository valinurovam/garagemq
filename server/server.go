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

	"github.com/sasha-s/go-deadlock"
	log "github.com/sirupsen/logrus"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/auth"
	"github.com/valinurovam/garagemq/config"
	"github.com/valinurovam/garagemq/interfaces"
	"github.com/valinurovam/garagemq/metrics"
	"github.com/valinurovam/garagemq/msgstorage"
	"github.com/valinurovam/garagemq/srvstorage"
	"github.com/valinurovam/garagemq/storage"
)

type ServerState int

// server state statuses
const (
	Stopped ServerState = iota
	Running
	Stopping
)

type SrvMetricsState struct {
	Publish *metrics.TrackCounter
	Deliver *metrics.TrackCounter
	Confirm *metrics.TrackCounter
	Ack     *metrics.TrackCounter
	Get     *metrics.TrackCounter

	Ready   *metrics.TrackCounter
	Unacked *metrics.TrackCounter
	Total   *metrics.TrackCounter

	TrafficIn  *metrics.TrackCounter
	TrafficOut *metrics.TrackCounter
}

// Server implements AMQP server
type Server struct {
	connSeq      uint64
	host         string
	port         string
	protoVersion string
	listener     *net.TCPListener
	connLock     deadlock.Mutex
	connections  map[uint64]*Connection
	config       *config.Config
	users        map[string]string
	vhostsLock   deadlock.Mutex
	vhosts       map[string]*VirtualHost
	status       ServerState
	storage      *srvstorage.SrvStorage
	metrics      *SrvMetricsState

	wg       sync.WaitGroup
	shutdown chan struct{}
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
		connSeq:      0,
		shutdown:     make(chan struct{}),
	}
	server.initMetrics()

	return
}

func (srv *Server) initMetrics() {
	srv.metrics = &SrvMetricsState{
		Publish: metrics.AddCounter("server.publish"),
		Deliver: metrics.AddCounter("server.deliver"),
		Confirm: metrics.AddCounter("server.confirm"),
		Get:     metrics.AddCounter("server.get"),
		Ack:     metrics.AddCounter("server.acknowledge"),

		Ready:   metrics.AddCounter("server.ready"),
		Unacked: metrics.AddCounter("server.unacked"),
		Total:   metrics.AddCounter("server.total"),

		TrafficIn:  metrics.AddCounter("server.traffic_in"),
		TrafficOut: metrics.AddCounter("server.traffic_out"),
	}
}

// Start starts main server loop
func (srv *Server) Start() {
	log.WithFields(log.Fields{
		"pid": os.Getpid(),
	}).Info("Server starting")

	srv.wg.Add(1)
	go srv.hookSignals()

	srv.initServerStorage()
	srv.initUsers()
	if srv.storage.IsFirstStart() {
		srv.initDefaultVirtualHosts()
	} else {
		srv.initVirtualHostsFromStorage()
	}

	if err := srv.initListener(); err != nil {
		// todo handle error
	}

	srv.wg.Add(1)
	go srv.acceptConnections()

	srv.storage.UpdateLastStart()

	srv.wg.Wait()
}

// Stop stop server and all vhosts
func (srv *Server) Stop() {
	srv.vhostsLock.Lock()
	defer srv.vhostsLock.Unlock()

	close(srv.shutdown)
	// stop accept new connections
	if err := srv.listener.Close(); err != nil {
		log.WithError(err).Error("unable to close listener, unexpected error")
	}
	log.Info("Listener closed")

	var wg sync.WaitGroup
	srv.connLock.Lock()
	for _, conn := range srv.connections {
		conn := conn
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn.safeClose()
		}()
	}
	srv.connLock.Unlock()
	wg.Wait()
	log.Info("All connections safe closed")

	// stop exchanges and queues
	for _, virtualHost := range srv.vhosts {
		if err := virtualHost.Stop(); err != nil {
			log.WithError(err).WithFields(log.Fields{
				"vhost": virtualHost.name,
			}).Error("error on stop virtual host")
		}
	}

	if srv.storage != nil {
		if err := srv.storage.Close(); err != nil {
			log.WithError(err).Error("error on close server storage")
		}
	}

	srv.status = Stopped
}

func (srv *Server) getVhost(name string) *VirtualHost {
	srv.vhostsLock.Lock()
	defer srv.vhostsLock.Unlock()

	return srv.vhosts[name]
}

func (srv *Server) initListener() error {
	address := srv.host + ":" + srv.port
	tcpAddr, err := net.ResolveTCPAddr("tcp4", address)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"address": address,
		}).Error("Error resolving tcp address")
		return err
	}
	srv.listener, err = net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"address": address,
		}).Error("Error on listener start")
		return err
	}

	log.WithFields(log.Fields{
		"address": address,
	}).Info("Server started")

	daemonReady()

	return nil
}

func (srv *Server) acceptConnections() {
	defer srv.wg.Done()

	for {
		conn, err := srv.listener.AcceptTCP()
		if err != nil {
			select {
			case <-srv.shutdown:
				return
			}
			log.WithError(err).Error("unable to accept connection")
			continue
		}
		log.WithFields(log.Fields{
			"from": conn.RemoteAddr().String(),
			"to":   conn.LocalAddr().String(),
		}).Info("accepting connection")

		if srv.config.TCP.ReadBufSize > 0 {
			err = conn.SetReadBuffer(srv.config.TCP.ReadBufSize)
			if err != nil {
				log.WithError(err).Error("unable to accept connection: set read buffer")
				continue
			}
		}

		if srv.config.TCP.WriteBufSize > 0 {
			err = conn.SetWriteBuffer(srv.config.TCP.WriteBufSize)
			if err != nil {
				log.WithError(err).Error("unable to accept connection: set write buffer")
				continue
			}
		}

		err = conn.SetNoDelay(srv.config.TCP.Nodelay)
		if err != nil {
			log.WithError(err).Error("unable to accept connection: set no delay")
			continue
		}

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
			srv.config.Security.PasswordCheck,
		)
	}
	return false
}

func (srv *Server) initUsers() {
	for _, user := range srv.config.Users {
		srv.users[user.Username] = user.Password
	}
}

func (srv *Server) initServerStorage() {
	srv.storage = srvstorage.NewSrvStorage(srv.getStorageInstance("server", true), srv.protoVersion)
}

func (srv *Server) initDefaultVirtualHosts() {
	log.WithFields(log.Fields{
		"vhost": srv.config.Vhost.DefaultPath,
	}).Info("Initialize default vhost")

	log.Info("Initialize host message msgStorage")
	msgStoragePersistent := msgstorage.NewMsgStorage(srv.getStorageInstance("vhost_default", true), srv.protoVersion)
	msgStorageTransient := msgstorage.NewMsgStorage(srv.getStorageInstance("vhost_default", false), srv.protoVersion)

	srv.vhostsLock.Lock()
	defer srv.vhostsLock.Unlock()
	srv.vhosts[srv.config.Vhost.DefaultPath] = NewVhost(srv.config.Vhost.DefaultPath, true, msgStoragePersistent, msgStorageTransient, srv)
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
		msgStoragePersistent := msgstorage.NewMsgStorage(srv.getStorageInstance(storageName, true), srv.protoVersion)
		msgStorageTransient := msgstorage.NewMsgStorage(srv.getStorageInstance(storageName, false), srv.protoVersion)
		srv.vhosts[host] = NewVhost(host, system, msgStoragePersistent, msgStorageTransient, srv)
	}

	srv.vhostsLock.Lock()
	defer srv.vhostsLock.Unlock()
}

func (srv *Server) getStorageInstance(name string, isPersistent bool) interfaces.DbStorage {
	// very ugly solution, but don't know how to deal with "/" vhost for example
	// rabbitmq generate random uniq id for msgstore and touch .vhost file with vhost name into folder

	h := md5.New()
	h.Write([]byte(name))
	name = hex.EncodeToString(h.Sum(nil))

	// Sometimes we don't want to persist the database (e.g. when running unit tests or running without write permissions)
	// In that case, we can specify ":memory:" as the default path in the config file - only BuntDB supports it
	var stPath string

	inMemory := srv.config.Db.DefaultPath == config.DbPathMemory
	if inMemory && srv.config.Db.Engine != config.DbEngineTypeBuntDb {
		panic("Only BuntDB supports in-memory storage")
	}

	if inMemory {
		stPath = config.DbPathMemory
	} else {
		stPath = fmt.Sprintf("%s/%s/%s", srv.config.Db.DefaultPath, srv.config.Db.Engine, name)

		if !isPersistent {
			stPath += ".transient"
			if err := os.RemoveAll(stPath); err != nil {
				panic(err)
			}
		}

		if err := os.MkdirAll(stPath, 0777); err != nil {
			// TODO is here true way to handle error?
			panic(err)
		}
	}

	log.WithFields(log.Fields{
		"path":   stPath,
		"engine": srv.config.Db.Engine,
	}).Info("Open db storage")

	switch srv.config.Db.Engine {
	case config.DbEngineTypeBadger:
		return storage.NewBadger(stPath)
	case config.DbEngineTypeBuntDb:
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
	}
}

// Special method for calling in tests without os.Exit(0)
// Commented out as it's not currently used
// func (srv *Server) testOnSignal(sig os.Signal) {
// 	switch sig {
// 	case syscall.SIGTERM, syscall.SIGINT:
// 		srv.Stop()
// 	}
// }

func (srv *Server) hookSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		defer srv.wg.Done()

		for {
			select {
			case <-srv.shutdown:
				return
			case sig := <-c:
				log.Infof("Received [%d:%s] signal from OS", sig, sig.String())
				srv.onSignal(sig)
			}
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

func (srv *Server) GetVhost(name string) *VirtualHost {
	return srv.getVhost(name)
}

func (srv *Server) GetVhosts() map[string]*VirtualHost {
	return srv.vhosts
}

func (srv *Server) GetConnections() map[uint64]*Connection {
	return srv.connections
}

func (srv *Server) GetProtoVersion() string {
	return srv.protoVersion
}

func (srv *Server) GetMetrics() *SrvMetricsState {
	return srv.metrics
}
