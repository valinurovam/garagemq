package server

import (
	"errors"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/binding"
	"github.com/valinurovam/garagemq/config"
	"github.com/valinurovam/garagemq/exchange"
	"github.com/valinurovam/garagemq/msgstorage"
	"github.com/valinurovam/garagemq/queue"
	"github.com/valinurovam/garagemq/srvstorage"
)

const EX_DEFAULT_NAME = ""

type VirtualHost struct {
	name       string
	system     bool
	exLock     sync.Mutex
	exchanges  map[string]*exchange.Exchange
	quLock     sync.Mutex
	queues     map[string]*queue.Queue
	msgStorage *msgstorage.MsgStorage
	srv        *Server
	srvStorage *srvstorage.SrvStorage
	srvConfig  *config.Config
	logger     *log.Entry
}

func NewVhost(name string, system bool, msgStorage *msgstorage.MsgStorage, srv *Server) *VirtualHost {
	vhost := &VirtualHost{
		name:       name,
		system:     system,
		exchanges:  make(map[string]*exchange.Exchange),
		queues:     make(map[string]*queue.Queue),
		msgStorage: msgStorage,
		srvStorage: srv.storage,
		srvConfig:  srv.config,
		srv:        srv,
	}

	vhost.logger = log.WithFields(log.Fields{
		"vhost": name,
	})

	vhost.initSystemExchanges()
	vhost.loadExchanges()
	vhost.loadQueues()

	vhost.logger.Info("Load messages into queues")

	vhost.loadMessagesIntoQueues()
	for _, q := range vhost.GetQueues() {
		q.Start()
		vhost.logger.WithFields(log.Fields{
			"name":   q.GetName(),
			"length": q.Length(),
		}).Info("Messages loaded into queue")
	}

	go vhost.handleConfirms()

	return vhost
}

func (vhost *VirtualHost) handleConfirms() {
	confirmsChan := vhost.msgStorage.ReceiveConfirms()
	for confirm := range confirmsChan {
		if !confirm.ConfirmMeta.IsConfirmable() {
			continue
		}
		channel := vhost.srv.getConfirmChannel(&confirm.ConfirmMeta)
		if channel == nil {
			continue
		}
		channel.AddConfirm(&confirm.ConfirmMeta)
	}
}

func (vhost *VirtualHost) initSystemExchanges() {
	vhost.logger.Info("Initialize host default exchanges...")
	for _, exType := range []byte{
		exchange.EX_TYPE_DIRECT,
		exchange.EX_TYPE_FANOUT,
		exchange.EX_TYPE_HEADERS,
		exchange.EX_TYPE_TOPIC,
	} {
		exTypeAlias, _ := exchange.GetExchangeTypeAlias(exType)
		exName := "amq." + exTypeAlias
		vhost.AppendExchange(exchange.New(exName, exType, true, false, false, true))
	}

	systemExchange := exchange.New(EX_DEFAULT_NAME, exchange.EX_TYPE_DIRECT, true, false, false, true)
	vhost.AppendExchange(systemExchange)
}

func (vhost *VirtualHost) GetQueue(name string) *queue.Queue {
	vhost.quLock.Lock()
	defer vhost.quLock.Unlock()
	return vhost.getQueue(name)
}

func (vhost *VirtualHost) GetQueues() map[string]*queue.Queue {
	vhost.quLock.Lock()
	defer vhost.quLock.Unlock()
	return vhost.queues
}

func (vhost *VirtualHost) getQueue(name string) *queue.Queue {
	return vhost.queues[name]
}

func (vhost *VirtualHost) GetExchange(name string) *exchange.Exchange {
	vhost.exLock.Lock()
	defer vhost.exLock.Unlock()
	return vhost.getExchange(name)
}

func (vhost *VirtualHost) getExchange(name string) *exchange.Exchange {
	return vhost.exchanges[name]
}

func (vhost *VirtualHost) GetDefaultExchange() *exchange.Exchange {
	return vhost.exchanges[EX_DEFAULT_NAME]
}

func (vhost *VirtualHost) AppendExchange(ex *exchange.Exchange) {
	vhost.exLock.Lock()
	defer vhost.exLock.Unlock()
	exTypeAlias, _ := exchange.GetExchangeTypeAlias(ex.ExType())
	vhost.logger.WithFields(log.Fields{
		"name": ex.GetName(),
		"type": exTypeAlias,
	}).Info("Append exchange")
	vhost.exchanges[ex.GetName()] = ex

	if ex.IsDurable() && !ex.IsSystem() {
		vhost.srvStorage.AddExchange(vhost.name, ex)
	}
}

func (vhost *VirtualHost) NewQueue(name string, connId uint64, exclusive bool, autoDelete bool, durable bool, shardSize int) *queue.Queue {
	return queue.NewQueue(
		name,
		connId,
		exclusive,
		autoDelete,
		durable,
		shardSize,
		vhost.msgStorage,
	)
}

func (vhost *VirtualHost) AppendQueue(qu *queue.Queue) {
	vhost.quLock.Lock()
	defer vhost.quLock.Unlock()
	vhost.logger.WithFields(log.Fields{
		"queueName": qu.GetName(),
	}).Info("Append queue")

	vhost.queues[qu.GetName()] = qu

	ex := vhost.GetDefaultExchange()
	bind := binding.New(qu.GetName(), EX_DEFAULT_NAME, qu.GetName(), &amqp.Table{}, false)
	ex.AppendBinding(bind)

	if qu.IsDurable() {
		vhost.srvStorage.AddQueue(vhost.name, qu)
	}
}

func (vhost *VirtualHost) loadQueues() {
	vhost.logger.Info("Initialize queues...")
	queueNames := vhost.srvStorage.GetVhostQueues(vhost.name)
	if len(queueNames) == 0 {
		return
	}
	for _, name := range queueNames {
		vhost.AppendQueue(
			vhost.NewQueue(name, 0, false, false, true, vhost.srvConfig.Queue.ShardSize),
		)
	}
}

func (vhost *VirtualHost) loadMessagesIntoQueues() {
	vhost.msgStorage.Iterate(func(queue string, message *amqp.Message) {
		q, ok := vhost.queues[queue]
		if !ok {
			return
		}

		q.Push(message, true)
	})
}

func (vhost *VirtualHost) loadExchanges() {
	vhost.logger.Info("Initialize exchanges...")
	exchanges := vhost.srvStorage.GetVhostExchanges(vhost.name)
	if len(exchanges) == 0 {
		return
	}
	for _, ex := range exchanges {
		vhost.AppendExchange(ex)
	}
}

func (vhost *VirtualHost) DeleteQueue(queueName string, ifUnused bool, ifEmpty bool) (uint64, error) {
	vhost.quLock.Lock()
	defer vhost.quLock.Unlock()

	qu := vhost.getQueue(queueName)
	if qu == nil {
		return 0, errors.New("not found")
	}

	var length, err = qu.Delete(ifUnused, ifEmpty)
	if err != nil {
		return 0, err
	}
	for _, ex := range vhost.exchanges {
		ex.RemoveQueueBindings(queueName)
	}
	vhost.srvStorage.DelQueue(vhost.name, qu)
	delete(vhost.queues, queueName)

	return length, nil
}

func (vhost *VirtualHost) Stop() error {
	vhost.quLock.Lock()
	vhost.exLock.Lock()
	defer vhost.quLock.Unlock()
	defer vhost.exLock.Unlock()
	vhost.logger.Info("Stop virtual host")
	for _, qu := range vhost.queues {
		qu.Stop()
		vhost.logger.WithFields(log.Fields{
			"queueName": qu.GetName(),
		}).Info("Queue stopped")
	}

	vhost.msgStorage.Close()
	vhost.logger.Info("Storage closed")
	return nil
}

func (vhost *VirtualHost) Name() string {
	return vhost.name
}
