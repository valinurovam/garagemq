package vhost

import (
	"github.com/valinurovam/garagemq/exchange"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/queue"
	"sync"
	"github.com/valinurovam/garagemq/binding"
)

const EX_DEFAULT_NAME = ""

type VirtualHost struct {
	name      string
	system    bool
	exLock    sync.Mutex
	exchanges map[string]*exchange.Exchange
	quLock    sync.Mutex
	queues    map[string]*queue.Queue
}

func New(name string, system bool) *VirtualHost {
	vhost := &VirtualHost{
		name:      name,
		system:    system,
		exchanges: make(map[string]*exchange.Exchange),
		queues:    make(map[string]*queue.Queue),
	}

	vhost.initSystemExchanges()

	return vhost
}

func (vhost *VirtualHost) initSystemExchanges() {
	for _, exType := range []int{
		exchange.EX_TYPE_DIRECT,
		exchange.EX_TYPE_FANOUT,
		exchange.EX_TYPE_HEADERS,
		exchange.EX_TYPE_TOPIC,
	} {
		exTypeAlias, _ := exchange.GetExchangeTypeAlias(exType)
		exName := "amq." + exTypeAlias
		vhost.exchanges[exName] = exchange.New(exName, exType, true, false, false, true, &amqp.Table{})
	}

	systemExchange := exchange.New(EX_DEFAULT_NAME, exchange.EX_TYPE_DIRECT, true, false, false, true, &amqp.Table{})
	vhost.AppendExchange(systemExchange)
}

func (vhost *VirtualHost) GetQueue(name string) *queue.Queue {
	return vhost.queues[name]
}

func (vhost *VirtualHost) GetExchange(name string) *exchange.Exchange {
	return vhost.exchanges[name]
}

func (vhost *VirtualHost) GetDefaultExchange() *exchange.Exchange {
	return vhost.exchanges[EX_DEFAULT_NAME]
}

func (vhost *VirtualHost) AppendExchange(ex *exchange.Exchange) {
	vhost.exLock.Lock()
	defer vhost.exLock.Unlock()
	vhost.exchanges[ex.Name] = ex
}

func (vhost *VirtualHost) AppendQueue(qe *queue.Queue) {
	vhost.quLock.Lock()
	defer vhost.quLock.Unlock()
	vhost.queues[qe.Name] = qe

	ex := vhost.GetDefaultExchange()
	bind := binding.New(qe.Name, EX_DEFAULT_NAME, qe.Name, &amqp.Table{}, false)
	ex.AppendBinding(bind)
}
