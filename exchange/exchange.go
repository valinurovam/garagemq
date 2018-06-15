package exchange

import (
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/binding"
	"errors"
	"fmt"
	"sync"
)

const (
	EX_TYPE_DIRECT  = iota + 1
	EX_TYPE_FANOUT
	EX_TYPE_TOPIC
	EX_TYPE_HEADERS
)

var exchangeTypeIdAliasMap = map[int]string{
	EX_TYPE_DIRECT:  "direct",
	EX_TYPE_FANOUT:  "fanout",
	EX_TYPE_TOPIC:   "topic",
	EX_TYPE_HEADERS: "headers",
}

var exchangeTypeAliasIdMap = map[string]int{
	"direct":  EX_TYPE_DIRECT,
	"fanout":  EX_TYPE_FANOUT,
	"topic":   EX_TYPE_TOPIC,
	"headers": EX_TYPE_HEADERS,
}

type Exchange struct {
	Name       string
	ExType     int
	durable    bool
	autoDelete bool
	internal   bool
	system     bool
	arguments  *amqp.Table
	bindLock   sync.Mutex
	bindings   []*binding.Binding
}

func GetExchangeTypeAlias(id int) (alias string, err error) {
	if alias, ok := exchangeTypeIdAliasMap[id]; ok {
		return alias, nil
	}
	return "", errors.New(fmt.Sprintf("Undefined exchange type '%d'", id))
}

func GetExchangeTypeId(alias string) (id int, err error) {
	if id, ok := exchangeTypeAliasIdMap[alias]; ok {
		return id, nil
	}
	return 0, errors.New(fmt.Sprintf("Undefined exchange alias '%s'", alias))
}

func New(name string, exType int, durable bool, autoDelete bool, internal bool, system bool, arguments *amqp.Table) *Exchange {
	return &Exchange{
		Name:       name,
		ExType:     exType,
		durable:    durable,
		autoDelete: autoDelete,
		internal:   internal,
		system:     system,
	}
}

func (ex *Exchange) AppendBinding(newBind *binding.Binding) {
	ex.bindLock.Lock()
	defer ex.bindLock.Unlock()
	for _, bind := range ex.bindings {
		if bind.Equal(newBind) {
			return
		}
	}
	ex.bindings = append(ex.bindings, newBind)
}

func (ex *Exchange) RemoveBiding(rmBind *binding.Binding) {
	ex.bindLock.Lock()
	defer ex.bindLock.Unlock()
	for i, bind := range ex.bindings {
		if bind.Equal(rmBind) {
			ex.bindings = append(ex.bindings[:i], ex.bindings[i+1:]...)
			return
		}
	}
}

func (ex *Exchange) GetMatchedQueues(message *amqp.Message) (matchedQueues map[string]bool) {
	matchedQueues = make(map[string]bool)
	switch ex.ExType {
	case EX_TYPE_DIRECT:
		for _, bind := range ex.bindings {
			if bind.MatchDirect(message.Exchange, message.RoutingKey) {
				matchedQueues[bind.Queue] = true
				return
			}
		}
	case EX_TYPE_FANOUT:
		for _, bind := range ex.bindings {
			if bind.MatchFanout(message.Exchange) {
				matchedQueues[bind.Queue] = true
			}
		}
	case EX_TYPE_TOPIC:
		for _, bind := range ex.bindings {
			if bind.MatchTopic(message.Exchange, message.RoutingKey) {
				matchedQueues[bind.Queue] = true
			}
		}
	}
	return
}

func (exA *Exchange) EqualWithErr(exB *Exchange) error {
	errTemplate := "inequivalent arg '%s' for exchange '%s': received '%s' but current is '%s'"
	if exA.ExType != exB.ExType {
		aliasA, _ := GetExchangeTypeAlias(exA.ExType)
		aliasB, _ := GetExchangeTypeAlias(exB.ExType)
		return errors.New(fmt.Sprintf(
			errTemplate,
			"type",
			exA.Name,
			aliasB,
			aliasA,
		))
	}
	if exA.durable != exB.durable {
		return errors.New(fmt.Sprintf(errTemplate, "durable", exA.Name, exB.durable, exA.durable))
	}
	if exA.autoDelete != exB.autoDelete {
		return errors.New(fmt.Sprintf(errTemplate, "autoDelete", exA.Name, exB.autoDelete, exA.autoDelete))
	}
	if exA.internal != exB.internal {
		return errors.New(fmt.Sprintf(errTemplate, "internal", exA.Name, exB.internal, exA.internal))
	}
	return nil
}
