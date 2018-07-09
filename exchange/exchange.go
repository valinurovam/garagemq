package exchange

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/interfaces"
)

const (
	EX_TYPE_DIRECT  = iota + 1
	EX_TYPE_FANOUT
	EX_TYPE_TOPIC
	EX_TYPE_HEADERS
)

var exchangeTypeIdAliasMap = map[byte]string{
	EX_TYPE_DIRECT:  "direct",
	EX_TYPE_FANOUT:  "fanout",
	EX_TYPE_TOPIC:   "topic",
	EX_TYPE_HEADERS: "headers",
}

var exchangeTypeAliasIdMap = map[string]byte{
	"direct":  EX_TYPE_DIRECT,
	"fanout":  EX_TYPE_FANOUT,
	"topic":   EX_TYPE_TOPIC,
	"headers": EX_TYPE_HEADERS,
}

type Exchange struct {
	Name       string
	exType     byte
	durable    bool
	autoDelete bool
	internal   bool
	system     bool
	arguments  *amqp.Table
	bindLock   sync.Mutex
	bindings   []interfaces.Binding
}

func GetExchangeTypeAlias(id byte) (alias string, err error) {
	if alias, ok := exchangeTypeIdAliasMap[id]; ok {
		return alias, nil
	}
	return "", errors.New(fmt.Sprintf("Undefined exchange type '%d'", id))
}

func GetExchangeTypeId(alias string) (id byte, err error) {
	if id, ok := exchangeTypeAliasIdMap[alias]; ok {
		return id, nil
	}
	return 0, errors.New(fmt.Sprintf("Undefined exchange alias '%s'", alias))
}

func New(name string, exType byte, durable bool, autoDelete bool, internal bool, system bool) *Exchange {
	return &Exchange{
		Name:       name,
		exType:     exType,
		durable:    durable,
		autoDelete: autoDelete,
		internal:   internal,
		system:     system,
	}
}

func (ex *Exchange) AppendBinding(newBind interfaces.Binding) {
	ex.bindLock.Lock()
	defer ex.bindLock.Unlock()
	for _, bind := range ex.bindings {
		if bind.Equal(newBind) {
			return
		}
	}
	ex.bindings = append(ex.bindings, newBind)
}

func (ex *Exchange) RemoveBinding(rmBind interfaces.Binding) {
	ex.bindLock.Lock()
	defer ex.bindLock.Unlock()
	for i, bind := range ex.bindings {
		if bind.Equal(rmBind) {
			ex.bindings = append(ex.bindings[:i], ex.bindings[i+1:]...)
			return
		}
	}
}

func (ex *Exchange) RemoveQueueBindings(queueName string) {
	var newBindings []interfaces.Binding
	ex.bindLock.Lock()
	defer ex.bindLock.Unlock()
	for _, bind := range ex.bindings {
		if bind.GetQueue() != queueName {
			newBindings = append(newBindings, bind)
		}
	}

	ex.bindings = newBindings
}

func (ex *Exchange) GetMatchedQueues(message *amqp.Message) (matchedQueues map[string]bool) {
	matchedQueues = make(map[string]bool)
	switch ex.exType {
	case EX_TYPE_DIRECT:
		for _, bind := range ex.bindings {
			if bind.MatchDirect(message.Exchange, message.RoutingKey) {
				matchedQueues[bind.GetQueue()] = true
				return
			}
		}
	case EX_TYPE_FANOUT:
		for _, bind := range ex.bindings {
			if bind.MatchFanout(message.Exchange) {
				matchedQueues[bind.GetQueue()] = true
			}
		}
	case EX_TYPE_TOPIC:
		for _, bind := range ex.bindings {
			if bind.MatchTopic(message.Exchange, message.RoutingKey) {
				matchedQueues[bind.GetQueue()] = true
			}
		}
	}
	return
}

func (exA *Exchange) EqualWithErr(exB interfaces.Exchange) error {
	errTemplate := "inequivalent arg '%s' for exchange '%s': received '%s' but current is '%s'"
	if exA.exType != exB.ExType() {
		aliasA, _ := GetExchangeTypeAlias(exA.exType)
		aliasB, _ := GetExchangeTypeAlias(exB.ExType())
		return errors.New(fmt.Sprintf(
			errTemplate,
			"type",
			exA.Name,
			aliasB,
			aliasA,
		))
	}
	if exA.durable != exB.IsDurable() {
		return errors.New(fmt.Sprintf(errTemplate, "durable", exA.Name, exB.IsDurable(), exA.durable))
	}
	if exA.autoDelete != exB.IsAutoDelete() {
		return errors.New(fmt.Sprintf(errTemplate, "autoDelete", exA.Name, exB.IsAutoDelete(), exA.autoDelete))
	}
	if exA.internal != exB.IsInternal() {
		return errors.New(fmt.Sprintf(errTemplate, "internal", exA.Name, exB.IsInternal(), exA.internal))
	}
	return nil
}

func (ex *Exchange) GetBindings() []interfaces.Binding {
	ex.bindLock.Lock()
	defer ex.bindLock.Unlock()
	return ex.bindings
}

func (ex *Exchange) IsDurable() bool {
	return ex.durable
}

func (ex *Exchange) IsSystem() bool {
	return ex.system
}

func (ex *Exchange) IsAutoDelete() bool {
	return ex.autoDelete
}

func (ex *Exchange) IsInternal() bool {
	return ex.internal
}

func (ex *Exchange) Marshal(protoVersion string) []byte {
	buf := bytes.NewBuffer(make([]byte, 0))
	amqp.WriteShortstr(buf, ex.Name)
	amqp.WriteOctet(buf, ex.exType)
	return buf.Bytes()
}

func (ex *Exchange) Unmarshal(data []byte) {
	buf := bytes.NewReader(data)
	ex.Name, _ = amqp.ReadShortstr(buf)
	ex.exType, _ = amqp.ReadOctet(buf)
	ex.durable = true
}

func (ex *Exchange) GetName() string {
	return ex.Name
}

func (ex *Exchange) ExType() byte {
	return ex.exType
}
