package binding

import (
	"bytes"
	"regexp"
	"strings"

	"github.com/valinurovam/garagemq/amqp"
)

// Binding represents AMQP-binding
type Binding struct {
	Queue      string
	Exchange   string
	RoutingKey string
	Arguments  *amqp.Table
	regexp     *regexp.Regexp
	topic      bool
}

// NewBinding returns new instance of Binding
func NewBinding(queue string, exchange string, routingKey string, arguments *amqp.Table, topic bool) *Binding {
	binding := &Binding{
		Queue:      queue,
		Exchange:   exchange,
		RoutingKey: routingKey,
		Arguments:  arguments,
		topic:      topic,
	}

	if topic {
		binding.regexp, _ = buildRegexp(routingKey)
	}

	return binding
}

// @todo may be better will be trie or dfa than regexp
// @see http://www.rabbitmq.com/blog/2010/09/14/very-fast-and-scalable-topic-routing-part-1/
// @see http://www.rabbitmq.com/blog/2011/03/28/very-fast-and-scalable-topic-routing-part-2/
//
// buildRegexp generate regexp from topic-match string
func buildRegexp(routingKey string) (*regexp.Regexp, error) {
	routingKey = strings.TrimSpace(routingKey)
	routingParts := strings.Split(routingKey, ".")

	for idx, routingPart := range routingParts {
		if routingPart == "*" {
			routingParts[idx] = "*"
		} else if routingPart == "#" {
			routingParts[idx] = "#"
		} else {
			routingParts[idx] = regexp.QuoteMeta(routingPart)
		}
	}

	routingKey = strings.Join(routingParts, "\\.")
	routingKey = strings.Replace(routingKey, "*", `([^\.]+)`, -1)

	for strings.HasPrefix(routingKey, "#\\.") {
		routingKey = strings.TrimPrefix(routingKey, "#\\.")
		if strings.HasPrefix(routingKey, "#\\.") {
			continue
		}
		routingKey = `(.*\.?)+` + routingKey
	}

	for strings.HasSuffix(routingKey, "\\.#") {
		routingKey = strings.TrimSuffix(routingKey, "\\.#")
		if strings.HasSuffix(routingKey, "\\.#") {
			continue
		}
		routingKey = routingKey + `(.*\.?)+`
	}
	routingKey = strings.Replace(routingKey, "\\.#\\.", `(.*\.?)+`, -1)
	routingKey = strings.Replace(routingKey, "#", `(.*\.?)+`, -1)
	pattern := "^" + routingKey + "$"

	return regexp.Compile(pattern)
}

// MatchDirect check is message can be routed from direct-exchange to queue
// with compare exchange and routing key
func (b *Binding) MatchDirect(exchange string, routingKey string) bool {
	return b.Exchange == exchange && b.RoutingKey == routingKey
}

// MatchFanout check is message can be routed from fanout-exchange to queue
// with compare only exchange
func (b *Binding) MatchFanout(exchange string) bool {
	return b.Exchange == exchange
}

// MatchTopic check is message can be routed from topic-exchange to queue
// with compare exchange and match topic-pattern with routing key
func (b *Binding) MatchTopic(exchange string, routingKey string) bool {
	return b.Exchange == exchange && b.regexp.MatchString(routingKey)
}

// GetExchange returns binding's exchange
func (b *Binding) GetExchange() string {
	return b.Exchange
}

// GetRoutingKey returns binding's routing key
func (b *Binding) GetRoutingKey() string {
	return b.RoutingKey
}

// GetQueue returns binding's queue
func (b *Binding) GetQueue() string {
	return b.Queue
}

// Equal returns is given binding equal to current
// with compare exchange, routing key and queue
func (b *Binding) Equal(bind *Binding) bool {
	return b.Exchange == bind.GetExchange() &&
		b.Queue == bind.GetQueue() &&
		b.RoutingKey == bind.GetRoutingKey()
}

// GetName generate binding name by concatenating its params
func (b *Binding) GetName() string {
	return strings.Join(
		[]string{b.Queue, b.Exchange, b.RoutingKey},
		"_",
	)
}

// Marshal returns raw representation of binding to store into storage
func (b *Binding) Marshal() []byte {
	buf := bytes.NewBuffer(make([]byte, 0))
	amqp.WriteShortstr(buf, b.Queue)
	amqp.WriteShortstr(buf, b.Exchange)
	amqp.WriteShortstr(buf, b.RoutingKey)
	var topic byte
	if b.topic {
		topic = 1
	}
	amqp.WriteOctet(buf, topic)
	return buf.Bytes()
}

// Unmarshal returns binding from storage raw bytes data
func (b *Binding) Unmarshal(data []byte) {
	buf := bytes.NewReader(data)
	b.Queue, _ = amqp.ReadShortstr(buf)
	b.Exchange, _ = amqp.ReadShortstr(buf)
	b.RoutingKey, _ = amqp.ReadShortstr(buf)
	topic, _ := amqp.ReadOctet(buf)
	b.topic = topic == 1

	if b.topic {
		b.regexp, _ = buildRegexp(b.RoutingKey)
	}
}
