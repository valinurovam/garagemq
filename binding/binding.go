package binding

import (
	"bytes"
	"regexp"
	"strings"

	"github.com/valinurovam/garagemq/amqp"
)

type Binding struct {
	Queue      string
	Exchange   string
	RoutingKey string
	Arguments  *amqp.Table
	regexp     *regexp.Regexp
	topic      bool
}

func New(queue string, exchange string, routingKey string, arguments *amqp.Table, topic bool) *Binding {
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
		if (strings.HasSuffix(routingKey, "\\.#")) {
			continue
		}
		routingKey = routingKey + `(.*\.?)+`
	}
	routingKey = strings.Replace(routingKey, "\\.#\\.", `(.*\.?)+`, -1)
	routingKey = strings.Replace(routingKey, "#", `(.*\.?)+`, -1)
	pattern := "^" + routingKey + "$"

	return regexp.Compile(pattern)
}

func (b *Binding) MatchDirect(exchange string, routingKey string) bool {
	return b.Exchange == exchange && b.RoutingKey == routingKey
}

func (b *Binding) MatchFanout(exchange string) bool {
	return b.Exchange == exchange
}

func (b *Binding) MatchTopic(exchange string, routingKey string) bool {
	return b.Exchange == exchange && b.regexp.MatchString(routingKey)
}

func (b *Binding) GetExchange() string {
	return b.Exchange
}

func (b *Binding) GetRoutingKey() string {
	return b.RoutingKey
}

func (b *Binding) GetQueue() string {
	return b.Queue
}

func (bA *Binding) Equal(bB *Binding) bool {
	return bA.Exchange == bB.GetExchange() &&
		bA.Queue == bB.GetQueue() &&
		bA.RoutingKey == bB.GetRoutingKey()
}

func (b *Binding) GetName() string {
	return strings.Join(
		[]string{b.Queue, b.Exchange, b.RoutingKey},
		"_",
	)
}

func (b *Binding) Marshal() []byte {
	buf := bytes.NewBuffer(make([]byte, 0))
	amqp.WriteShortstr(buf, b.Queue)
	amqp.WriteShortstr(buf, b.Exchange)
	amqp.WriteShortstr(buf, b.RoutingKey)
	var topic byte = 0
	if b.topic {
		topic = 1
	}
	amqp.WriteOctet(buf, topic)
	return buf.Bytes()
}

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
