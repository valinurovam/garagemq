package binding

import (
	"github.com/valinurovam/garagemq/amqp"
	"regexp"
	"strings"
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
	routingParts := strings.Split(routingKey, ".")
	for idx, routingPart := range routingParts {
		if routingPart == "#" {
			routingParts[idx] = `[^\.]+`
		} else if routingPart == "*" {
			routingParts[idx] = `.*`
		} else {
			routingParts[idx] = regexp.QuoteMeta(routingPart)
		}
	}

	pattern := "^" + strings.Join(routingParts, `\.`) + "$"

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
