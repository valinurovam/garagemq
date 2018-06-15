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

func (bA *Binding) Equal(bB *Binding) bool {
	return bA.Exchange == bB.Exchange &&
		bA.Queue == bB.Queue &&
		bA.RoutingKey == bB.RoutingKey
}
