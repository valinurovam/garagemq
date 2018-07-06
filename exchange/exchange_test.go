package exchange

import (
	"testing"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/binding"
)

func getTestEx() *Exchange {
	return &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_DIRECT,
		durable:    false,
		autoDelete: false,
		internal:   false,
		system:     false,
	}
}

func TestNew(t *testing.T) {
	e := getTestEx()

	et := New(e.Name, e.ExType, e.durable, e.autoDelete, e.internal, e.system, &amqp.Table{})
	if err := e.EqualWithErr(et); err != nil {
		t.Fatal(err)
	}
}

func TestExchange_AppendBinding(t *testing.T) {
	e := getTestEx()
	b := binding.New("test", "test", "test", &amqp.Table{}, false)
	e.AppendBinding(b)
	e.AppendBinding(b)
	l := len(e.GetBindings())
	if l != 1 {
		t.Fatalf("Expected 1 binding in exchange, %d given", l)
	}

	found := false
	for _, b := range e.GetBindings() {
		if b.Equal(b) {
			found = true
		}
	}

	if !found {
		t.Fatal("Appended binding not found")
	}
}

func TestExchange_RemoveBinding(t *testing.T) {
	e := getTestEx()
	b := binding.New("test", "test", "test", &amqp.Table{}, false)
	e.AppendBinding(b)
	e.RemoveBinding(b)

	found := false
	for _, b := range e.GetBindings() {
		if b.Equal(b) {
			found = true
		}
	}

	if found {
		t.Fatal("Appended binding found after remove")
	}
}

func TestExchange_GetBindings(t *testing.T) {
	e := getTestEx()
	b := binding.New("test", "test", "test", &amqp.Table{}, false)
	e.AppendBinding(b)
	l := len(e.GetBindings())
	if l != 1 {
		t.Fatalf("Expected 1 binding in exchange, %d given", l)
	}
}

func TestExchange_RemoveQueueBindings(t *testing.T) {
	e := getTestEx()

	e.AppendBinding(binding.New("test", "test", "test1", &amqp.Table{}, false))
	e.AppendBinding(binding.New("test2", "test", "test2", &amqp.Table{}, false))
	e.AppendBinding(binding.New("test", "test", "test3", &amqp.Table{}, false))
	e.RemoveQueueBindings("test")

	if len(e.GetBindings()) != 1 {
		t.Fatal("Found bindings for queue after RemoveQueueBindings")
	}

	found := false
	for _, b := range e.GetBindings() {
		if b.GetQueue() == "test" {
			found = true
		}
	}

	if found {
		t.Fatal("Found bindings for queue after RemoveQueueBindings")
	}
}

func TestExchange_GetMatchedQueues_Direct(t *testing.T) {
	e := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_DIRECT,
		durable:    false,
		autoDelete: false,
		internal:   false,
		system:     false,
	}
	e.AppendBinding(binding.New("test_q", "test", "test_rk", &amqp.Table{}, false))

	matched := e.GetMatchedQueues(&amqp.Message{
		Exchange:   "test",
		RoutingKey: "test_rk",
	})

	if found, ok := matched["test_q"]; !(ok && found) {
		t.Fatal("Direct match not found")
	}
}

func TestExchange_GetMatchedQueues_Fanout(t *testing.T) {
	e := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_FANOUT,
		durable:    false,
		autoDelete: false,
		internal:   false,
		system:     false,
	}
	e.AppendBinding(binding.New("test_q1", "test", "test_rk", &amqp.Table{}, false))
	e.AppendBinding(binding.New("test_q2", "test", "test_rk", &amqp.Table{}, false))

	matched := e.GetMatchedQueues(&amqp.Message{
		Exchange: "test",
	})

	if len(matched) != 2 {
		t.Fatal("Fanout match not found")
	}
}

// For topic exchange test much simple, cause topic bidnings full tested in bindings test
// @see binding/binding_test.go:83
func TestExchange_GetMatchedQueues_Topic(t *testing.T) {

	e := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_TOPIC,
		durable:    false,
		autoDelete: false,
		internal:   false,
		system:     false,
	}
	e.AppendBinding(binding.New("test_q1", "test", "test_rk.#", &amqp.Table{}, true))
	e.AppendBinding(binding.New("test_q2", "test", "test_rk", &amqp.Table{}, true))
	e.AppendBinding(binding.New("test_q3", "test", "test", &amqp.Table{}, true))

	matched := e.GetMatchedQueues(&amqp.Message{
		Exchange:   "test",
		RoutingKey: "test_rk",
	})

	if len(matched) != 2 {
		t.Fatal("Topic match not found")
	}

	matched = e.GetMatchedQueues(&amqp.Message{
		Exchange:   "test",
		RoutingKey: "test",
	})

	if len(matched) != 1 {
		t.Fatal("Topic match not found")
	}

	if found, ok := matched["test_q3"]; !(ok && found) {
		t.Fatal("Topic match not found")
	}

	matched = e.GetMatchedQueues(&amqp.Message{
		Exchange:   "test",
		RoutingKey: "test_rk.test",
	})

	if len(matched) != 1 {
		t.Fatal("Topic match not found")
	}

	if found, ok := matched["test_q1"]; !(ok && found) {
		t.Fatal("Topic match not found")
	}
}

func TestExchange_EqualWithErr_Success(t *testing.T) {
	e1 := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_TOPIC,
		durable:    false,
		autoDelete: false,
		internal:   false,
		system:     false,
	}

	e2 := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_TOPIC,
		durable:    false,
		autoDelete: false,
		internal:   false,
		system:     false,
	}

	if err := e1.EqualWithErr(e2); err != nil {
		t.Fatal(err)
	}
}

func TestExchange_EqualWithErr_Failed_ExType(t *testing.T) {
	e1 := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_TOPIC,
		durable:    false,
		autoDelete: false,
		internal:   false,
		system:     false,
	}

	e2 := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_DIRECT,
		durable:    false,
		autoDelete: false,
		internal:   false,
		system:     false,
	}

	if err := e1.EqualWithErr(e2); err == nil {
		t.Fatal("Expected inequivalent error")
	}
}

func TestExchange_EqualWithErr_Failed_Durable(t *testing.T) {
	e1 := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_DIRECT,
		durable:    false,
		autoDelete: false,
		internal:   false,
		system:     false,
	}

	e2 := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_DIRECT,
		durable:    true,
		autoDelete: false,
		internal:   false,
		system:     false,
	}

	if err := e1.EqualWithErr(e2); err == nil {
		t.Fatal("Expected inequivalent error")
	}
}

func TestExchange_EqualWithErr_Failed_AutoDelete(t *testing.T) {
	e1 := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_DIRECT,
		durable:    false,
		autoDelete: false,
		internal:   false,
		system:     false,
	}

	e2 := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_DIRECT,
		durable:    false,
		autoDelete: true,
		internal:   false,
		system:     false,
	}

	if err := e1.EqualWithErr(e2); err == nil {
		t.Fatal("Expected inequivalent error")
	}
}

func TestExchange_EqualWithErr_Failed_Internal(t *testing.T) {
	e1 := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_DIRECT,
		durable:    false,
		autoDelete: false,
		internal:   false,
		system:     false,
	}

	e2 := &Exchange{
		Name:       "test",
		ExType:     EX_TYPE_DIRECT,
		durable:    false,
		autoDelete: false,
		internal:   true,
		system:     false,
	}

	if err := e1.EqualWithErr(e2); err == nil {
		t.Fatal("Expected inequivalent error")
	}
}

func TestGetExchangeTypeAlias(t *testing.T) {
	var actual string
	var err error
	for id, expected := range exchangeTypeIdAliasMap {
		if actual, err = GetExchangeTypeAlias(id); err != nil {
			t.Fatal(err)
		}
		if expected != actual {
			t.Fatalf("Expected '%s' for id '%d', actual '%s'", expected, id, actual)
		}
	}

	if _, err = GetExchangeTypeAlias(10); err == nil {
		t.Fatal("Expected 'Undefined exchange type' error")
	}
}

func TestGetExchangeTypeId(t *testing.T) {
	var actual int
	var err error
	for alias, expected := range exchangeTypeAliasIdMap {
		if actual, err = GetExchangeTypeId(alias); err != nil {
			t.Fatal(err)
		}
		if expected != actual {
			t.Fatalf("Expected '%d' for alias '%s', actual '%d'", expected, alias, actual)
		}
	}

	if _, err = GetExchangeTypeId("test"); err == nil {
		t.Fatal("Expected 'Undefined exchange alias' error")
	}
}
