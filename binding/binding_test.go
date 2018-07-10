package binding_test

import (
	"sort"
	"testing"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/binding"
)

func bindingsProviderData(topic bool) []*binding.Binding {
	bindData := map[string]string{
		"t1":  "a.b.c",
		"t2":  "a.*.c",
		"t3":  "a.#.b",
		"t4":  "a.b.b.c",
		"t5":  "#",
		"t6":  "#.#",
		"t7":  "#.b",
		"t8":  "*.*",
		"t9":  "a.*",
		"t10": "*.b.c",
		"t11": "a.#",
		"t12": "a.#.#",
		"t13": "b.b.c",
		"t14": "a.b.b",
		"t15": "a.b",
		"t16": "b.c",
		"t17": "",
		"t18": "*.*.*",
		"t19": "vodka.martini",
		"t20": "a.b.c",
		"t21": "*.#",
		"t22": "#.*.#",
		"t23": "*.#.#",
		"t24": "#.#.#",
		"t25": "*",
		"t26": "#.b.#",
	}

	result := []*binding.Binding{}

	for queue, key := range bindData {
		result = append(result, binding.New(queue, "", key, &amqp.Table{}, topic))
	}

	return result
}

func matchesProviderDataDirect() map[string][]string {
	return map[string][]string{
		"a.b.c":               {"t1", "t20"},
		"a.b":                 {"t15"},
		"a.b.b":               {"t14"},
		"":                    {"t17"},
		"b.c.c":               {},
		"a.a.a.a.a":           {},
		"vodka.gin":           {},
		"vodka.martini":       {"t19"},
		"b.b.c":               {"t13"},
		"nothing.here.at.all": {},
		"oneword":             {},
	}
}

func matchesProviderDataTopic() map[string][]string {
	return map[string][]string{
		"a.b.c":               {"t1", "t2", "t5", "t6", "t10", "t11", "t12", "t18", "t20", "t21", "t22", "t23", "t24", "t26"},
		"a.b":                 {"t3", "t5", "t6", "t7", "t8", "t9", "t11", "t12", "t15", "t21", "t22", "t23", "t24", "t26"},
		"a.b.b":               {"t3", "t5", "t6", "t7", "t11", "t12", "t14", "t18", "t21", "t22", "t23", "t24", "t26"},
		"":                    {"t5", "t6", "t17", "t24"},
		"b.c.c":               {"t5", "t6", "t18", "t21", "t22", "t23", "t24", "t26"},
		"a.a.a.a.a":           {"t5", "t6", "t11", "t12", "t21", "t22", "t23", "t24"},
		"vodka.gin":           {"t5", "t6", "t8", "t21", "t22", "t23", "t24"},
		"vodka.martini":       {"t5", "t6", "t8", "t19", "t21", "t22", "t23", "t24"},
		"b.b.c":               {"t5", "t6", "t10", "t13", "t18", "t21", "t22", "t23", "t24", "t26"},
		"nothing.here.at.all": {"t5", "t6", "t21", "t22", "t23", "t24"},
		"oneword":             {"t5", "t6", "t21", "t22", "t23", "t24", "t25"},
	}
}

func TestBinding_MatchTopic(t *testing.T) {
	bindings := bindingsProviderData(true)
	matchesExpected := matchesProviderDataTopic()
	for key, matches := range matchesExpected {
		bindMatches := []string{}
		for _, bind := range bindings {
			if bind.MatchTopic("", key) {
				bindMatches = append(bindMatches, bind.GetQueue())
			}
		}
		if !testEq(matches, bindMatches) {
			t.Fatalf("Error on matching key '%s'", key)
		}
	}
}

func TestBinding_MatchDirect(t *testing.T) {
	bindings := bindingsProviderData(false)
	matchesExpected := matchesProviderDataDirect()
	for key, matches := range matchesExpected {
		bindMatches := []string{}
		for _, bind := range bindings {
			if bind.MatchDirect("", key) {
				bindMatches = append(bindMatches, bind.GetQueue())
			}
		}
		if !testEq(matches, bindMatches) {
			t.Fatalf("Error on matching key '%s'", key)
		}
	}
}

func TestBinding_MatchFanout(t *testing.T) {
	bindings := bindingsProviderData(false)
	matched := true
	for _, bind := range bindings {
		if !bind.MatchFanout("") {
			// all should be matched
			matched = false
		}
	}
	if !matched {
		t.Fatalf("Error on matching fanout binding")
	}
}

func TestBinding_Equal(t *testing.T) {
	b1 := binding.New("test_q", "test_ex", "test_key", &amqp.Table{}, true)
	b2 := binding.New("test_q", "test_ex", "test_key", &amqp.Table{}, true)

	if !b1.Equal(b2) {
		t.Fatalf("Excpected equal bindings")
	}

	b1 = binding.New("test_q1", "test_ex", "test_key", &amqp.Table{}, true)
	b2 = binding.New("test_q", "test_ex", "test_key", &amqp.Table{}, true)

	if b1.Equal(b2) {
		t.Fatalf("Excpected not equal bindings")
	}

	b1 = binding.New("test_q", "test_ex2", "test_key", &amqp.Table{}, true)
	b2 = binding.New("test_q", "test_ex", "test_key", &amqp.Table{}, true)

	if b1.Equal(b2) {
		t.Fatalf("Excpected not equal bindings")
	}

	b1 = binding.New("test_q", "test_ex", "test_key3", &amqp.Table{}, true)
	b2 = binding.New("test_q", "test_ex", "test_key", &amqp.Table{}, true)

	if b1.Equal(b2) {
		t.Fatalf("Excpected not equal bindings")
	}
}

func testEq(a, b []string) bool {
	sort.Strings(a)
	sort.Strings(b)
	if a == nil && b == nil {
		return true;
	}

	if a == nil || b == nil {
		return false;
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
