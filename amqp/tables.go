package amqp

type Table struct {
	data map[string]interface{}
}

func (table *Table) Set(key string, value interface{}) {
	table.data[key] = value
}
