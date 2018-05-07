package amqp

type Table struct {
	Data map[string]interface{}
}

func NewTable() *Table {
	return &Table{
		Data: make(map[string]interface{}),
	}
}
func (table *Table) Set(key string, value interface{}) {
	table.Data[key] = value
}
