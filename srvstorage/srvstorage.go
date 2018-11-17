package srvstorage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/valinurovam/garagemq/binding"
	"github.com/valinurovam/garagemq/exchange"
	"github.com/valinurovam/garagemq/interfaces"
	"github.com/valinurovam/garagemq/queue"
)

const queuePrefix = "vhost.queue"
const exchangePrefix = "vhost.exchange"
const bindingPrefix = "vhost.binding"
const vhostPrefix = "server.vhost"

// SrvStorage implements storage for store all durable server entities
type SrvStorage struct {
	db           interfaces.DbStorage
	protoVersion string
}

// NewSrvStorage returns instance of server storage
func NewSrvStorage(db interfaces.DbStorage, protoVersion string) *SrvStorage {
	return &SrvStorage{
		db:           db,
		protoVersion: protoVersion,
	}
}

// IsFirstStart checks is storage has lastStartTime key
func (storage *SrvStorage) IsFirstStart() bool {
	// TODO Handle error
	data, _ := storage.db.Get("lastStartTime")

	if len(data) != 0 {
		return false
	}

	return true
}

// UpdateLastStart update last server start time
func (storage *SrvStorage) UpdateLastStart() error {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, time.Now().Unix())
	return storage.db.Set("lastStartTime", buf.Bytes())
}

// AddVhost add vhost into storage
func (storage *SrvStorage) AddVhost(vhost string, system bool) error {
	key := fmt.Sprintf("%s.%s", vhostPrefix, vhost)
	if system {
		return storage.db.Set(key, []byte{1})
	}

	return storage.db.Set(key, []byte{})
}

// GetVhosts returns stored virtual hosts
func (storage *SrvStorage) GetVhosts() map[string]bool {
	vhosts := make(map[string]bool)
	storage.db.Iterate(
		func(key []byte, value []byte) {
			if !bytes.HasPrefix(key, []byte(vhostPrefix)) {
				return
			}
			vhost := getVhostFromKey(string(key))
			system := bytes.Equal(value, []byte{1})
			vhosts[vhost] = system
		},
	)

	return vhosts
}

// AddBinding add binding into storage
func (storage *SrvStorage) AddBinding(vhost string, bind *binding.Binding) error {
	key := fmt.Sprintf("%s.%s.%s", bindingPrefix, vhost, bind.GetName())
	data, err := bind.Marshal(storage.protoVersion)
	if err != nil {
		return err
	}
	return storage.db.Set(key, data)
}

// DelBinding remove binding from storage
func (storage *SrvStorage) DelBinding(vhost string, bind *binding.Binding) error {
	key := fmt.Sprintf("%s.%s.%s", bindingPrefix, vhost, bind.GetName())
	return storage.db.Del(key)
}

// AddExchange add exchange into storage
func (storage *SrvStorage) AddExchange(vhost string, ex *exchange.Exchange) error {
	key := fmt.Sprintf("%s.%s.%s", exchangePrefix, vhost, ex.GetName())
	data, err := ex.Marshal(storage.protoVersion)
	if err != nil {
		return err
	}
	return storage.db.Set(key, data)
}

// DelExchange remove exchange from storage
func (storage *SrvStorage) DelExchange(vhost string, ex *exchange.Exchange) error {
	key := fmt.Sprintf("%s.%s.%s", exchangePrefix, vhost, ex.GetName())
	return storage.db.Del(key)
}

// AddQueue add queue into storage
func (storage *SrvStorage) AddQueue(vhost string, queue *queue.Queue) error {
	key := fmt.Sprintf("%s.%s.%s", queuePrefix, vhost, queue.GetName())
	data, err := queue.Marshal(storage.protoVersion)
	if err != nil {
		return err
	}
	return storage.db.Set(key, data)
}

// DelQueue remove queue from storage
func (storage *SrvStorage) DelQueue(vhost string, queue *queue.Queue) error {
	key := fmt.Sprintf("%s.%s.%s", queuePrefix, vhost, queue.GetName())
	return storage.db.Del(key)
}

// GetVhostQueues returns queue names that has given vhost
func (storage *SrvStorage) GetVhostQueues(vhost string) []*queue.Queue {
	var queues []*queue.Queue
	storage.db.Iterate(
		func(key []byte, value []byte) {
			if !bytes.HasPrefix(key, []byte(queuePrefix)) || getVhostFromKey(string(key)) != vhost {
				return
			}
			q := &queue.Queue{}
			q.Unmarshal(value, storage.protoVersion)
			queues = append(queues, q)
		},
	)

	return queues
}

// GetVhostExchanges returns exchanges that has given vhost
func (storage *SrvStorage) GetVhostExchanges(vhost string) []*exchange.Exchange {
	var exchanges []*exchange.Exchange
	storage.db.Iterate(
		func(key []byte, value []byte) {
			if !bytes.HasPrefix(key, []byte(exchangePrefix)) || getVhostFromKey(string(key)) != vhost {
				return
			}
			ex := &exchange.Exchange{}
			ex.Unmarshal(value)
			exchanges = append(exchanges, ex)
		},
	)

	return exchanges
}

// GetVhostBindings returns bindings that has given vhost
func (storage *SrvStorage) GetVhostBindings(vhost string) []*binding.Binding {
	var bindings []*binding.Binding
	storage.db.Iterate(
		func(key []byte, value []byte) {
			if !bytes.HasPrefix(key, []byte(bindingPrefix)) || getVhostFromKey(string(key)) != vhost {
				return
			}
			bind := &binding.Binding{}
			bind.Unmarshal(value, storage.protoVersion)
			bindings = append(bindings, bind)
		},
	)

	return bindings
}

func getVhostFromKey(key string) string {
	parts := strings.Split(key, ".")
	return parts[2]
}

// Close properly close storage database
func (storage *SrvStorage) Close() error {
	return storage.db.Close()
}
