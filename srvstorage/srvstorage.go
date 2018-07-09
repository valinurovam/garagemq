package srvstorage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/valinurovam/garagemq/exchange"
	"github.com/valinurovam/garagemq/interfaces"
)

const queuePrefix = "vhost.queue"
const exchangePrefix = "vhost.exchange"
const vhostPrefix = "server.vhost"

type SrvStorage struct {
	db           interfaces.DbStorage
	protoVersion string
}

func New(db interfaces.DbStorage, protoVersion string) *SrvStorage {
	return &SrvStorage{
		db:           db,
		protoVersion: protoVersion,
	}
}

func (storage *SrvStorage) IsFirstStart() bool {
	// TODO Handle error
	data, _ := storage.db.Get("lastStartTime")

	if len(data) != 0 {
		return false
	}

	return true
}

func (storage *SrvStorage) UpdateLastStart() error {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, time.Now().Unix())
	return storage.db.Set("lastStartTime", buf.Bytes())
}

func (storage *SrvStorage) AddVhost(vhost string, system bool) error {
	key := fmt.Sprintf("%s.%s", vhostPrefix, vhost)
	if system {
		return storage.db.Set(key, []byte{1})
	} else {
		return storage.db.Set(key, []byte{})
	}
}

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

func (storage *SrvStorage) AddExchange(vhost string, ex interfaces.Exchange) error {
	key := fmt.Sprintf("%s.%s.%s", exchangePrefix, vhost, ex.GetName())
	return storage.db.Set(key, ex.Marshal(storage.protoVersion))
}

func (storage *SrvStorage) DelExchange(vhost string, ex interfaces.Exchange) error {
	key := fmt.Sprintf("%s.%s.%s", exchangePrefix, vhost, ex.GetName())
	return storage.db.Del(key)
}

func (storage *SrvStorage) AddQueue(vhost string, queue interfaces.AmqpQueue) error {
	key := fmt.Sprintf("%s.%s.%s", queuePrefix, vhost, queue.GetName())
	return storage.db.Set(key, queue.Marshal(storage.protoVersion))
}

func (storage *SrvStorage) DelQueue(vhost string, queue interfaces.AmqpQueue) error {
	key := fmt.Sprintf("%s.%s.%s", queuePrefix, vhost, queue.GetName())
	return storage.db.Del(key)
}

func (storage *SrvStorage) GetVhostQueues(vhost string) []string {
	queueNames := []string{}
	storage.db.Iterate(
		func(key []byte, value []byte) {
			if !bytes.HasPrefix(key, []byte(queuePrefix)) || getVhostFromKey(string(key)) != vhost {
				return
			}
			queueNames = append(queueNames, string(value))

		},
	)

	return queueNames
}

func (storage *SrvStorage) GetVhostExchanges(vhost string) []interfaces.Exchange {
	exchanges := []interfaces.Exchange{}
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

func getVhostFromKey(key string) string {
	parts := strings.Split(key, ".")
	return parts[2]
}
