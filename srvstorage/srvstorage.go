package srvstorage

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/valinurovam/garagemq/interfaces"
)

const queuePrefix = "vhost.queue"

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

func (storage *SrvStorage) AddQueue(vhost string, queue interfaces.AmqpQueue) error {
	key := fmt.Sprintf("%s.%s.%s", queuePrefix, vhost, queue.GetName())
	return storage.db.Set(key, queue.Marshal(storage.protoVersion))
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

func getVhostFromKey(key string) string {
	parts := strings.Split(key, ".")
	return parts[2]
}
