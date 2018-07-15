package msgstorage

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/interfaces"
)

type MsgStorage struct {
	db           interfaces.DbStorage
	protoVersion string
}

func New(db interfaces.DbStorage, protoVersion string) *MsgStorage {
	return &MsgStorage{
		db:           db,
		protoVersion: protoVersion,
	}
}

func (storage *MsgStorage) Add(message *amqp.Message, queue string) error {
	if data, err := message.Marshal(storage.protoVersion); err == nil {
		return storage.db.Set(makeKey(message.Id, queue), data)
	} else {
		return err
	}
}

func (storage *MsgStorage) Update(message *amqp.Message, queue string) error {
	if data, err := message.Marshal(storage.protoVersion); err == nil {
		return storage.db.Set(makeKey(message.Id, queue), data)
	} else {
		return err
	}
}

func (storage *MsgStorage) Del(id uint64, queue string) error {
	return storage.db.Del(makeKey(id, queue))
}

func (storage *MsgStorage) Iterate(fn func(queue string, message *amqp.Message)) {
	storage.db.Iterate(
		func(key []byte, value []byte) {
			queueName := getQueueFromKey(string(key))
			message := &amqp.Message{}
			message.Unmarshal(value, storage.protoVersion)
			fn(queueName, message)
		},
	)
}

func (storage *MsgStorage) PurgeQueue(queue string) {
	prefix := []byte("msg." + queue)
	storage.db.Iterate(
		func(key []byte, value []byte) {
			if !bytes.HasPrefix(key, prefix) {
				return
			}
			storage.db.Del(string(key))
		},
	)
}

func (storage *MsgStorage) Close() error {
	return storage.db.Close()
}

func makeKey(id uint64, queue string) string {
	return "msg." + queue + "." + strconv.FormatInt(int64(id), 10)
}

func getQueueFromKey(key string) string {
	parts := strings.Split(key, ".")
	return parts[1]
}
