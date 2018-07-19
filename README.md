# GarageMQ

AMQP-server implemented with #golang

### Goals of this project:

- Have fun and learn a lon
- Implement RabbitMQ in Go
- Protocol compatible with RabbitMQ and standard AMQP 0-9-1.

### TODO
- [ ] Add comments
- [ ] Optimize binds
- [ ] Fix golint exceptions
- [ ] Replication
- [ ] Own backend for durable entities and persistent messages
- [ ] Migrate to message reference counting

### Backend for durable entities and persistent messages
Database backend is changeable through config `db.engine` 
```
db:
  defaultPath: db
  engine: badger
```
- Badger https://github.com/dgraph-io/badger
- BuntDB https://github.com/tidwall/buntdb

### Implemented methods
 
#### connection 

- [x] connection.start
- [x] connection.startOk 
- [ ] connection.secure
- [ ] connection.secureOk 
- [x] connection.tune
- [x] connection.tuneOk 
- [x] connection.open
- [x] connection.openOk 
- [x] connection.close
- [x] connection.closeOk 

#### channel

- [x] channel.open
- [x] channel.openOk
- [x] channel.flow
- [x] channel.flowOk
- [x] channel.close
- [x] channel.closeOk

#### exchange

- [x] exchange.declare
- [x] exchange.declareOk
- [ ] exchange.delete
- [ ] exchange.deleteOk
- [ ] exchange.bind
- [ ] exchange.unbind

#### queue

- [x] queue.declare
- [x] queue.declareOk
- [x] queue.bind
- [x] queue.bindOk
- [x] queue.unbind
- [x] queue.unbindOk
- [x] queue.purge
- [x] queue.purgeOk
- [x] queue.delete
- [x] queue.deleteOk

#### basic

Qos method implemented for standard AMQP and RabbitMQ mode. It means that by default qos applies for connection(global=true) or channel(global=false). 
RabbitMQ Qos means for channel(global=true) or each new consumer(global=false).

- [x] basic.qos
- [x] basic.qosOk
- [x] basic.consume
- [x] basic.consumeOk
- [x] basic.cancel
- [x] basic.cancelOk
- [x] basic.publish
- [x] basic.return
- [x] basic.deliver
- [x] basic.get
- [x] basic.getOk
- [x] basic.getEmpty 
- [x] basic.ack
- [x] basic.nack
- [x] basic.reject
- [ ] basic.recoverAsync
- [ ] basic.recover
- [ ] basic.recoverOk 

#### confirm

- [x] confirm.select
- [x] confirm.selectOk

#### tx

- [ ] tx.select
- [ ] tx.selectOk
- [ ] tx.commit
- [ ] tx.commitOk
- [ ] tx.rollback
- [ ] tx.rollbackOk
