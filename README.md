## AMQP-server implemented with #golang

### Implemented versions 
- AMQP - 0.0.9.1
- AMQP - RabbitMQ

### Backend for durable entities
Backend is changeable through config `db.engine` 
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
- [ ] channel.flowOk
- [x] channel.close
- [x] channel.closeOk

#### exchange

- [x] exchange.declare
- [x] exchange.declareOk
- [ ] exchange.delete
- [ ] exchange.deleteOk

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

Qos method implemented only for standard AMQP mode. It means that qos applies for connection(global=true) or channel(global=false). 
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
- [ ] basic.get
- [ ] basic.getOk
- [ ] basic.getEmpty 
- [x] basic.ack
- [ ] basic.reject
- [ ] basic.recoverAsync
- [ ] basic.recover
- [ ] basic.recoverOk 
 
#### tx

- [ ] tx.select
- [ ] tx.selectOk
- [ ] tx.commit
- [ ] tx.commitOk
- [ ] tx.rollback
- [ ] tx.rollbackOk
