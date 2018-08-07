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

### Implemented all AMQP methods, except:
 
#### connection 

- [ ] connection.secure
- [ ] connection.secureOk 

#### exchange

- [ ] exchange.delete
- [ ] exchange.deleteOk
- [ ] exchange.bind
- [ ] exchange.unbind

#### basic

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

### QOS

`basic.qos` method implemented for standard AMQP and RabbitMQ mode. It means that by default qos applies for connection(global=true) or channel(global=false). 
RabbitMQ Qos means for channel(global=true) or each new consumer(global=false).