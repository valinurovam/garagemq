# GarageMQ [![Build Status](https://travis-ci.org/valinurovam/garagemq.svg?branch=master)](https://travis-ci.org/valinurovam/garagemq) [![Coverage Status](https://coveralls.io/repos/github/valinurovam/garagemq/badge.svg)](https://coveralls.io/github/valinurovam/garagemq) [![Go Report Card](https://goreportcard.com/badge/github.com/valinurovam/garagemq)](https://goreportcard.com/report/github.com/valinurovam/garagemq)

AMQP-server implemented with #golang

## Goals of this project:

- Have fun and learn a lon
- Implement RabbitMQ in Go
- Protocol compatible with RabbitMQ and standard AMQP 0-9-1.

## Compatibilities
GarageMQ compatible with any AMQP or RabbitMQ clients (tested streadway/amqp and php-amqp lib)

## Building

The quick way to start GarageMQ is by using `docker` as shown below. 

```shell
go get -u github.com/valinurovam/garagemq/...
cd $GOPATH/src/github.com/valinurovam/garagemq
docker build -t garagemq .
docker run -d --name garagemq -p 5672:5672 garagemq

```
... or ...
```shell
go get -u github.com/valinurovam/garagemq/...
cd $GOPATH/src/github.com/valinurovam/garagemq
make run
```

## Performance

Performance tests with load testing tool https://github.com/rabbitmq/rabbitmq-perf-test on machine:
``` 
MacBook Pro (15-inch, 2016)
Processor 2,6 GHz Intel Core i7
Memory 16 GB 2133 MHz LPDDR3
```

```shell
./bin/runjava com.rabbitmq.perf.PerfTest --exchange test -uri amqp://guest:guest@localhost:5672 --queue test --consumers 10 --producers 5 --qos 100 -flag persistent
...
...
id: test-145413-796, sending rate avg: 75123 msg/s
id: test-145413-796, receiving rate avg: 70337 msg/s
```

## Internals

### Backend for durable entities and persistent messages
Database backend is changeable through config `db.engine` 
```
db:
  defaultPath: db
  engine: badger
```
```
db:
  defaultPath: db
  engine: buntdb
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

### Admin server

The administration server is available at standard `:15672` port and is `read only mode` at the moment. Main page above, and [more screenshots](/readme) at /readme folder

![Overview](readme/overview.jpg)

## TODO
- [ ] Add more comments
- [ ] Optimize binds
- [ ] Replication
- [ ] Own backend for durable entities and persistent messages
- [ ] Migrate to message reference counting