# GarageMQ [![Build Status](https://github.com/valinurovam/garagemq/actions/workflows/build.yml/badge.svg)](https://github.com/valinurovam/garagemq/actions) [![Coverage Status](https://coveralls.io/repos/github/valinurovam/garagemq/badge.svg)](https://coveralls.io/github/valinurovam/garagemq) [![Go Report Card](https://goreportcard.com/badge/github.com/valinurovam/garagemq)](https://goreportcard.com/report/github.com/valinurovam/garagemq)

GarageMQ is a message broker that implement the Advanced Message Queuing Protocol (AMQP). Compatible with any AMQP or RabbitMQ clients (tested streadway/amqp and php-amqp lib)

#### Table of Contents
- [Goals of this project](#goals-of-this-project)
- [Demo](#demo)
- [Installation and Building](#installation-and-building)
  - [Docker](#docker)
  - [Go get](#go-get)
  - [Execution flags](#execution-flags)
  - [Default config params](#default-config-params)
- [Performance tests](#performance-tests)
  - [Persistent messages](#persistent-messages)
  - [Transient messages](#transient-messages)
- [Internals](#internals)
  - [Backend for durable entities](#backend-for-durable-entities)
  - [QOS](#qos)
  - [Admin server](#admin-server)
- [TODO](#todo)
- [Contribution](#contribution)

## Goals of this project

- Have fun and learn a lon
- Implement AMQP message broker in Go
- Make protocol compatible with RabbitMQ and standard AMQP 0-9-1.


## Demo
Simple demo server on Digital Ocean, ```2 GB Memory / 25 GB Disk / FRA1 - Ubuntu Docker 17.12.0~ce on 16.04```

|     Server    | Port | Admin port | Login | Password | Virtual Host |
|:-------------:|:----:|:----------:|:-----:|:--------:|:------------:|
| 46.101.117.78 | 5672 |    15672    | guest |   guest  |       /      |

- [AdminServer - http://46.101.117.78:15672/](http://46.101.117.78:15672/)
- Connect uri - ```amqp://guest:guest@46.101.117.78:5672```

## Installation and Building
### Docker

The quick way to start with GarageMQ is by using `docker`. You can build it by your own or pull from docker-hub 
```shell
docker pull amplitudo/garagemq
docker run --name garagemq -p 5672:5672 -p 15672:15672 amplitudo/garagemq
```
or

```shell
go get -u github.com/valinurovam/garagemq/...
cd $GOPATH/src/github.com/valinurovam/garagemq
docker build -t garagemq .
docker run --name garagemq -p 5672:5672 -p 15672:15672 garagemq

```
### Go get
You can also use [go get](https://golang.org/cmd/go/#hdr-Download_and_install_packages_and_dependencies): ```go get -u github.com/valinurovam/garagemq/...```
```shell
go get -u github.com/valinurovam/garagemq/...
cd $GOPATH/src/github.com/valinurovam/garagemq
make build.all && make run
```
### Execution flags
| Flag | Default | Description | ENV |
| :--- | :--- | :--- | :--- |
| --config | [default config](#default-config-params) | Config path | GMQ_CONFIG |
| --log-file | stdout | Log file path or `stdout`, `stderr`| GMQ_LOG_FILE |
| --log-level | info | Logger level | GMQ_LOG_LEVEL |
| --hprof | false | Enable or disable [hprof profiler](https://golang.org/pkg/net/http/pprof/#pkg-overview) | GMQ_HPROF |
| --hprof-host | 0.0.0.0 | Profiler host | GMQ_HPROF_HOST |
| --hprof-port | 8080 | Profiler port | GMQ_HPROF_PORT |

### Default config params
```yaml
# Proto name to implement (amqp-rabbit or amqp-0-9-1)
proto: amqp-rabbit
# User list
users:
  - username: guest
    password: 084e0343a0486ff05530df6c705c8bb4 # guest md5
# Server TCP settings
tcp:
  ip: 0.0.0.0
  port: 5672
  nodelay: false
  readBufSize: 196608
  writeBufSize: 196608
# Admin-server settings
admin:
  ip: 0.0.0.0
  port: 15672
queue:
  shardSize: 8192
  maxMessagesInRam: 131072
# DB settings
db:
  # default path 
  defaultPath: db
  # backend engine (badger or buntdb) 
  engine: badger
# Default virtual host path  
vhost:
  defaultPath: /
# Security check rule (md5 or bcrypt)
security:
  passwordCheck: md5
connection:
  channelsMax: 4096
  frameMaxSize: 65536
```

## Performance tests

Performance tests with load testing tool https://github.com/rabbitmq/rabbitmq-perf-test on test-machine:
``` 
MacBook Pro (15-inch, 2016)
Processor 2,6 GHz Intel Core i7
Memory 16 GB 2133 MHz LPDDR3
```
### Persistent messages
```shell
./bin/runjava com.rabbitmq.perf.PerfTest --exchange test -uri amqp://guest:guest@localhost:5672 --queue test --consumers 10 --producers 5 --qos 100 -flag persistent
...
...
id: test-235131-686, sending rate avg: 53577 msg/s
id: test-235131-686, receiving rate avg: 51941 msg/s
```
### Transient messages
```shell
./bin/runjava com.rabbitmq.perf.PerfTest --exchange test -uri amqp://guest:guest@localhost:5672 --queue test --consumers 10 --producers 5 --qos 100
...
...
id: test-235231-085, sending rate avg: 71247 msg/s
id: test-235231-085, receiving rate avg: 69009 msg/s
```

## Internals

### Backend for durable entities
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

### QOS

`basic.qos` method implemented for standard AMQP and RabbitMQ mode. It means that by default qos applies for connection(global=true) or channel(global=false). 
RabbitMQ Qos means for channel(global=true) or each new consumer(global=false).

### Admin server

The administration server is available at standard `:15672` port and is `read only mode` at the moment. Main page above, and [more screenshots](/readme) at /readme folder

![Overview](readme/overview.jpg)

## TODO
- [ ] Optimize binds
- [ ] Replication and clusterization
- [ ] Own backend for durable entities and persistent messages
- [ ] Migrate to message reference counting

## Contribution
Contribution of any kind is always welcome and appreciated. Contribution Guidelines in WIP