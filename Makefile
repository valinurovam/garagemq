amqp.gen:
	go run protocol/protogen.go && go fmt amqp/*_generated.go

deps:
	dep ensure && cd admin-frontend && yarn install

build.all: deps
	go build -o cmd/server cmd/server.go && cd admin-frontend && yarn build

build:
	go build -o cmd/server cmd/server.go

run:
	go build -o cmd/server cmd/server.go && cmd/server

vet:
	go vet github.com/valinurovam/garagemq...

test:
	ulimit -n 1024 && go test -cover github.com/valinurovam/garagemq...