amqp.gen:
	go run protocol/protogen.go && go fmt amqp/*_generated.go

build:
	go build -o cmd/server cmd/server.go

run:
	go build -o cmd/server cmd/server.go && cmd/server