amqp.gen:
	go run protocol/protogen.go && go fmt amqp/*_generated.go