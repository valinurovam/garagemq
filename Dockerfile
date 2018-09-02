FROM golang:1.10-alpine
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh make

ADD . /go/src/github.com/valinurovam/garagemq
WORKDIR /go/src/github.com/valinurovam/garagemq

RUN GOOS=linux GOARCH=amd64 make build

EXPOSE 5672 15672

CMD ["cmd/server"]
