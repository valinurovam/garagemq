# build stage
FROM golang as builder

ENV GO111MODULE=on

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/garagemq

# final stage
FROM alpine
COPY --from=builder /app/bin/garagemq /app/
COPY --from=builder /app/admin-frontend/build /app/admin-frontend/build
WORKDIR /app

EXPOSE 5672 15672

ENTRYPOINT ["/app/garagemq"]