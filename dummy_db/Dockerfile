FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod ./
# No go.sum found, so skipping COPY go.sum

RUN go mod download

COPY . .

RUN go build -o dummy_db main.go

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/dummy_db .

CMD ["./dummy_db"]