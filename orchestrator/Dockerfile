FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod ./
# No go.sum found

RUN go mod download

COPY . .

RUN go build -o orchestrator main.go

FROM alpine:latest

# Install docker CLI if needed (optional, since it will use socket)
RUN apk add --no-cache docker-cli

WORKDIR /root/

COPY --from=builder /app/orchestrator .

CMD ["./orchestrator"]