FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o orchestrator main.go

FROM scratch

WORKDIR /

COPY --from=builder /app/orchestrator /orchestrator

EXPOSE 8080/tcp 50052/tcp

ENTRYPOINT ["/orchestrator"]