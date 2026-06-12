FROM golang:1.26-alpine AS builder

WORKDIR /app

RUN apk add --no-cache protobuf git
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN PATH="/go/bin:${PATH}" \
    protoc --go_out=. --go-grpc_out=. proto/dummydb.proto

RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o /out/dummy_db .

FROM scratch

WORKDIR /

COPY --from=builder /out/dummy_db /dummy_db

EXPOSE 50051/tcp

ENTRYPOINT ["/dummy_db"]