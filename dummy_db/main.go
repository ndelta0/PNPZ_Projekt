package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"dummy_db/pb"
)

type Config struct {
	ListenAddr string `json:"listen_addr"`
	DataFile   string `json:"data_file"`
	LogLevel   string `json:"log_level"`
	ConfigFile string `json:"-"`
}

type Store struct {
	mu       sync.RWMutex
	data     map[string]string
	dataFile string
	logger   *slog.Logger
}

func NewStore(dataFile string, logger *slog.Logger) (*Store, error) {
	s := &Store{
		data:     make(map[string]string),
		dataFile: dataFile,
		logger:   logger,
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, err := os.ReadFile(s.dataFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.logger.Info("data file not found; starting with empty store", "file", s.dataFile)
			return nil
		}
		return err
	}

	if len(b) == 0 {
		return nil
	}

	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		return fmt.Errorf("failed to parse data file: %w", err)
	}

	s.data = m
	s.logger.Info("store loaded", "keys", len(s.data))
	return nil
}

func (s *Store) saveLocked() error {
	dir := filepath.Dir(s.dataFile)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, ".dummydb-*.json")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(s.data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}

	if err := os.Rename(tmpName, s.dataFile); err != nil {
		os.Remove(tmpName)
		return err
	}

	return nil
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
	if err := s.saveLocked(); err != nil {
		delete(s.data, key)
		return err
	}
	return nil
}

func (s *Store) Delete(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.data[key]
	if !ok {
		return false, nil
	}

	old := s.data[key]
	delete(s.data, key)
	if err := s.saveLocked(); err != nil {
		s.data[key] = old
		return false, err
	}
	return true, nil
}

func (s *Store) List() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

type server struct {
	pb.UnimplementedKeyValueStoreServer
	store  *Store
	logger *slog.Logger
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.logger.Debug("Get request", "key", req.GetKey())

	if req.GetKey() == "" {
		return &pb.GetResponse{Error: "key is required"}, nil
	}
	value, ok := s.store.Get(req.GetKey())
	if !ok {
		s.logger.Debug("Get miss", "key", req.GetKey())
		return &pb.GetResponse{Found: false}, nil
	}

	s.logger.Debug("Get hit", "key", req.GetKey())
	return &pb.GetResponse{Found: true, Value: value}, nil
}

func (s *server) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	s.logger.Debug("Set request", "key", req.GetKey())

	if req.GetKey() == "" {
		return &pb.SetResponse{Ok: false, Error: "key is required"}, nil
	}
	if err := s.store.Set(req.GetKey(), req.GetValue()); err != nil {
		s.logger.Error("set failed", "key", req.GetKey(), "err", err)
		return &pb.SetResponse{Ok: false, Error: err.Error()}, nil
	}
	s.logger.Debug("Set success", "key", req.GetKey())
	return &pb.SetResponse{Ok: true}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.logger.Debug("Delete request", "key", req.GetKey())

	if req.GetKey() == "" {
		return &pb.DeleteResponse{Ok: false, Error: "key is required"}, nil
	}
	ok, err := s.store.Delete(req.GetKey())
	if err != nil {
		s.logger.Error("delete failed", "key", req.GetKey(), "err", err)
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}

	s.logger.Debug("Delete success", "key", req.GetKey())
	return &pb.DeleteResponse{Ok: ok}, nil
}

func (s *server) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	s.logger.Debug("List request")

	items := s.store.List()
	resp := &pb.ListResponse{
		Items: make([]*pb.KeyValue, 0, len(items)),
	}
	for k, v := range items {
		resp.Items = append(resp.Items, &pb.KeyValue{Key: k, Value: v})
	}

	s.logger.Debug("List success", "count", len(resp.Items))
	return resp, nil
}

func loadConfig(path string) (Config, error) {
	cfg := Config{
		ListenAddr: ":50051",
		DataFile:   "./data/db.json",
		LogLevel:   "info",
	}
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func requestLoggerInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()

		p, _ := peer.FromContext(ctx)
		fields := []any{
			"method", info.FullMethod,
		}
		if p != nil {
			fields = append(fields, "peer", p.Addr.String())
		}

		logger.Debug("grpc request received", fields...)

		resp, err := handler(ctx, req)

		fields = append(fields, "duration_ms", time.Since(start).Milliseconds())
		if err != nil {
			logger.Error("grpc request failed", append(fields, "err", err)...)
		} else {
			logger.Debug("grpc request completed", fields...)
		}

		return resp, err
	}
}

func main() {
	var (
		configPath = flag.String("config", "", "path to JSON config file")
		listenAddr = flag.String("listen", "", "gRPC listen address")
		dataFile   = flag.String("data", "", "path to JSON backing file")
		logLevel   = flag.String("log-level", "", "debug|info|warn|error")
	)
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		panic(err)
	}

	if *listenAddr != "" {
		cfg.ListenAddr = *listenAddr
	}
	if *dataFile != "" {
		cfg.DataFile = *dataFile
	}
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}

	level := new(slog.LevelVar)
	switch cfg.LogLevel {
	case "debug":
		level.Set(slog.LevelDebug)
	case "warn":
		level.Set(slog.LevelWarn)
	case "error":
		level.Set(slog.LevelError)
	default:
		level.Set(slog.LevelInfo)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	store, err := NewStore(cfg.DataFile, logger)
	if err != nil {
		logger.Error("failed to initialize store", "err", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Error("failed to listen", "addr", cfg.ListenAddr, "err", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(requestLoggerInterceptor(logger)))
	pb.RegisterKeyValueStoreServer(grpcServer, &server{
		store:  store,
		logger: logger,
	})

	logger.Info("server starting", "addr", cfg.ListenAddr, "data_file", cfg.DataFile)

	if err := grpcServer.Serve(lis); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
