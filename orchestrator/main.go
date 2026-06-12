package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	rand "math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	labelManaged = "orchestrator.managed"
	labelProject = "orchestrator.project"
	labelService = "orchestrator.service"
	labelIndex   = "orchestrator.index"

	serviceDB       = "db"
	serviceBackend  = "backend"
	serviceFrontend = "frontend"
)

type Config struct {
	DockerSocket       string
	HTTPAddr           string
	BackendProxyAddr   string
	ProjectName        string
	NetworkName        string
	DBImage            string
	BackendImage       string
	FrontendImage      string
	DBDataPath         string
	FrontendHostPort   string
	BackendCPUs        float64
	BackendMinReplicas int
	BackendMaxReplicas int
	ScaleUpCPU         float64
	ScaleDownCPU       float64
	ScaleUpLatency     time.Duration
	ScaleDownLatency   time.Duration
	ScaleCooldown      time.Duration
	ReconcileInterval  time.Duration
	HealthInterval     time.Duration
}

func loadConfig() Config {
	cfg := Config{
		DockerSocket:       envString("DOCKER_SOCKET", "/var/run/docker.sock"),
		HTTPAddr:           envString("ORCH_HTTP_ADDR", ":8080"),
		BackendProxyAddr:   envString("ORCH_BACKEND_PROXY_ADDR", ":50052"),
		ProjectName:        envString("ORCH_PROJECT", "dummy-stack"),
		NetworkName:        envString("ORCH_NETWORK", "dummy_stack_net"),
		DBImage:            envString("ORCH_DB_IMAGE", "dummy_db:latest"),
		BackendImage:       envString("ORCH_BACKEND_IMAGE", "dummy_backend:latest"),
		FrontendImage:      envString("ORCH_FRONTEND_IMAGE", "dummy_frontend:latest"),
		DBDataPath:         envString("ORCH_DB_DATA_PATH", "/tmp/dummy_stack/db.json"),
		FrontendHostPort:   envString("ORCH_FRONTEND_HOST_PORT", "3001"),
		BackendCPUs:        envFloat("ORCH_BACKEND_CPUS", 0),
		BackendMinReplicas: envInt("ORCH_BACKEND_MIN", 1),
		BackendMaxReplicas: envInt("ORCH_BACKEND_MAX", 3),
		ScaleUpCPU:         envFloat("ORCH_SCALE_UP_CPU", 70),
		ScaleDownCPU:       envFloat("ORCH_SCALE_DOWN_CPU", 20),
		ScaleUpLatency:     envDurationMS("ORCH_SCALE_UP_LATENCY_MS", 600*time.Millisecond),
		ScaleDownLatency:   envDurationMS("ORCH_SCALE_DOWN_LATENCY_MS", 200*time.Millisecond),
		ScaleCooldown:      envDurationMS("ORCH_SCALE_COOLDOWN_MS", 30*time.Second),
		ReconcileInterval:  envDurationMS("ORCH_RECONCILE_INTERVAL_MS", 10*time.Second),
		HealthInterval:     envDurationMS("ORCH_HEALTH_INTERVAL_MS", 5*time.Second),
	}

	if cfg.BackendMinReplicas < 0 {
		cfg.BackendMinReplicas = 0
	}
	if cfg.BackendMaxReplicas < 1 {
		cfg.BackendMaxReplicas = 1
	}
	if cfg.BackendMinReplicas > cfg.BackendMaxReplicas {
		cfg.BackendMinReplicas = cfg.BackendMaxReplicas
	}
	return cfg
}

func envString(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envFloat(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDurationMS(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return time.Duration(parsed) * time.Millisecond
}

type DockerClient struct {
	httpClient *http.Client
	socket     string
}

func NewDockerClient(socket string) *DockerClient {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}
	return &DockerClient{
		httpClient: &http.Client{Transport: transport},
		socket:     socket,
	}
}

func (c *DockerClient) do(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("docker %s %s failed: %s: %s", method, path, resp.Status, strings.TrimSpace(string(data)))
	}

	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *DockerClient) stream(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker"+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("docker stream %s failed: %s: %s", path, resp.Status, strings.TrimSpace(string(data)))
	}
	return resp, nil
}

func (c *DockerClient) Ping(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/_ping", nil, nil)
}

type NetworkInspect struct {
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

func (c *DockerClient) InspectNetwork(ctx context.Context, name string) (NetworkInspect, error) {
	var network NetworkInspect
	err := c.do(ctx, http.MethodGet, "/networks/"+url.PathEscape(name), nil, &network)
	return network, err
}

func (c *DockerClient) CreateNetwork(ctx context.Context, name string, labels map[string]string) error {
	body := map[string]any{
		"Name":       name,
		"Driver":     "bridge",
		"Attachable": true,
		"Labels":     labels,
	}
	return c.do(ctx, http.MethodPost, "/networks/create", body, nil)
}

func (c *DockerClient) ConnectNetwork(ctx context.Context, networkName, containerID string) error {
	body := map[string]any{"Container": containerID}
	return c.do(ctx, http.MethodPost, "/networks/"+url.PathEscape(networkName)+"/connect", body, nil)
}

type ContainerSummary struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	Image  string            `json:"Image"`
	State  string            `json:"State"`
	Status string            `json:"Status"`
	Labels map[string]string `json:"Labels"`
}

func (c *DockerClient) ListContainers(ctx context.Context, labels map[string]string) ([]ContainerSummary, error) {
	filters := map[string][]string{}
	for key, value := range labels {
		filters["label"] = append(filters["label"], key+"="+value)
	}
	filterJSON, err := json.Marshal(filters)
	if err != nil {
		return nil, err
	}
	path := "/containers/json?all=1&filters=" + url.QueryEscape(string(filterJSON))

	var containers []ContainerSummary
	if err := c.do(ctx, http.MethodGet, path, nil, &containers); err != nil {
		return nil, err
	}
	return containers, nil
}

type ContainerInspect struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Image string `json:"Image"`
	State struct {
		Status     string `json:"Status"`
		Running    bool   `json:"Running"`
		Paused     bool   `json:"Paused"`
		Restarting bool   `json:"Restarting"`
		OOMKilled  bool   `json:"OOMKilled"`
		Dead       bool   `json:"Dead"`
		ExitCode   int    `json:"ExitCode"`
		Error      string `json:"Error"`
		StartedAt  string `json:"StartedAt"`
		FinishedAt string `json:"FinishedAt"`
	} `json:"State"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	NetworkSettings struct {
		Networks map[string]struct {
			IPAddress string `json:"IPAddress"`
		} `json:"Networks"`
	} `json:"NetworkSettings"`
}

func (c *DockerClient) InspectContainer(ctx context.Context, id string) (ContainerInspect, error) {
	var inspected ContainerInspect
	err := c.do(ctx, http.MethodGet, "/containers/"+url.PathEscape(id)+"/json", nil, &inspected)
	return inspected, err
}

type ContainerCreateRequest struct {
	Image            string              `json:"Image"`
	Cmd              []string            `json:"Cmd,omitempty"`
	Env              []string            `json:"Env,omitempty"`
	Labels           map[string]string   `json:"Labels,omitempty"`
	ExposedPorts     map[string]struct{} `json:"ExposedPorts,omitempty"`
	HostConfig       HostConfig          `json:"HostConfig,omitempty"`
	NetworkingConfig NetworkingConfig    `json:"NetworkingConfig,omitempty"`
	Healthcheck      *Healthcheck        `json:"Healthcheck,omitempty"`
}

type HostConfig struct {
	Binds         []string                 `json:"Binds,omitempty"`
	PortBindings  map[string][]PortBinding `json:"PortBindings,omitempty"`
	RestartPolicy RestartPolicy            `json:"RestartPolicy,omitempty"`
	NanoCPUs      int64                    `json:"NanoCpus,omitempty"`
}

type PortBinding struct {
	HostIP   string `json:"HostIp,omitempty"`
	HostPort string `json:"HostPort,omitempty"`
}

type RestartPolicy struct {
	Name string `json:"Name"`
}

type NetworkingConfig struct {
	EndpointsConfig map[string]EndpointSettings `json:"EndpointsConfig,omitempty"`
}

type EndpointSettings struct {
	Aliases []string `json:"Aliases,omitempty"`
}

type Healthcheck struct {
	Test        []string `json:"Test,omitempty"`
	Interval    int64    `json:"Interval,omitempty"`
	Timeout     int64    `json:"Timeout,omitempty"`
	Retries     int      `json:"Retries,omitempty"`
	StartPeriod int64    `json:"StartPeriod,omitempty"`
}

type ContainerCreateResponse struct {
	ID       string   `json:"Id"`
	Warnings []string `json:"Warnings"`
}

func (c *DockerClient) CreateContainer(ctx context.Context, name string, req ContainerCreateRequest) (ContainerCreateResponse, error) {
	var response ContainerCreateResponse
	path := "/containers/create?name=" + url.QueryEscape(name)
	err := c.do(ctx, http.MethodPost, path, req, &response)
	return response, err
}

func (c *DockerClient) StartContainer(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/containers/"+url.PathEscape(id)+"/start", nil, nil)
}

func (c *DockerClient) RemoveContainer(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/containers/"+url.PathEscape(id)+"?force=true&v=true", nil, nil)
}

type StatsResponse struct {
	Read    string `json:"read"`
	CPUStat struct {
		CPUUsage struct {
			TotalUsage        uint64   `json:"total_usage"`
			PercpuUsage       []uint64 `json:"percpu_usage"`
			UsageInKernelmode uint64   `json:"usage_in_kernelmode"`
			UsageInUsermode   uint64   `json:"usage_in_usermode"`
		} `json:"cpu_usage"`
		SystemCPUUsage uint64 `json:"system_cpu_usage"`
		OnlineCPUs     uint32 `json:"online_cpus"`
	} `json:"cpu_stats"`
	PreCPUStat struct {
		CPUUsage struct {
			TotalUsage uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
		SystemCPUUsage uint64 `json:"system_cpu_usage"`
	} `json:"precpu_stats"`
	MemoryStats struct {
		Usage uint64 `json:"usage"`
		Limit uint64 `json:"limit"`
	} `json:"memory_stats"`
}

func (c *DockerClient) ContainerStats(ctx context.Context, id string) (StatsResponse, error) {
	var stats StatsResponse
	err := c.do(ctx, http.MethodGet, "/containers/"+url.PathEscape(id)+"/stats?stream=false", nil, &stats)
	return stats, err
}

type Event struct {
	Time    time.Time      `json:"time"`
	Type    string         `json:"type"`
	Service string         `json:"service,omitempty"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

type EventBroker struct {
	mu          sync.Mutex
	subscribers map[chan Event]struct{}
	history     []Event
	limit       int
}

func NewEventBroker(limit int) *EventBroker {
	return &EventBroker{
		subscribers: make(map[chan Event]struct{}),
		limit:       limit,
	}
}

func (b *EventBroker) Publish(event Event) {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}

	b.mu.Lock()
	b.history = append(b.history, event)
	if len(b.history) > b.limit {
		b.history = b.history[len(b.history)-b.limit:]
	}
	for subscriber := range b.subscribers {
		select {
		case subscriber <- event:
		default:
		}
	}
	b.mu.Unlock()
}

func (b *EventBroker) Subscribe() (<-chan Event, func(), []Event) {
	ch := make(chan Event, 64)
	b.mu.Lock()
	history := append([]Event(nil), b.history...)
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()

	cancel := func() {
		b.mu.Lock()
		delete(b.subscribers, ch)
		close(ch)
		b.mu.Unlock()
	}
	return ch, cancel, history
}

type BackendRef struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	IPAddress string    `json:"ip_address"`
	Healthy   bool      `json:"healthy"`
	LatencyMS float64   `json:"latency_ms"`
	LastCheck time.Time `json:"last_check"`
}

type Metrics struct {
	AverageCPU       float64 `json:"average_cpu"`
	AverageLatencyMS float64 `json:"average_latency_ms"`
	MemoryBytes      uint64  `json:"memory_bytes"`
}

type ContainerStatus struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Service   string            `json:"service"`
	Image     string            `json:"image"`
	State     string            `json:"state"`
	Status    string            `json:"status"`
	Running   bool              `json:"running"`
	OOMKilled bool              `json:"oom_killed"`
	ExitCode  int               `json:"exit_code"`
	IPAddress string            `json:"ip_address,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type StatusSnapshot struct {
	Project                string            `json:"project"`
	Network                string            `json:"network"`
	Enabled                bool              `json:"enabled"`
	Config                 RuntimeConfig     `json:"config"`
	Traffic                TrafficStats      `json:"traffic"`
	DesiredBackendReplicas int               `json:"desired_backend_replicas"`
	BackendMinReplicas     int               `json:"backend_min_replicas"`
	BackendMaxReplicas     int               `json:"backend_max_replicas"`
	Metrics                Metrics           `json:"metrics"`
	Backends               []BackendRef      `json:"backends"`
	Containers             []ContainerStatus `json:"containers"`
	Events                 []Event           `json:"events"`
}

type RuntimeConfig struct {
	HTTPAddr            string  `json:"http_addr"`
	BackendProxyAddr    string  `json:"backend_proxy_addr"`
	ProjectName         string  `json:"project_name"`
	NetworkName         string  `json:"network_name"`
	DBImage             string  `json:"db_image"`
	BackendImage        string  `json:"backend_image"`
	FrontendImage       string  `json:"frontend_image"`
	DBDataPath          string  `json:"db_data_path"`
	FrontendHostPort    string  `json:"frontend_host_port"`
	BackendCPUs         float64 `json:"backend_cpus"`
	BackendMinReplicas  int     `json:"backend_min_replicas"`
	BackendMaxReplicas  int     `json:"backend_max_replicas"`
	ScaleUpCPU          float64 `json:"scale_up_cpu"`
	ScaleDownCPU        float64 `json:"scale_down_cpu"`
	ScaleUpLatencyMS    int64   `json:"scale_up_latency_ms"`
	ScaleDownLatencyMS  int64   `json:"scale_down_latency_ms"`
	ScaleCooldownMS     int64   `json:"scale_cooldown_ms"`
	ReconcileIntervalMS int64   `json:"reconcile_interval_ms"`
	HealthIntervalMS    int64   `json:"health_interval_ms"`
}

type RuntimeConfigPatch struct {
	DBImage             *string  `json:"db_image"`
	BackendImage        *string  `json:"backend_image"`
	FrontendImage       *string  `json:"frontend_image"`
	DBDataPath          *string  `json:"db_data_path"`
	FrontendHostPort    *string  `json:"frontend_host_port"`
	BackendCPUs         *float64 `json:"backend_cpus"`
	BackendMinReplicas  *int     `json:"backend_min_replicas"`
	BackendMaxReplicas  *int     `json:"backend_max_replicas"`
	ScaleUpCPU          *float64 `json:"scale_up_cpu"`
	ScaleDownCPU        *float64 `json:"scale_down_cpu"`
	ScaleUpLatencyMS    *int64   `json:"scale_up_latency_ms"`
	ScaleDownLatencyMS  *int64   `json:"scale_down_latency_ms"`
	ScaleCooldownMS     *int64   `json:"scale_cooldown_ms"`
	ReconcileIntervalMS *int64   `json:"reconcile_interval_ms"`
	HealthIntervalMS    *int64   `json:"health_interval_ms"`
}

type TrafficConfig struct {
	RequestsPerSecond int    `json:"requests_per_second"`
	Concurrency       int    `json:"concurrency"`
	RandomDelay       bool   `json:"random_delay"`
	RandomDelayChance int    `json:"random_delay_chance"`
	MaxRandomDelayMS  int    `json:"max_random_delay_ms"`
	SlowLoris         bool   `json:"slow_loris"`
	SlowLorisChance   int    `json:"slow_loris_chance"`
	BurstDoS          bool   `json:"burst_dos"`
	BurstChance       int    `json:"burst_chance"`
	BurstSize         int    `json:"burst_size"`
	TimeoutMS         int    `json:"timeout_ms"`
	KeyPrefix         string `json:"key_prefix"`
}

type TrafficStats struct {
	Running          bool          `json:"running"`
	Config           TrafficConfig `json:"config"`
	StartedAt        *time.Time    `json:"started_at,omitempty"`
	Sent             uint64        `json:"sent"`
	OK               uint64        `json:"ok"`
	Failed           uint64        `json:"failed"`
	InFlight         int           `json:"in_flight"`
	Slow             uint64        `json:"slow"`
	Bursts           uint64        `json:"bursts"`
	AverageLatencyMS float64       `json:"average_latency_ms"`
	ActualRPS        float64       `json:"actual_rps"`
	LastError        string        `json:"last_error,omitempty"`
}

type Manager struct {
	cfg         Config
	docker      *DockerClient
	logger      *slog.Logger
	events      *EventBroker
	traffic     *LoadGenerator
	reconcileMu sync.Mutex

	mu              sync.RWMutex
	enabled         bool
	desiredBackends int
	backends        []BackendRef
	failures        map[string]int
	containers      []ContainerStatus
	metrics         Metrics
	lastScale       time.Time
	roundRobin      int
}

func NewManager(cfg Config, docker *DockerClient, logger *slog.Logger, events *EventBroker) *Manager {
	m := &Manager{
		cfg:             cfg,
		docker:          docker,
		logger:          logger,
		events:          events,
		enabled:         true,
		desiredBackends: cfg.BackendMinReplicas,
		failures:        make(map[string]int),
	}
	m.traffic = NewLoadGenerator(m, logger, events)
	return m
}

func (m *Manager) Run(ctx context.Context) {
	m.publish("info", "", "orchestrator starting", map[string]any{
		"network":               m.cfg.NetworkName,
		"backend_min_replicas":  m.cfg.BackendMinReplicas,
		"backend_max_replicas":  m.cfg.BackendMaxReplicas,
		"db_data_path":          m.cfg.DBDataPath,
		"backend_proxy_address": m.cfg.BackendProxyAddr,
	})

	m.Reconcile(ctx, "startup")

	go m.watchDockerEvents(ctx)
	go m.reconcileLoop(ctx)
	go m.healthLoop(ctx)
}

func (m *Manager) reconcileLoop(ctx context.Context) {
	for {
		timer := time.NewTimer(m.reconcileInterval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			m.Reconcile(ctx, "periodic")
		}
	}
}

func (m *Manager) healthLoop(ctx context.Context) {
	for {
		timer := time.NewTimer(m.healthInterval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			m.refreshBackendHealth(ctx)
			m.refreshMetrics(ctx)
			m.applyAutoscaling(ctx)
		}
	}
}

func (m *Manager) reconcileInterval() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg.ReconcileInterval
}

func (m *Manager) healthInterval() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg.HealthInterval
}

func (m *Manager) Reconcile(ctx context.Context, reason string) {
	m.reconcileMu.Lock()
	defer m.reconcileMu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	if !m.isEnabled() {
		m.refreshContainers(ctx)
		m.refreshBackendRefs(ctx)
		m.publish("info", "", "reconcile skipped while stopped", map[string]any{"reason": reason})
		return
	}

	if err := m.ensureNetwork(ctx); err != nil {
		m.publish("error", "", "network reconcile failed", map[string]any{"error": err.Error()})
		return
	}

	containers, err := m.managedContainers(ctx)
	if err != nil {
		m.publish("error", "", "container list failed", map[string]any{"error": err.Error()})
		return
	}

	byService := groupByService(containers)
	m.ensureSingleton(ctx, serviceDB, byService[serviceDB])
	m.ensureBackendReplicas(ctx, byService[serviceBackend])
	m.ensureSingleton(ctx, serviceFrontend, byService[serviceFrontend])
	m.refreshContainers(ctx)
	m.refreshBackendRefs(ctx)
	m.publish("info", "", "reconcile completed", map[string]any{"reason": reason})
}

func (m *Manager) ensureNetwork(ctx context.Context) error {
	_, err := m.docker.InspectNetwork(ctx, m.cfg.NetworkName)
	if err != nil {
		if createErr := m.docker.CreateNetwork(ctx, m.cfg.NetworkName, m.baseLabels("network", "")); createErr != nil {
			return createErr
		}
		m.publish("action", "", "network created", map[string]any{"network": m.cfg.NetworkName})
	}

	hostname, hostErr := os.Hostname()
	if hostErr == nil && hostname != "" {
		err := m.docker.ConnectNetwork(ctx, m.cfg.NetworkName, hostname)
		if err != nil && !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "is already connected") && !strings.Contains(err.Error(), "No such container") {
			m.logger.Debug("self network attach skipped", "err", err)
		}
	}
	return nil
}

func (m *Manager) managedContainers(ctx context.Context) ([]ContainerSummary, error) {
	return m.docker.ListContainers(ctx, map[string]string{
		labelManaged: "true",
		labelProject: m.cfg.ProjectName,
	})
}

func groupByService(containers []ContainerSummary) map[string][]ContainerSummary {
	grouped := map[string][]ContainerSummary{}
	for _, container := range containers {
		service := container.Labels[labelService]
		grouped[service] = append(grouped[service], container)
	}
	return grouped
}

func (m *Manager) ensureSingleton(ctx context.Context, service string, containers []ContainerSummary) {
	running := make([]ContainerSummary, 0, len(containers))
	for _, container := range containers {
		if container.State == "running" {
			running = append(running, container)
			continue
		}
		m.removeContainer(ctx, service, container.ID, "stale singleton container")
	}

	sort.Slice(running, func(i, j int) bool {
		return containerName(running[i]) < containerName(running[j])
	})

	for i := 1; i < len(running); i++ {
		m.removeContainer(ctx, service, running[i].ID, "extra singleton container")
	}
	if len(running) > 0 {
		return
	}

	if err := m.createServiceContainer(ctx, service, 1); err != nil {
		m.publish("error", service, "container create failed", map[string]any{"error": err.Error()})
	}
}

func (m *Manager) ensureBackendReplicas(ctx context.Context, containers []ContainerSummary) {
	runningByIndex := map[int]ContainerSummary{}
	usedIndexes := map[int]bool{}

	for _, container := range containers {
		index := parseIndex(container.Labels[labelIndex])
		if index > 0 {
			usedIndexes[index] = true
		}
		if container.State != "running" {
			m.removeContainer(ctx, serviceBackend, container.ID, "stale backend container")
			continue
		}
		if index <= 0 || runningByIndex[index].ID != "" {
			m.removeContainer(ctx, serviceBackend, container.ID, "duplicate backend container")
			continue
		}
		runningByIndex[index] = container
	}

	runningCount := len(runningByIndex)
	desired := m.getDesiredBackends()

	if runningCount > desired {
		indexes := make([]int, 0, len(runningByIndex))
		for index := range runningByIndex {
			indexes = append(indexes, index)
		}
		sort.Sort(sort.Reverse(sort.IntSlice(indexes)))
		for _, index := range indexes {
			if runningCount <= desired {
				break
			}
			m.removeContainer(ctx, serviceBackend, runningByIndex[index].ID, "backend scale down")
			runningCount--
		}
		return
	}

	for runningCount < desired {
		index := firstAvailableIndex(usedIndexes, m.cfg.BackendMaxReplicas)
		if index == 0 {
			m.publish("error", serviceBackend, "no backend index available", nil)
			return
		}
		usedIndexes[index] = true
		if err := m.createServiceContainer(ctx, serviceBackend, index); err != nil {
			m.publish("error", serviceBackend, "container create failed", map[string]any{"index": index, "error": err.Error()})
			return
		}
		runningCount++
	}
}

func firstAvailableIndex(used map[int]bool, max int) int {
	for i := 1; i <= max; i++ {
		if !used[i] {
			return i
		}
	}
	return 0
}

func parseIndex(value string) int {
	index, _ := strconv.Atoi(value)
	return index
}

func (m *Manager) createServiceContainer(ctx context.Context, service string, index int) error {
	req, name := m.containerSpec(service, index)
	created, err := m.docker.CreateContainer(ctx, name, req)
	if err != nil {
		return err
	}
	if err := m.docker.StartContainer(ctx, created.ID); err != nil {
		_ = m.docker.RemoveContainer(ctx, created.ID)
		return err
	}
	m.publish("action", service, "container started", map[string]any{"name": name, "id": shortID(created.ID)})
	return nil
}

func (m *Manager) containerSpec(service string, index int) (ContainerCreateRequest, string) {
	labels := m.baseLabels(service, strconv.Itoa(index))
	networking := NetworkingConfig{
		EndpointsConfig: map[string]EndpointSettings{
			m.cfg.NetworkName: {
				Aliases: aliasesForService(service, index),
			},
		},
	}

	switch service {
	case serviceDB:
		dataDir := filepath.Dir(m.cfg.DBDataPath)
		dataFile := filepath.Base(m.cfg.DBDataPath)
		name := "dummy_db"
		return ContainerCreateRequest{
			Image:  m.cfg.DBImage,
			Cmd:    []string{"--data", "/data/" + dataFile},
			Labels: labels,
			ExposedPorts: map[string]struct{}{
				"50051/tcp": {},
			},
			HostConfig: HostConfig{
				Binds:         []string{dataDir + ":/data"},
				RestartPolicy: RestartPolicy{Name: "no"},
			},
			NetworkingConfig: networking,
		}, name
	case serviceBackend:
		name := fmt.Sprintf("dummy_backend_%d", index)
		return ContainerCreateRequest{
			Image:  m.cfg.BackendImage,
			Env:    []string{"DB_GRPC_HOST=dummy_db", "DB_GRPC_PORT=50051"},
			Labels: labels,
			ExposedPorts: map[string]struct{}{
				"50052/tcp": {},
			},
			HostConfig:       HostConfig{RestartPolicy: RestartPolicy{Name: "no"}, NanoCPUs: nanoCPUs(m.cfg.BackendCPUs)},
			NetworkingConfig: networking,
			Healthcheck: &Healthcheck{
				Test:        []string{"CMD-SHELL", "python -c \"import urllib.request; urllib.request.urlopen('http://127.0.0.1:50052/health', timeout=2)\""},
				Interval:    int64(5 * time.Second),
				Timeout:     int64(2 * time.Second),
				Retries:     3,
				StartPeriod: int64(5 * time.Second),
			},
		}, name
	case serviceFrontend:
		name := "dummy_frontend"
		return ContainerCreateRequest{
			Image:  m.cfg.FrontendImage,
			Env:    []string{"PUBLIC_BACKEND_URL=http://localhost" + m.cfg.BackendProxyAddr},
			Labels: labels,
			ExposedPorts: map[string]struct{}{
				"3000/tcp": {},
			},
			HostConfig: HostConfig{
				PortBindings: map[string][]PortBinding{
					"3000/tcp": {{HostIP: "0.0.0.0", HostPort: m.cfg.FrontendHostPort}},
				},
				RestartPolicy: RestartPolicy{Name: "no"},
			},
			NetworkingConfig: networking,
		}, name
	default:
		panic("unknown service: " + service)
	}
}

func aliasesForService(service string, index int) []string {
	switch service {
	case serviceDB:
		return []string{"dummy_db"}
	case serviceBackend:
		return []string{"dummy_backend", fmt.Sprintf("dummy_backend_%d", index)}
	case serviceFrontend:
		return []string{"dummy_frontend"}
	default:
		return nil
	}
}

func (m *Manager) baseLabels(service, index string) map[string]string {
	labels := map[string]string{
		labelManaged: "true",
		labelProject: m.cfg.ProjectName,
	}
	if service != "" {
		labels[labelService] = service
	}
	if index != "" {
		labels[labelIndex] = index
	}
	return labels
}

func (m *Manager) removeContainer(ctx context.Context, service, id, reason string) {
	if err := m.docker.RemoveContainer(ctx, id); err != nil {
		m.publish("error", service, "container remove failed", map[string]any{"id": shortID(id), "reason": reason, "error": err.Error()})
		return
	}
	m.publish("action", service, "container removed", map[string]any{"id": shortID(id), "reason": reason})
}

func (m *Manager) refreshContainers(ctx context.Context) {
	containers, err := m.managedContainers(ctx)
	if err != nil {
		m.publish("error", "", "status refresh failed", map[string]any{"error": err.Error()})
		return
	}

	statuses := make([]ContainerStatus, 0, len(containers))
	for _, summary := range containers {
		inspected, err := m.docker.InspectContainer(ctx, summary.ID)
		if err != nil {
			continue
		}
		statuses = append(statuses, containerStatus(summary, inspected, m.cfg.NetworkName))
	}

	sort.Slice(statuses, func(i, j int) bool {
		if statuses[i].Service == statuses[j].Service {
			return statuses[i].Name < statuses[j].Name
		}
		return statuses[i].Service < statuses[j].Service
	})

	m.mu.Lock()
	m.containers = statuses
	m.mu.Unlock()
}

func containerStatus(summary ContainerSummary, inspected ContainerInspect, networkName string) ContainerStatus {
	return ContainerStatus{
		ID:        inspected.ID,
		Name:      strings.TrimPrefix(inspected.Name, "/"),
		Service:   summary.Labels[labelService],
		Image:     summary.Image,
		State:     inspected.State.Status,
		Status:    summary.Status,
		Running:   inspected.State.Running,
		OOMKilled: inspected.State.OOMKilled,
		ExitCode:  inspected.State.ExitCode,
		IPAddress: containerIP(inspected, networkName),
		Labels:    summary.Labels,
	}
}

func (m *Manager) refreshBackendRefs(ctx context.Context) {
	containers, err := m.managedContainers(ctx)
	if err != nil {
		return
	}
	refs := make([]BackendRef, 0)
	for _, summary := range containers {
		if summary.Labels[labelService] != serviceBackend || summary.State != "running" {
			continue
		}
		inspected, err := m.docker.InspectContainer(ctx, summary.ID)
		if err != nil {
			continue
		}
		ip := containerIP(inspected, m.cfg.NetworkName)
		if ip == "" {
			continue
		}
		refs = append(refs, BackendRef{
			ID:        inspected.ID,
			Name:      strings.TrimPrefix(inspected.Name, "/"),
			IPAddress: ip,
			Healthy:   true,
		})
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Name < refs[j].Name })

	m.mu.Lock()
	existing := map[string]BackendRef{}
	for _, backend := range m.backends {
		existing[backend.ID] = backend
	}
	for i := range refs {
		if previous, ok := existing[refs[i].ID]; ok {
			refs[i].Healthy = previous.Healthy
			refs[i].LatencyMS = previous.LatencyMS
			refs[i].LastCheck = previous.LastCheck
		}
	}
	m.backends = refs
	m.mu.Unlock()
}

func containerIP(inspected ContainerInspect, networkName string) string {
	if network, ok := inspected.NetworkSettings.Networks[networkName]; ok {
		return network.IPAddress
	}
	for _, network := range inspected.NetworkSettings.Networks {
		if network.IPAddress != "" {
			return network.IPAddress
		}
	}
	return ""
}

func (m *Manager) refreshBackendHealth(ctx context.Context) {
	m.refreshBackendRefs(ctx)

	backends := m.backendSnapshot()
	client := &http.Client{Timeout: 2 * time.Second}
	now := time.Now().UTC()
	updated := make([]BackendRef, 0, len(backends))
	unhealthy := make([]BackendRef, 0)

	for _, backend := range backends {
		start := time.Now()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+backend.IPAddress+":50052/health", nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		latency := time.Since(start)
		healthy := err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300
		if resp != nil {
			_ = resp.Body.Close()
		}

		backend.Healthy = healthy
		backend.LatencyMS = float64(latency.Microseconds()) / 1000
		backend.LastCheck = now
		updated = append(updated, backend)

		if healthy {
			m.failures[backend.ID] = 0
			continue
		}
		m.failures[backend.ID]++
		if m.failures[backend.ID] >= 3 {
			unhealthy = append(unhealthy, backend)
		}
	}

	m.mu.Lock()
	m.backends = updated
	m.mu.Unlock()

	for _, backend := range unhealthy {
		m.publish("warning", serviceBackend, "backend health check failed repeatedly", map[string]any{
			"name": backend.Name,
			"id":   shortID(backend.ID),
		})
		m.removeContainer(ctx, serviceBackend, backend.ID, "failed health checks")
		delete(m.failures, backend.ID)
	}

	if len(unhealthy) > 0 {
		m.Reconcile(ctx, "health")
	}
}

func (m *Manager) refreshMetrics(ctx context.Context) {
	backends := m.backendSnapshot()
	if len(backends) == 0 {
		m.mu.Lock()
		m.metrics = Metrics{}
		m.mu.Unlock()
		return
	}

	var cpuTotal float64
	var memoryTotal uint64
	var cpuCount int
	for _, backend := range backends {
		stats, err := m.docker.ContainerStats(ctx, backend.ID)
		if err != nil {
			continue
		}
		cpu := calculateCPU(stats)
		if !math.IsNaN(cpu) && !math.IsInf(cpu, 0) {
			cpuTotal += cpu
			cpuCount++
		}
		memoryTotal += stats.MemoryStats.Usage
	}

	var latencyTotal float64
	var latencyCount int
	for _, backend := range backends {
		if backend.Healthy && backend.LatencyMS > 0 {
			latencyTotal += backend.LatencyMS
			latencyCount++
		}
	}

	metrics := Metrics{MemoryBytes: memoryTotal}
	if cpuCount > 0 {
		metrics.AverageCPU = round2(cpuTotal / float64(cpuCount))
	}
	if latencyCount > 0 {
		metrics.AverageLatencyMS = round2(latencyTotal / float64(latencyCount))
	}

	m.mu.Lock()
	m.metrics = metrics
	m.mu.Unlock()
}

func calculateCPU(stats StatsResponse) float64 {
	cpuDelta := float64(stats.CPUStat.CPUUsage.TotalUsage - stats.PreCPUStat.CPUUsage.TotalUsage)
	systemDelta := float64(stats.CPUStat.SystemCPUUsage - stats.PreCPUStat.SystemCPUUsage)
	onlineCPUs := float64(stats.CPUStat.OnlineCPUs)
	if onlineCPUs == 0 {
		onlineCPUs = float64(len(stats.CPUStat.CPUUsage.PercpuUsage))
	}
	if cpuDelta <= 0 || systemDelta <= 0 || onlineCPUs <= 0 {
		return 0
	}
	return (cpuDelta / systemDelta) * onlineCPUs * 100
}

func round2(value float64) float64 {
	return math.Round(value*100) / 100
}

func (m *Manager) applyAutoscaling(ctx context.Context) {
	m.mu.RLock()
	enabled := m.enabled
	metrics := m.metrics
	desired := m.desiredBackends
	lastScale := m.lastScale
	m.mu.RUnlock()

	if !enabled {
		return
	}
	if time.Since(lastScale) < m.cfg.ScaleCooldown {
		return
	}

	next := desired
	switch {
	case (metrics.AverageCPU >= m.cfg.ScaleUpCPU || time.Duration(metrics.AverageLatencyMS*float64(time.Millisecond)) >= m.cfg.ScaleUpLatency) && desired < m.cfg.BackendMaxReplicas:
		next = desired + 1
	case metrics.AverageCPU <= m.cfg.ScaleDownCPU && metrics.AverageLatencyMS > 0 && time.Duration(metrics.AverageLatencyMS*float64(time.Millisecond)) <= m.cfg.ScaleDownLatency && desired > m.cfg.BackendMinReplicas:
		next = desired - 1
	}
	if next == desired {
		return
	}

	m.mu.Lock()
	m.desiredBackends = next
	m.lastScale = time.Now()
	m.mu.Unlock()

	m.publish("action", serviceBackend, "autoscale decision", map[string]any{
		"from":               desired,
		"to":                 next,
		"average_cpu":        metrics.AverageCPU,
		"average_latency_ms": metrics.AverageLatencyMS,
	})
	m.Reconcile(ctx, "autoscale")
}

func (m *Manager) SetBackendReplicas(ctx context.Context, replicas int) error {
	if replicas < m.cfg.BackendMinReplicas || replicas > m.cfg.BackendMaxReplicas {
		return fmt.Errorf("backend replicas must be between %d and %d", m.cfg.BackendMinReplicas, m.cfg.BackendMaxReplicas)
	}
	m.mu.Lock()
	m.enabled = true
	previous := m.desiredBackends
	m.desiredBackends = replicas
	m.lastScale = time.Now()
	m.mu.Unlock()

	m.publish("action", serviceBackend, "manual scale requested", map[string]any{"from": previous, "to": replicas})
	m.Reconcile(ctx, "manual-scale")
	return nil
}

func (m *Manager) RuntimeConfig() RuntimeConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return runtimeConfigFrom(m.cfg)
}

func runtimeConfigFrom(cfg Config) RuntimeConfig {
	return RuntimeConfig{
		HTTPAddr:            cfg.HTTPAddr,
		BackendProxyAddr:    cfg.BackendProxyAddr,
		ProjectName:         cfg.ProjectName,
		NetworkName:         cfg.NetworkName,
		DBImage:             cfg.DBImage,
		BackendImage:        cfg.BackendImage,
		FrontendImage:       cfg.FrontendImage,
		DBDataPath:          cfg.DBDataPath,
		FrontendHostPort:    cfg.FrontendHostPort,
		BackendCPUs:         cfg.BackendCPUs,
		BackendMinReplicas:  cfg.BackendMinReplicas,
		BackendMaxReplicas:  cfg.BackendMaxReplicas,
		ScaleUpCPU:          cfg.ScaleUpCPU,
		ScaleDownCPU:        cfg.ScaleDownCPU,
		ScaleUpLatencyMS:    cfg.ScaleUpLatency.Milliseconds(),
		ScaleDownLatencyMS:  cfg.ScaleDownLatency.Milliseconds(),
		ScaleCooldownMS:     cfg.ScaleCooldown.Milliseconds(),
		ReconcileIntervalMS: cfg.ReconcileInterval.Milliseconds(),
		HealthIntervalMS:    cfg.HealthInterval.Milliseconds(),
	}
}

func (m *Manager) UpdateRuntimeConfig(ctx context.Context, patch RuntimeConfigPatch) error {
	servicesToReplace := map[string]bool{}

	m.mu.Lock()
	cfg := m.cfg

	if patch.DBImage != nil && strings.TrimSpace(*patch.DBImage) != "" && cfg.DBImage != strings.TrimSpace(*patch.DBImage) {
		cfg.DBImage = strings.TrimSpace(*patch.DBImage)
		servicesToReplace[serviceDB] = true
	}
	if patch.BackendImage != nil && strings.TrimSpace(*patch.BackendImage) != "" && cfg.BackendImage != strings.TrimSpace(*patch.BackendImage) {
		cfg.BackendImage = strings.TrimSpace(*patch.BackendImage)
		servicesToReplace[serviceBackend] = true
	}
	if patch.FrontendImage != nil && strings.TrimSpace(*patch.FrontendImage) != "" && cfg.FrontendImage != strings.TrimSpace(*patch.FrontendImage) {
		cfg.FrontendImage = strings.TrimSpace(*patch.FrontendImage)
		servicesToReplace[serviceFrontend] = true
	}
	if patch.DBDataPath != nil && strings.TrimSpace(*patch.DBDataPath) != "" && cfg.DBDataPath != strings.TrimSpace(*patch.DBDataPath) {
		cfg.DBDataPath = strings.TrimSpace(*patch.DBDataPath)
		servicesToReplace[serviceDB] = true
	}
	if patch.FrontendHostPort != nil && strings.TrimSpace(*patch.FrontendHostPort) != "" && cfg.FrontendHostPort != strings.TrimSpace(*patch.FrontendHostPort) {
		cfg.FrontendHostPort = strings.TrimSpace(*patch.FrontendHostPort)
		servicesToReplace[serviceFrontend] = true
	}
	if patch.BackendCPUs != nil && cfg.BackendCPUs != *patch.BackendCPUs {
		cfg.BackendCPUs = clampFloat(*patch.BackendCPUs, 0, 128)
		servicesToReplace[serviceBackend] = true
	}
	if patch.BackendMinReplicas != nil {
		cfg.BackendMinReplicas = max(0, *patch.BackendMinReplicas)
	}
	if patch.BackendMaxReplicas != nil {
		cfg.BackendMaxReplicas = max(1, *patch.BackendMaxReplicas)
	}
	if cfg.BackendMinReplicas > cfg.BackendMaxReplicas {
		m.mu.Unlock()
		return fmt.Errorf("backend min replicas cannot exceed max replicas")
	}
	if m.desiredBackends < cfg.BackendMinReplicas {
		m.desiredBackends = cfg.BackendMinReplicas
	}
	if m.desiredBackends > cfg.BackendMaxReplicas {
		m.desiredBackends = cfg.BackendMaxReplicas
	}
	if patch.ScaleUpCPU != nil {
		cfg.ScaleUpCPU = clampFloat(*patch.ScaleUpCPU, 0, 1000)
	}
	if patch.ScaleDownCPU != nil {
		cfg.ScaleDownCPU = clampFloat(*patch.ScaleDownCPU, 0, 1000)
	}
	if patch.ScaleUpLatencyMS != nil {
		cfg.ScaleUpLatency = durationFromMS(*patch.ScaleUpLatencyMS, cfg.ScaleUpLatency)
	}
	if patch.ScaleDownLatencyMS != nil {
		cfg.ScaleDownLatency = durationFromMS(*patch.ScaleDownLatencyMS, cfg.ScaleDownLatency)
	}
	if patch.ScaleCooldownMS != nil {
		cfg.ScaleCooldown = durationFromMS(*patch.ScaleCooldownMS, cfg.ScaleCooldown)
	}
	if patch.ReconcileIntervalMS != nil {
		cfg.ReconcileInterval = durationFromMS(*patch.ReconcileIntervalMS, cfg.ReconcileInterval)
	}
	if patch.HealthIntervalMS != nil {
		cfg.HealthInterval = durationFromMS(*patch.HealthIntervalMS, cfg.HealthInterval)
	}

	m.cfg = cfg
	m.mu.Unlock()

	m.publish("action", "", "runtime config updated", map[string]any{"config": runtimeConfigFrom(cfg)})

	if len(servicesToReplace) > 0 {
		if servicesToReplace[serviceDB] {
			servicesToReplace[serviceBackend] = true
		}
		containers, err := m.managedContainers(ctx)
		if err != nil {
			return err
		}
		for _, container := range containers {
			service := container.Labels[labelService]
			if servicesToReplace[service] {
				m.removeContainer(ctx, service, container.ID, "runtime config changed")
			}
		}
	}

	m.Reconcile(ctx, "config-update")
	return nil
}

func durationFromMS(ms int64, fallback time.Duration) time.Duration {
	if ms <= 0 {
		return fallback
	}
	return time.Duration(ms) * time.Millisecond
}

func clampFloat(value, minValue, maxValue float64) float64 {
	return math.Max(minValue, math.Min(maxValue, value))
}

func nanoCPUs(cpus float64) int64 {
	if cpus <= 0 {
		return 0
	}
	return int64(cpus * 1_000_000_000)
}

func (m *Manager) getDesiredBackends() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.desiredBackends
}

func (m *Manager) isEnabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.enabled
}

func (m *Manager) StartAll(ctx context.Context) {
	m.mu.Lock()
	wasEnabled := m.enabled
	m.enabled = true
	if m.desiredBackends < m.cfg.BackendMinReplicas {
		m.desiredBackends = m.cfg.BackendMinReplicas
	}
	m.mu.Unlock()

	if !wasEnabled {
		m.publish("action", "", "orchestration started", nil)
	}
	m.Reconcile(ctx, "manual-start")
}

func (m *Manager) RemoveAll(ctx context.Context) error {
	m.mu.Lock()
	m.enabled = false
	m.mu.Unlock()

	containers, err := m.managedContainers(ctx)
	if err != nil {
		return err
	}
	for _, container := range containers {
		m.removeContainer(ctx, container.Labels[labelService], container.ID, "manual stop")
	}
	m.refreshContainers(ctx)
	m.refreshBackendRefs(ctx)
	m.publish("action", "", "all managed containers removed", nil)
	return nil
}

func (m *Manager) Snapshot() StatusSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.events.mu.Lock()
	history := append([]Event(nil), m.events.history...)
	m.events.mu.Unlock()

	return StatusSnapshot{
		Project:                m.cfg.ProjectName,
		Network:                m.cfg.NetworkName,
		Enabled:                m.enabled,
		Config:                 runtimeConfigFrom(m.cfg),
		Traffic:                m.traffic.Stats(),
		DesiredBackendReplicas: m.desiredBackends,
		BackendMinReplicas:     m.cfg.BackendMinReplicas,
		BackendMaxReplicas:     m.cfg.BackendMaxReplicas,
		Metrics:                m.metrics,
		Backends:               append([]BackendRef(nil), m.backends...),
		Containers:             append([]ContainerStatus(nil), m.containers...),
		Events:                 history,
	}
}

func (m *Manager) backendSnapshot() []BackendRef {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]BackendRef(nil), m.backends...)
}

func (m *Manager) NextBackend() (BackendRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	healthy := make([]BackendRef, 0, len(m.backends))
	for _, backend := range m.backends {
		if backend.Healthy && backend.IPAddress != "" {
			healthy = append(healthy, backend)
		}
	}
	if len(healthy) == 0 {
		return BackendRef{}, errors.New("no healthy backend replicas available")
	}
	backend := healthy[m.roundRobin%len(healthy)]
	m.roundRobin++
	return backend, nil
}

func (m *Manager) watchDockerEvents(ctx context.Context) {
	filters := map[string][]string{
		"label": {
			labelManaged + "=true",
			labelProject + "=" + m.cfg.ProjectName,
		},
		"type": {"container"},
	}
	filterJSON, _ := json.Marshal(filters)
	path := "/events?filters=" + url.QueryEscape(string(filterJSON))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		resp, err := m.docker.stream(ctx, path)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.publish("error", "", "docker event stream failed", map[string]any{"error": err.Error()})
			time.Sleep(2 * time.Second)
			continue
		}

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			var event struct {
				Status string `json:"status"`
				ID     string `json:"id"`
				Actor  struct {
					Attributes map[string]string `json:"Attributes"`
				} `json:"Actor"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				continue
			}
			service := event.Actor.Attributes[labelService]
			name := event.Actor.Attributes["name"]
			m.publish("docker", service, "docker event: "+event.Status, map[string]any{
				"id":   shortID(event.ID),
				"name": name,
			})

			switch event.Status {
			case "die", "oom", "kill", "stop", "destroy":
				go m.Reconcile(ctx, "docker-event:"+event.Status)
			}
		}
		_ = resp.Body.Close()

		if err := scanner.Err(); err != nil && ctx.Err() == nil {
			m.publish("error", "", "docker event scanner failed", map[string]any{"error": err.Error()})
			time.Sleep(2 * time.Second)
		}
	}
}

func (m *Manager) publish(kind, service, message string, details map[string]any) {
	event := Event{
		Type:    kind,
		Service: service,
		Message: message,
		Details: details,
	}
	m.events.Publish(event)
	m.logger.Info(message, "type", kind, "service", service, "details", details)
}

func containerName(container ContainerSummary) string {
	if len(container.Names) == 0 {
		return container.ID
	}
	return strings.TrimPrefix(container.Names[0], "/")
}

func shortID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

type LoadGenerator struct {
	manager *Manager
	logger  *slog.Logger
	events  *EventBroker

	mu        sync.Mutex
	config    TrafficConfig
	running   bool
	cancel    context.CancelFunc
	startedAt *time.Time
	sent      uint64
	ok        uint64
	failed    uint64
	inFlight  int
	slow      uint64
	bursts    uint64
	latencyMS float64
	lastError string
}

func NewLoadGenerator(manager *Manager, logger *slog.Logger, events *EventBroker) *LoadGenerator {
	return &LoadGenerator{
		manager: manager,
		logger:  logger,
		events:  events,
		config: TrafficConfig{
			RequestsPerSecond: 50,
			Concurrency:       100,
			RandomDelay:       true,
			RandomDelayChance: 20,
			MaxRandomDelayMS:  750,
			SlowLoris:         false,
			SlowLorisChance:   5,
			BurstDoS:          false,
			BurstChance:       5,
			BurstSize:         25,
			TimeoutMS:         8000,
			KeyPrefix:         "orch-load",
		},
	}
}

func (g *LoadGenerator) Start(cfg TrafficConfig) TrafficStats {
	cfg = normalizeTrafficConfig(cfg, g.config)

	g.mu.Lock()
	if g.cancel != nil {
		g.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	now := time.Now().UTC()
	g.config = cfg
	g.running = true
	g.cancel = cancel
	g.startedAt = &now
	g.sent = 0
	g.ok = 0
	g.failed = 0
	g.inFlight = 0
	g.slow = 0
	g.bursts = 0
	g.latencyMS = 0
	g.lastError = ""
	g.mu.Unlock()

	g.events.Publish(Event{Type: "action", Service: serviceBackend, Message: "orchestrator traffic generator started", Details: map[string]any{"config": cfg}})
	go g.run(ctx)
	return g.Stats()
}

func (g *LoadGenerator) Configure(cfg TrafficConfig) TrafficStats {
	cfg = normalizeTrafficConfig(cfg, g.config)
	g.mu.Lock()
	g.config = cfg
	g.mu.Unlock()
	return g.Stats()
}

func (g *LoadGenerator) Stop() TrafficStats {
	g.mu.Lock()
	if g.cancel != nil {
		g.cancel()
	}
	g.cancel = nil
	g.running = false
	g.mu.Unlock()

	g.events.Publish(Event{Type: "action", Service: serviceBackend, Message: "orchestrator traffic generator stopped"})
	return g.Stats()
}

func (g *LoadGenerator) Stats() TrafficStats {
	g.mu.Lock()
	defer g.mu.Unlock()

	stats := TrafficStats{
		Running:   g.running,
		Config:    g.config,
		StartedAt: g.startedAt,
		Sent:      g.sent,
		OK:        g.ok,
		Failed:    g.failed,
		InFlight:  g.inFlight,
		Slow:      g.slow,
		Bursts:    g.bursts,
		LastError: g.lastError,
	}
	if g.ok > 0 {
		stats.AverageLatencyMS = round2(g.latencyMS / float64(g.ok))
	}
	if g.startedAt != nil {
		elapsed := time.Since(*g.startedAt).Seconds()
		if elapsed > 0 {
			stats.ActualRPS = round2(float64(g.sent) / elapsed)
		}
	}
	return stats
}

func normalizeTrafficConfig(cfg, fallback TrafficConfig) TrafficConfig {
	if cfg.RequestsPerSecond <= 0 {
		cfg.RequestsPerSecond = fallback.RequestsPerSecond
	}
	cfg.RequestsPerSecond = min(cfg.RequestsPerSecond, 10000)
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = fallback.Concurrency
	}
	cfg.Concurrency = min(max(cfg.Concurrency, 1), 5000)
	cfg.RandomDelayChance = min(max(cfg.RandomDelayChance, 0), 100)
	if cfg.MaxRandomDelayMS < 0 {
		cfg.MaxRandomDelayMS = 0
	}
	cfg.SlowLorisChance = min(max(cfg.SlowLorisChance, 0), 100)
	cfg.BurstChance = min(max(cfg.BurstChance, 0), 100)
	if cfg.BurstSize <= 0 {
		cfg.BurstSize = fallback.BurstSize
	}
	cfg.BurstSize = min(max(cfg.BurstSize, 1), 500)
	if cfg.TimeoutMS <= 0 {
		cfg.TimeoutMS = fallback.TimeoutMS
	}
	cfg.TimeoutMS = min(max(cfg.TimeoutMS, 250), 60000)
	if strings.TrimSpace(cfg.KeyPrefix) == "" {
		cfg.KeyPrefix = fallback.KeyPrefix
	}
	return cfg
}

func (g *LoadGenerator) run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	carry := 0.0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cfg := g.configSnapshot()
			carry += float64(cfg.RequestsPerSecond) / 10
			count := int(carry)
			carry -= float64(count)
			for i := 0; i < count; i++ {
				if !g.tryStartRequest(cfg) {
					break
				}
			}
		}
	}
}

func (g *LoadGenerator) configSnapshot() TrafficConfig {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.config
}

func (g *LoadGenerator) tryStartRequest(cfg TrafficConfig) bool {
	g.mu.Lock()
	if !g.running || g.inFlight >= cfg.Concurrency {
		g.mu.Unlock()
		return false
	}
	g.sent++
	g.inFlight++
	g.mu.Unlock()

	go g.issueRequest(cfg)
	return true
}

func (g *LoadGenerator) issueRequest(cfg TrafficConfig) {
	start := time.Now()
	err := g.doRequest(cfg)
	latency := float64(time.Since(start).Microseconds()) / 1000

	g.mu.Lock()
	if err != nil {
		g.failed++
		g.lastError = err.Error()
	} else {
		g.ok++
		g.latencyMS += latency
	}
	g.inFlight--
	g.mu.Unlock()
}

func (g *LoadGenerator) doRequest(cfg TrafficConfig) error {
	if cfg.RandomDelay && chance(cfg.RandomDelayChance) && cfg.MaxRandomDelayMS > 0 {
		time.Sleep(time.Duration(rand.Intn(cfg.MaxRandomDelayMS)) * time.Millisecond)
	}
	if cfg.BurstDoS && chance(cfg.BurstChance) {
		g.mu.Lock()
		g.bursts++
		g.mu.Unlock()
		for i := 0; i < cfg.BurstSize; i++ {
			_ = g.normalRequest(cfg)
		}
		return nil
	}
	if cfg.SlowLoris && chance(cfg.SlowLorisChance) {
		g.mu.Lock()
		g.slow++
		g.mu.Unlock()
		return g.slowLorisRequest(cfg)
	}
	return g.normalRequest(cfg)
}

func (g *LoadGenerator) normalRequest(cfg TrafficConfig) error {
	backend, err := g.manager.NextBackend()
	if err != nil {
		return err
	}

	method, path, body := trafficRequest(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutMS)*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, "http://"+backend.IPAddress+":50052"+path, strings.NewReader(body))
	if err != nil {
		return err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 500 {
		return fmt.Errorf("backend %s returned HTTP %d", backend.Name, resp.StatusCode)
	}
	return nil
}

func trafficRequest(cfg TrafficConfig) (string, string, string) {
	key := fmt.Sprintf("%s-%d", cfg.KeyPrefix, rand.Intn(1000))
	switch roll := rand.Intn(100); {
	case roll < 35:
		return http.MethodGet, "/health", ""
	case roll < 65:
		return http.MethodGet, "/items", ""
	case roll < 85:
		return http.MethodPost, "/items", fmt.Sprintf(`{"key":%q,"value":%q}`, key, fmt.Sprintf("value-%d", time.Now().UnixNano()))
	case roll < 96:
		return http.MethodPut, "/items/" + url.PathEscape(key), fmt.Sprintf(`{"value":%q}`, fmt.Sprintf("updated-%d", time.Now().UnixNano()))
	default:
		return http.MethodDelete, "/items/" + url.PathEscape(key), ""
	}
}

func (g *LoadGenerator) slowLorisRequest(cfg TrafficConfig) error {
	backend, err := g.manager.NextBackend()
	if err != nil {
		return err
	}
	conn, err := net.DialTimeout("tcp", backend.IPAddress+":50052", time.Duration(cfg.TimeoutMS)*time.Millisecond)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Duration(cfg.TimeoutMS) * time.Millisecond))

	key := fmt.Sprintf("%s-slow-%d", cfg.KeyPrefix, time.Now().UnixNano())
	body := fmt.Sprintf(`{"key":%q,"value":"slow"}`, key)
	headers := fmt.Sprintf("POST /items HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", backend.IPAddress, len(body))
	if _, err := conn.Write([]byte(headers)); err != nil {
		return err
	}
	for _, ch := range []byte(body) {
		if _, err := conn.Write([]byte{ch}); err != nil {
			return err
		}
		time.Sleep(time.Duration(50+rand.Intn(150)) * time.Millisecond)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("slow request returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func chance(percent int) bool {
	return rand.Intn(100) < percent
}

type API struct {
	manager *Manager
	events  *EventBroker
	logger  *slog.Logger
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.health)
	mux.HandleFunc("/api/status", a.status)
	mux.HandleFunc("/api/config", a.config)
	mux.HandleFunc("/api/events", a.eventsStream)
	mux.HandleFunc("/api/traffic", a.traffic)
	mux.HandleFunc("/api/reconcile", a.reconcile)
	mux.HandleFunc("/api/start", a.start)
	mux.HandleFunc("/api/scale", a.scale)
	mux.HandleFunc("/api/stop", a.stop)
	return cors(mux)
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) status(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, a.manager.Snapshot())
}

func (a *API) config(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, a.manager.RuntimeConfig())
	case http.MethodPatch, http.MethodPost:
		var patch RuntimeConfigPatch
		if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if err := a.manager.UpdateRuntimeConfig(r.Context(), patch); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, a.manager.Snapshot())
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *API) traffic(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, a.manager.traffic.Stats())
	case http.MethodPost:
		var payload struct {
			Action string        `json:"action"`
			Config TrafficConfig `json:"config"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		switch payload.Action {
		case "start":
			writeJSON(w, http.StatusAccepted, a.manager.traffic.Start(payload.Config))
		case "configure":
			writeJSON(w, http.StatusOK, a.manager.traffic.Configure(payload.Config))
		case "stop":
			writeJSON(w, http.StatusOK, a.manager.traffic.Stop())
		default:
			writeError(w, http.StatusBadRequest, "action must be start, configure, or stop")
		}
	case http.MethodDelete:
		writeJSON(w, http.StatusOK, a.manager.traffic.Stop())
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *API) eventsStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	ch, cancel, history := a.events.Subscribe()
	defer cancel()

	for _, event := range history {
		writeSSE(w, event)
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-ch:
			writeSSE(w, event)
			flusher.Flush()
		}
	}
}

func (a *API) reconcile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	a.manager.Reconcile(r.Context(), "api")
	writeJSON(w, http.StatusAccepted, a.manager.Snapshot())
}

func (a *API) start(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	a.manager.StartAll(r.Context())
	writeJSON(w, http.StatusAccepted, a.manager.Snapshot())
}

func (a *API) scale(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var payload struct {
		BackendReplicas int `json:"backend_replicas"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := a.manager.SetBackendReplicas(r.Context(), payload.BackendReplicas); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, a.manager.Snapshot())
}

func (a *API) stop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := a.manager.RemoveAll(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, a.manager.Snapshot())
}

func writeSSE(w io.Writer, event Event) {
	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "event: %s\n", event.Type)
	fmt.Fprintf(w, "data: %s\n\n", data)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "content-type")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type BackendProxy struct {
	manager *Manager
	logger  *slog.Logger
}

func (p *BackendProxy) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend, err := p.manager.NextBackend()
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		target := &url.URL{Scheme: "http", Host: backend.IPAddress + ":50052"}
		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
			p.logger.Warn("backend proxy request failed", "backend", backend.Name, "err", err)
			writeError(w, http.StatusBadGateway, err.Error())
		}
		proxy.ServeHTTP(w, r)
	})
}

func main() {
	cfg := loadConfig()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	docker := NewDockerClient(cfg.DockerSocket)
	if err := docker.Ping(ctx); err != nil {
		logger.Error("docker daemon unavailable", "socket", cfg.DockerSocket, "err", err)
		os.Exit(1)
	}

	events := NewEventBroker(200)
	manager := NewManager(cfg, docker, logger, events)
	manager.Run(ctx)

	apiServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           (&API{manager: manager, events: events, logger: logger}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	proxyServer := &http.Server{
		Addr:              cfg.BackendProxyAddr,
		Handler:           (&BackendProxy{manager: manager, logger: logger}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go serveHTTP(&wg, logger, "api", apiServer)
	go serveHTTP(&wg, logger, "backend_proxy", proxyServer)

	<-ctx.Done()
	manager.traffic.Stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = apiServer.Shutdown(shutdownCtx)
	_ = proxyServer.Shutdown(shutdownCtx)
	wg.Wait()
}

func serveHTTP(wg *sync.WaitGroup, logger *slog.Logger, name string, server *http.Server) {
	defer wg.Done()
	logger.Info("http server starting", "name", name, "addr", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("http server stopped", "name", name, "err", err)
	}
}
