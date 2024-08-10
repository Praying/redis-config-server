package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/redis/go-redis/v9"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type SentinelCommand struct {
	Command string
	Args    []string
	Resp    chan string
}

const DEFAULT_DOWN_AFTER_MILLISECONDS int64 = 5000

func setupRaftNode(nodeID string, dataDir string, peers []raft.Server) (*raft.Raft, *raft.InmemTransport, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	// Setup Raft communication
	transportAddr, transport := raft.NewInmemTransport(raft.ServerAddress(nodeID))

	// Create Raft store
	logStore, err := raftboltdb.NewBoltStore(dataDir + "/raft-log.bolt")
	if err != nil {
		return nil, nil, err
	}

	stableStore, err := raftboltdb.NewBoltStore(dataDir + "/raft-stable.bolt")
	if err != nil {
		return nil, nil, err
	}

	snapshotStore := raft.NewDiscardSnapshotStore()

	// Create the Raft node
	ra, err := raft.NewRaft(config, &FSM{}, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, nil, err
	}

	// Bootstrap the cluster if necessary
	if len(peers) > 0 {
		configuration := raft.Configuration{
			Servers: peers,
		}
		ra.BootstrapCluster(configuration)
	}

	log.Printf("Raft node started at %s", transportAddr)

	return ra, transport, nil
}

// FSM (Finite State Machine) implementation for Raft
type FSM struct{}

func (f *FSM) Apply(log *raft.Log) interface{} {
	// 处理日志应用
	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (f *FSM) Restore(io.ReadCloser) error {
	return nil
}

func main() {
	// 解析 Sentinel 配置文件
	sentinelConfig, err := ParseSentinelConfig("sentinel.conf")
	if err != nil {
		log.Fatalf("Failed to parse Sentinel config: %v", err)
		return
	}
	// 初始化 Raft 节点
	peers := []raft.Server{
		{ID: "node1", Address: raft.ServerAddress("127.0.0.1:8001")},
		{ID: "node2", Address: raft.ServerAddress("127.0.0.1:8002")},
		{ID: "node3", Address: raft.ServerAddress("127.0.0.1:8003")},
	}
	raftNode, _, err := setupRaftNode("node1", "/tmp/raft1", peers)
	if err != nil {
		log.Fatalf("Failed to setup Raft node: %v", err)
	}

	// 初始化 ConfigServer
	cs := NewConfigServer(raftNode, sentinelConfig)
	err = cs.AddInstance("127.0.0.1:6379", "master", "", DEFAULT_DOWN_AFTER_MILLISECONDS)
	if err != nil {
		return
	}
	err = cs.AddInstance("127.0.0.1:6380", "slave", "127.0.0.1:6379", DEFAULT_DOWN_AFTER_MILLISECONDS)
	if err != nil {
		return
	}
	err = cs.AddInstance("127.0.0.1:6381", "slave", "127.0.0.1:6379", DEFAULT_DOWN_AFTER_MILLISECONDS)
	if err != nil {
		return
	}
	// Start the Redis protocol server
	go startRedisProtocolServer(cs, ":26379")

	// 启动监控
	go cs.MonitorInstances()

	// Setup signal handling to gracefully shut down the service
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for a termination signal
	sig := <-sigChan
	log.Printf("Received signal %v, shutting down...", sig)

	// Cleanup and shutdown procedures
	if err := raftNode.Shutdown().Error(); err != nil {
		log.Printf("Error shutting down Raft node: %v", err)
	}

	log.Println("Shutdown complete.")
}

func startRedisProtocolServer(cs *ConfigServer, address string) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()
	log.Printf("Listening for Redis protocol commands on %s...", address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go handleConnection(cs, conn)
	}
}

func handleConnection(cs *ConfigServer, conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		command, args, err := parseRedisCommand(reader)
		if err != nil {
			log.Printf("Failed to parse command: %v", err)
			return
		}

		response, err := cs.ProcessSentinelCommand(command, args...)
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("-ERR %v\r\n", err)))
		} else {
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", response)))
		}
	}
}

func parseRedisCommand(reader *bufio.Reader) (string, []string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", nil, err
	}
	if line[0] != '*' {
		return "", nil, fmt.Errorf("expected '*', got '%s'", line)
	}
	// Skip the number of arguments line
	_, err = reader.ReadString('\n')
	if err != nil {
		return "", nil, err
	}

	commandLine, err := reader.ReadString('\n')
	if err != nil {
		return "", nil, err
	}
	command := strings.TrimSpace(commandLine)

	var args []string
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		arg := strings.TrimSpace(line)
		if len(arg) == 0 {
			break
		}
		args = append(args, arg)
	}

	return command, args, nil
}

type RedisInstance struct {
	Address        string
	Role           string // "master" or "slave"
	MasterAddress  string // for slaves
	SlaveAddresses []string
	LastPingTime   time.Time
	DownAfterMs    int64
	IsDown         bool
	client         *redis.Client
}

// 初始化 Redis 客户端
func (r *RedisInstance) InitClient() {
	r.client = redis.NewClient(&redis.Options{
		Addr: r.Address,
	})
}

// Ping 实现 Redis 实例的健康检查
func (r *RedisInstance) Ping() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := r.client.Ping(ctx).Result()
	if err != nil {
		log.Printf("Failed to ping Redis instance at %s: %v", r.Address, err)
		r.IsDown = true
	} else {
		r.LastPingTime = time.Now()
		r.IsDown = false
	}

	return !r.IsDown
}

type ConfigServer struct {
	Raft           *raft.Raft
	RedisInstances map[string]*RedisInstance
	mu             sync.Mutex // Mutex for thread-safe access to RedisInstances map
	commandQueue   chan SentinelCommand
	StartTime      time.Time // Add this line to track uptime
}

func NewConfigServer(raftNode *raft.Raft, sentinelConfig *SentinelConfig) *ConfigServer {
	cs := &ConfigServer{
		Raft:           raftNode,
		RedisInstances: make(map[string]*RedisInstance),
		commandQueue:   make(chan SentinelCommand),
		StartTime:      time.Now(), // Initialize StartTime
	}

	// 初始化 Redis 实例
	for _, monitor := range sentinelConfig.Monitors {
		// 创建主节点实例
		masterKey := fmt.Sprintf("%s:%d", monitor.MasterAddress, monitor.MasterPort)
		masterInstance := &RedisInstance{
			Address:       monitor.MasterAddress,
			Role:          "master",
			MasterAddress: "",
			DownAfterMs:   monitor.DownAfterMilliseconds,
		}
		masterInstance.InitClient()
		cs.RedisInstances[masterKey] = masterInstance

		for _, slaveAddr := range monitor.SlaveAddresses {
			slaveInstance := &RedisInstance{
				Address:       slaveAddr,
				Role:          "slave",
				MasterAddress: masterKey,
				DownAfterMs:   monitor.DownAfterMilliseconds,
			}
			slaveInstance.InitClient() // 初始化 Redis 客户端
			cs.RedisInstances[slaveAddr] = slaveInstance
		}
	}

	return cs
}
func (cs *ConfigServer) AddInstance(address string, role string, masterAddr string, downAfterMs int64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Check if the instance already exists
	if _, exists := cs.RedisInstances[address]; exists {
		return fmt.Errorf("Redis instance with address %s already exists", address)
	}

	// Create a new RedisInstance
	newInstance := &RedisInstance{
		Address:       address,
		Role:          role,
		MasterAddress: masterAddr,
		DownAfterMs:   downAfterMs,
	}

	// Initialize the Redis client
	newInstance.InitClient()

	// Add the new instance to the map
	cs.RedisInstances[address] = newInstance

	log.Printf("Added Redis instance: %s, role: %s, master: %s", address, role, masterAddr)
	return nil
}
func (cs *ConfigServer) MonitorInstances() {
	for {
		for _, instance := range cs.RedisInstances {
			if instance.Ping() {
				instance.IsDown = false
				log.Printf("Instance %s is up", instance.Address)
			} else {
				instance.IsDown = true
				log.Printf("Instance %s is down", instance.Address)
				cs.HandleFailure(instance)
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func (cs *ConfigServer) runCommandProcessor() {
	for cmd := range cs.commandQueue {
		response, err := cs.ProcessSentinelCommand(cmd.Command, cmd.Args...)
		if err != nil {
			cmd.Resp <- fmt.Sprintf("ERROR: %v", err)
		} else {
			cmd.Resp <- response
		}
		close(cmd.Resp)
	}
}

func (cs *ConfigServer) ProcessSentinelCommand(command string, args ...string) (string, error) {
	command = strings.ToUpper(command)
	switch command {
	case "SENTINEL":
		if len(args) < 1 {
			return "", errors.New("ERR wrong number of arguments for 'SENTINEL' command")
		}
		subcommand := strings.ToUpper(args[0])
		// Remove the subcommand from the args list
		args = args[1:]

		switch subcommand {
		case "MONITOR":
			return cs.handleMonitor(args)
		case "RESET":
			return cs.handleReset(args)
		case "REMOVE":
			return cs.handleRemove(args)
		case "MASTERS":
			return cs.handleMasters(args)
		case "MASTER":
			return cs.handleMaster(args)
		case "INFO":
			return cs.handleInfo(args)
		default:
			return "", fmt.Errorf("ERR unknown subcommand '%s' for 'SENTINEL'", subcommand)
		}

	default:
		return "", fmt.Errorf("ERR unknown command '%s'", command)
	}
}

func (cs *ConfigServer) handleMonitor(args []string) (string, error) {
	if len(args) != 4 {
		return "", errors.New("ERR wrong number of arguments for 'SENTINEL MONITOR' command")
	}

	name := args[0]
	address := args[1]
	port := args[2]
	quorum := args[3]

	// Create the full address
	fullAddress := fmt.Sprintf("%s:%s", address, port)

	// Add the instance
	err := cs.AddInstance(fullAddress, "master", "", DEFAULT_DOWN_AFTER_MILLISECONDS)
	if err != nil {
		return "", fmt.Errorf("ERR failed to monitor instance: %v", err)
	}

	log.Printf("Monitoring new master: %s at %s with quorum %s", name, fullAddress, quorum)
	return "OK", nil
}

func (cs *ConfigServer) handleReset(args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("ERR wrong number of arguments for 'SENTINEL RESET' command")
	}

	name := args[0]

	// Reset logic - In this simplified version, we just log and return OK
	log.Printf("Resetting monitoring for: %s", name)
	return "OK", nil
}

func (cs *ConfigServer) handleRemove(args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("ERR wrong number of arguments for 'SENTINEL REMOVE' command")
	}

	address := args[0]

	cs.mu.Lock()
	defer cs.mu.Unlock()

	if _, exists := cs.RedisInstances[address]; !exists {
		return "", fmt.Errorf("ERR no such Redis instance: %s", address)
	}

	delete(cs.RedisInstances, address)
	log.Printf("Removed monitoring for Redis instance: %s", address)

	return "OK", nil
}

func (cs *ConfigServer) handleMasters(args []string) (string, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	var response strings.Builder

	// Iterate over all RedisInstances and collect information for master nodes
	for _, instance := range cs.RedisInstances {
		if instance.Role == "master" {
			response.WriteString(fmt.Sprintf("*11\r\n"))
			response.WriteString(fmt.Sprintf("$4\r\nname\r\n$%d\r\n%s\r\n", len(instance.Address), instance.Address))
			response.WriteString(fmt.Sprintf("$2\r\nip\r\n$%d\r\n%s\r\n", len(strings.Split(instance.Address, ":")[0]), strings.Split(instance.Address, ":")[0]))
			response.WriteString(fmt.Sprintf("$4\r\nport\r\n$%d\r\n%s\r\n", len(strings.Split(instance.Address, ":")[1]), strings.Split(instance.Address, ":")[1]))
			response.WriteString(fmt.Sprintf("$7\r\nquorum\r\n$1\r\n2\r\n"))
			response.WriteString(fmt.Sprintf("$13\r\nflags\r\n$%d\r\n%s\r\n", len(instance.Role), instance.Role))
			response.WriteString(fmt.Sprintf("$5\r\nlast-ping\r\n$%d\r\n%d\r\n", len(fmt.Sprintf("%d", instance.LastPingTime.Unix())), instance.LastPingTime.Unix()))
			response.WriteString(fmt.Sprintf("$12\r\nlast-ok-ping\r\n$%d\r\n%d\r\n", len(fmt.Sprintf("%d", instance.LastPingTime.Unix())), instance.LastPingTime.Unix()))
			response.WriteString(fmt.Sprintf("$10\r\nup-down-time\r\n$%d\r\n%d\r\n", len(fmt.Sprintf("%d", time.Now().Unix()-instance.LastPingTime.Unix())), time.Now().Unix()-instance.LastPingTime.Unix()))
			response.WriteString(fmt.Sprintf("$6\r\nstatus\r\n$%4\r\nup\r\n")) // Assuming the master is up if reachable
		}
	}

	return response.String(), nil
}

func (cs *ConfigServer) HandleFailure(instance *RedisInstance) {
	if instance.Role == "master" {
		newMaster := cs.SelectNewMaster()
		if newMaster != nil {
			cs.PromoteToMaster(newMaster)
			cs.UpdateSlaves(newMaster)
		}
	} else {
		log.Printf("Slave instance %s is down, skipping...", instance.Address)
	}
}

func (cs *ConfigServer) handleInfo(args []string) (string, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	var monitoredMasters int
	for _, instance := range cs.RedisInstances {
		if instance.Role == "master" {
			monitoredMasters++
		}
	}

	// Build the response in Redis protocol format
	var response strings.Builder
	response.WriteString("# Sentinel\r\n")
	response.WriteString(fmt.Sprintf("sentinel_masters:%d\r\n", monitoredMasters))
	// Add more fields as needed, for example:
	response.WriteString("sentinel_tilt:0\r\n")
	response.WriteString("sentinel_running_scripts:0\r\n")
	response.WriteString("sentinel_scripts_queue_length:0\r\n")
	response.WriteString("sentinel_simulate_failure_flags:0\r\n")
	response.WriteString(fmt.Sprintf("sentinel_rao_epoch:%d\r\n", cs.Raft.LastIndex()))
	response.WriteString(fmt.Sprintf("sentinel_current_epoch:%d\r\n", cs.Raft.LastIndex()))

	return response.String(), nil
}

func (cs *ConfigServer) handleMaster(args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("ERR wrong number of arguments for 'SENTINEL MASTER' command")
	}

	name := args[0]

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Find the master instance by name (address in this simplified example)
	instance, exists := cs.RedisInstances[name]
	if !exists || instance.Role != "master" {
		return "", fmt.Errorf("ERR no such master with name: %s", name)
	}

	// Build the response in Redis protocol format
	var response strings.Builder
	response.WriteString("*11\r\n")

	// Add fields for the master's information
	response.WriteString(fmt.Sprintf("$4\r\nname\r\n$%d\r\n%s\r\n", len(instance.Address), instance.Address))
	response.WriteString(fmt.Sprintf("$2\r\nip\r\n$%d\r\n%s\r\n", len(strings.Split(instance.Address, ":")[0]), strings.Split(instance.Address, ":")[0]))
	response.WriteString(fmt.Sprintf("$4\r\nport\r\n$%d\r\n%s\r\n", len(strings.Split(instance.Address, ":")[1]), strings.Split(instance.Address, ":")[1]))
	response.WriteString(fmt.Sprintf("$7\r\nquorum\r\n$1\r\n2\r\n"))
	response.WriteString(fmt.Sprintf("$13\r\nflags\r\n$%d\r\n%s\r\n", len(instance.Role), instance.Role))
	response.WriteString(fmt.Sprintf("$5\r\nlast-ping\r\n$%d\r\n%d\r\n", len(fmt.Sprintf("%d", instance.LastPingTime.Unix())), instance.LastPingTime.Unix()))
	response.WriteString(fmt.Sprintf("$12\r\nlast-ok-ping\r\n$%d\r\n%d\r\n", len(fmt.Sprintf("%d", instance.LastPingTime.Unix())), instance.LastPingTime.Unix()))
	response.WriteString(fmt.Sprintf("$10\r\nup-down-time\r\n$%d\r\n%d\r\n", len(fmt.Sprintf("%d", time.Now().Unix()-instance.LastPingTime.Unix())), time.Now().Unix()-instance.LastPingTime.Unix()))
	response.WriteString(fmt.Sprintf("$6\r\nstatus\r\n$4\r\nup\r\n")) // Assuming the master is up if reachable

	return response.String(), nil
}

func (cs *ConfigServer) SelectNewMaster() *RedisInstance {
	for _, instance := range cs.RedisInstances {
		if instance.Role == "slave" && !instance.IsDown {
			return instance
		}
	}
	return nil
}

func (cs *ConfigServer) PromoteToMaster(instance *RedisInstance) {
	instance.Role = "master"
	instance.MasterAddress = ""
	log.Printf("Instance %s promoted to master", instance.Address)

	// 通过 Raft 进行一致性写入
	future := cs.Raft.Apply([]byte(instance.Address), 5*time.Second)
	if err := future.Error(); err != nil {
		log.Printf("Error promoting master: %v", err)
	}
}

func (cs *ConfigServer) UpdateSlaves(newMaster *RedisInstance) {
	for _, instance := range cs.RedisInstances {
		if instance.Role == "slave" && !instance.IsDown {
			instance.MasterAddress = newMaster.Address
			log.Printf("Slave instance %s now replicating from new master %s", instance.Address, newMaster.Address)
		}
	}
}

type SentinelConfig struct {
	Port     int
	Monitors map[string]*RedisMonitor
}

type RedisMonitor struct {
	Name                  string
	MasterAddress         string
	MasterPort            int
	Quorum                int
	DownAfterMilliseconds int64
	FailoverTimeout       int64
	ParallelSyncs         int
	SlaveAddresses        []string
}

func ParseSentinelConfig(filePath string) (*SentinelConfig, error) {
	config := &SentinelConfig{
		Monitors: make(map[string]*RedisMonitor),
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "port") {
			fmt.Sscanf(line, "port %d", &config.Port)
		} else if strings.HasPrefix(line, "sentinel monitor") {
			var name, addr string
			var port, quorum int
			fmt.Sscanf(line, "sentinel monitor %s %s %d %d", &name, &addr, &port, &quorum)
			config.Monitors[name] = &RedisMonitor{
				Name:          name,
				MasterAddress: addr,
				MasterPort:    port,
				Quorum:        quorum,
			}
		} else if strings.HasPrefix(line, "sentinel down-after-milliseconds") {
			var name string
			var downAfterMs int64
			fmt.Sscanf(line, "sentinel down-after-milliseconds %s %d", &name, &downAfterMs)
			if monitor, exists := config.Monitors[name]; exists {
				monitor.DownAfterMilliseconds = downAfterMs
			}
		} else if strings.HasPrefix(line, "sentinel failover-timeout") {
			var name string
			var timeoutMs int64
			fmt.Sscanf(line, "sentinel failover-timeout %s %d", &name, &timeoutMs)
			if monitor, exists := config.Monitors[name]; exists {
				monitor.FailoverTimeout = timeoutMs
			}
		} else if strings.HasPrefix(line, "sentinel parallel-syncs") {
			var name string
			var parallelSyncs int
			fmt.Sscanf(line, "sentinel parallel-syncs %s %d", &name, &parallelSyncs)
			if monitor, exists := config.Monitors[name]; exists {
				monitor.ParallelSyncs = parallelSyncs
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return config, nil
}
