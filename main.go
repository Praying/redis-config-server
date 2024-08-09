package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/redis/go-redis/v9"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

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
	cs.AddInstance("127.0.0.1:6379", "master", "")
	cs.AddInstance("127.0.0.1:6380", "slave", "127.0.0.1:6379")
	cs.AddInstance("127.0.0.1:6381", "slave", "127.0.0.1:6379")

	// 启动监控
	go cs.MonitorInstances()

	// 阻塞主线程
	select {}
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
}

func NewConfigServer(raftNode *raft.Raft, sentinelConfig *SentinelConfig) *ConfigServer {
	cs := &ConfigServer{
		Raft:           raftNode,
		RedisInstances: make(map[string]*RedisInstance),
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
		masterInstance.InitClient() // 初始化 Redis 客户端
		cs.RedisInstances[masterKey] = masterInstance

		// 假设从节点信息会在真实环境中从其他地方获取并填充
		// 这里可以根据需要创建并初始化从节点实例
		// 示例: 创建从节点
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
func (cs *ConfigServer) AddInstance(address string, role string, masterAddr string) {
	cs.RedisInstances[address] = &RedisInstance{
		Address:       address,
		Role:          role,
		MasterAddress: masterAddr,
		DownAfterMs:   5000,
	}
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
