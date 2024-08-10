
# redis-config-server

## Overview

**redis-config-server** is a custom high-availability management service designed to mimic the behavior of Redis Sentinel, with extended functionalities and enhanced stability. This server is built using Go and integrates with a Raft-based consensus algorithm to provide consistent and fault-tolerant monitoring of Redis master-slave setups.

redis-config-server can manage multiple Redis instances, handle failover scenarios, and respond to Redis Sentinel commands using the Redis protocol. It is a robust alternative for managing Redis clusters, offering better scalability and customization options.

## Features

- **High Availability Management:** Monitors Redis master and slave nodes, providing automated failover in case of node failure.
- **Raft Consensus Algorithm:** Ensures consistent state across multiple redis-config-server instances, offering fault tolerance and reliability.
- **Redis Protocol Support:** Compatible with Redis Sentinel commands such as `MONITOR`, `RESET`, `REMOVE`, `MASTERS`, `MASTER`, and `INFO`.
- **Custom Command Processing:** Easily extendable to support additional Redis commands.
- **Real-time Monitoring:** Continuously monitors the health of Redis instances and takes action based on their status.

## Installation

### Prerequisites

- Go 1.16 or higher
- Redis 5.0 or higher
- `go-redis/redis/v9` library for Go
- `hashicorp/raft` library for Go

### Clone the Repository

```bash
git clone https://github.com/Praying/redis-config-server.git
cd redis-config-server
```

### Build the Project

```bash
go build -o redis-config-server main.go
```

## Usage

### Starting the redis-config-server

To start the redis-config-server, run the following command:

```bash
./redis-config-server
```

### Sentinel Commands

redis-config-server supports several Redis Sentinel commands:

#### 1. `SENTINEL MONITOR`

Monitors a new master instance:

```bash
redis-cli -p 26379 SENTINEL MONITOR <master-name> <ip> <port> <quorum>
```

#### 2. `SENTINEL RESET`

Resets monitoring for a specific master:

```bash
redis-cli -p 26379 SENTINEL RESET <master-name>
```

#### 3. `SENTINEL REMOVE`

Removes a master from monitoring:

```bash
redis-cli -p 26379 SENTINEL REMOVE <master-name>
```

#### 4. `SENTINEL MASTERS`

Lists all monitored master instances:

```bash
redis-cli -p 26379 SENTINEL MASTERS
```

#### 5. `SENTINEL MASTER <master-name>`

Gets detailed information about a specific master instance:

```bash
redis-cli -p 26379 SENTINEL MASTER <master-name>
```

#### 6. `SENTINEL INFO`

Provides general information about the redis-config-server instance:

```bash
redis-cli -p 26379 SENTINEL INFO
```

### Configuration

The server configuration is typically done via a Sentinel configuration file (`sentinel.conf`). Ensure that your Redis instances and configurations are properly set up before starting the redis-config-server.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue on GitHub.

### Contribution Guidelines

- Fork the repository.
- Create a new branch (`git checkout -b feature-branch`).
- Commit your changes (`git commit -m "Add new feature"`).
- Push to the branch (`git push origin feature-branch`).
- Open a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [HashiCorp Raft](https://github.com/hashicorp/raft) for the consensus algorithm.
- [go-redis](https://github.com/go-redis/redis) for the Redis client library.
- Redis for being a powerful in-memory data structure store.

---

This README provides a comprehensive overview of your project, instructions for setting it up, and details on how to use and contribute to the project. You can customize this further based on specific needs or additional features you implement.
