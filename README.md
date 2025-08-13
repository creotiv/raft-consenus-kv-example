# Raft KV Example

A Python-based Key-Value service built on top of the Raft consensus algorithm.

## Overview

This project implements a distributed key-value store using the Raft consensus protocol. It provides a REST API for basic KV operations (SET, GET, DELETE) with strong consistency guarantees.

## Architecture

- **Raft Consensus**: Implements leader election, log replication, and safety properties
- **HTTP API**: RESTful interface for KV operations
- **Multi-node Support**: Configurable cluster with multiple nodes
- **Linearizable Reads**: Support for strongly consistent read operations

## Quick Start

### Prerequisites

- Python 3.8+
- k6 (for load testing)

### Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Install k6 (for load testing):
```bash
# macOS
brew install k6

# Ubuntu/Debian
sudo gpg -k
sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update
sudo apt-get install k6

# Or download from https://k6.io/docs/getting-started/installation/
```

### Running the Service

1. **Start a single node:**
```bash
make run1  # Runs on port 7101
make run2  # Runs on port 7102
make run3  # Runs on port 7103
```

2. **Start all nodes:**
```bash
make run-all
```

### API Usage

The service exposes a REST API at `/kv`:

- **SET**: `POST /kv/set` - Set a key-value pair
- **GET**: `GET /kv/get/{key}` - Get a value by key
- **DELETE**: `DELETE /kv/del/{key}` - Delete a key
- **Health**: `GET /health` - Node health and status

Example:
```bash
# Set a key-value pair
curl -X POST localhost:7102/kv/set \
  -d '{"key":"a","value":"5"}' \
  -H "Content-Type: application/json"

# Get a value
curl localhost:7102/kv/get/a

# Delete a key
curl -X DELETE localhost:7102/kv/del/a
```

## Load Testing with k6

This project includes comprehensive k6 load testing scripts to test the KV service under various load conditions.

### Test Scripts

1. **`k6-simple.js`** - Quick test with 5 users for 1 minute
2. **`k6.js`** - Full load test with ramping users (10 → 20 → 0) over 16 minutes

### Running Tests

#### Quick Test (Recommended for development)
```bash
make test-k6
```

### Test Features

- **Random Key-Value Generation**: Each request uses unique, randomly generated keys and values
- **Parallel Users**: Tests with multiple concurrent users to simulate real-world load
- **Comprehensive Metrics**: Tracks latency, success rates, and error rates
- **Configurable Load**: Adjustable user counts and test durations
- **Realistic Patterns**: Random delays between requests to simulate human behavior

### Test Configuration

The full test (`k6.js`) includes:
- **Ramp-up Phase**: Gradually increase from 0 to 10 users over 2 minutes
- **Sustained Load**: Maintain 10 users for 5 minutes
- **Peak Load**: Ramp up to 20 users over 2 minutes
- **Peak Sustained**: Maintain 20 users for 5 minutes
- **Ramp-down**: Gradually decrease to 0 users over 2 minutes

### Performance Thresholds

Tests include performance thresholds:
- 95% of requests should complete within 2 seconds
- Error rate should be below 10%
- Custom error tracking for detailed analysis

### Customizing Tests

You can modify the test scripts to:
- Change the number of virtual users
- Adjust test duration
- Modify the target service URL
- Add custom metrics and checks
- Implement different load patterns
