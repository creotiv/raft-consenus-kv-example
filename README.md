# Raft KV Example

A Python-based Key-Value service built on top of the Raft consensus algorithm.

## Overview

This project implements a distributed key-value store using the Raft consensus protocol. It provides a REST API for basic KV operations (SET, GET, DELETE) with strong consistency guarantees.

> ⚠️ Status: Educational/experimental. Snapshots & membership changes are TODO. Good enough to study and benchmark; not yet production‑ready.

## Features

- ✅ Raft core: leader election, heartbeats, conflict repair, majority commit rule
- ✅ Batched proposals with single fsync per batch
- ✅ Linearizable reads via ReadIndex barrier
- ✅ Durable storage:
  - `log.bin` (binary: MAGIC + [index, term, len, cmd])
  - atomic metadata (`meta.json`) with fsync
- ✅ Simple KV state machine: `SET key value`, `DEL key`, `GET`
- ⚠️ TODO: snapshots, InstallSnapshot RPC, joint‑consensus membership, log compaction

## Project structure
```
├── app/ # (HTTP client API – FastAPI) ← not described in article
├── configs/ # node1.yaml / node2.yaml / node3.yaml
├── raftkv/
│ ├── config.py # YAML loader + data dir init
│ ├── election.py # Election timer / RequestVote
│ ├── interfaces.py # Abstract boundaries
│ ├── logstore.py # Binary log with fsync
│ ├── models.py # Pydantic types for Raft RPCs/log
│ ├── node.py # Orchestrator (state, loops, propose path)
│ ├── readindex.py # Linearizable read barrier
│ ├── replication.py # AppendEntries, batching, commit rule
│ ├── rpc.py # RPC client + server handlers
│ ├── state.py # State controller + structured state logging
│ ├── statemachine.py # KV apply() + get()
│ ├── storage.py # Metadata (term/votedFor) with atomic replace
│ ├── membership.py # TODO
│ ├── snapshot.py # TODO
│ └── errors.py # TODO (prefer over raising HTTPException in core)
├── main.py # App bootstrap
├── Makefile # run1/run2/run3/run-all/test-k6
├── k6-bench.js # quick load test
└── pyproject.toml
```


## Quick Start

### Prerequisites

- Python 3.11+
- k6 (for load testing)

### Installation

1. Install Python dependencies:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
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

**`k6-bench.js`** - Quick test with 100 users for 30 sec

### Running Tests

#### Quick Test (Recommended for development)
```bash
make test-k6
```
