# Vistrit (विस्तृत)

**An Educational Distributed Computing Platform in Rust**

Vistrit (Sanskrit for "distributed/expanded") is a distributed computing platform designed for educational purposes. It features a custom binary protocol and demonstrates core distributed systems concepts.

## 🎯 Educational Goals

Learn about:
- **Binary Protocol Design** - Custom VistritProtocol with framing, versioning, and checksums
- **Async Networking** - Tokio-based TCP communication
- **Distributed Scheduling** - Task distribution across worker nodes
- **Fault Tolerance** - Heartbeats, timeouts, and retry logic
- **Leader Election** - Bully algorithm implementation
- **Serialization** - Efficient binary serialization with serde + bincode

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐
│   CLI / SDK     │────▶│   Coordinator   │
└─────────────────┘     └────────┬────────┘
                                 │
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
              ┌──────────┐ ┌──────────┐ ┌──────────┐
              │ Worker 1 │ │ Worker 2 │ │ Worker N │
              └──────────┘ └──────────┘ └──────────┘
```

## 📦 Crates

| Crate | Description |
|-------|-------------|
| `vistrit-protocol` | Custom binary protocol definitions and codec |
| `vistrit-core` | Shared types for nodes, tasks, and errors |
| `vistrit-coordinator` | Coordinator node implementation |
| `vistrit-worker` | Worker node implementation |
| `vistrit-client` | Client SDK and CLI tool |

## 🚀 Quick Start

### Build

```bash
cargo build --release
```

### Run Coordinator

```bash
cargo run --bin vistrit-coordinator -- --bind 0.0.0.0:7878
```

### Run Worker

```bash
cargo run --bin vistrit-worker -- --coordinator localhost:7878
```

### Submit Task

```bash
cargo run --bin vistrit-client -- submit --task "hello world"
```

## 📖 Documentation

- [Architecture Guide](docs/architecture.md)
- [Protocol Specification](docs/protocol.md)
- [Tutorials](docs/tutorials/)

## 📄 License

MIT License - See [LICENSE](LICENSE) for details.
