# shell-mesh

**Dynamic mesh networking for heterogeneous device fleets — ESP32 to cloud, self-organizing.**

A Rust library for building mesh topologies that detect their own structure, route messages, and adapt as nodes join and leave. Designed for the SuperInstance fleet: devices from microcontrollers to GPUs collaborating in real-time.

## What This Gives You

- **Self-organizing topology** — automatically detects star, full-mesh, hierarchical, or custom layouts
- **Typed device nodes** — ESP32, Jetson, Desktop, Server, Cloud — each with capabilities and status
- **Structured messaging** — Discovery, ResourceOffer, TaskRequest, TaskResult, Heartbeat
- **Serde-serializable** — every type derives `Serialize`/`Deserialize` for network transport

## Quick Start

```rust
use shell_mesh::{MeshNode, DeviceType, MeshTopology, MeshMessage, MessageType};

// Create nodes
let esp = MeshNode::new("esp-01", DeviceType::ESP32, "192.168.1.10")
    .with_capabilities(vec!["sensor", "gpio"]);
let server = MeshNode::new("srv-01", DeviceType::Server, "10.0.0.1")
    .with_capabilities(vec!["inference", "storage"])
    .with_children(vec!["esp-01"]);

// Detect topology from a set of nodes
let mut nodes = std::collections::HashMap::new();
nodes.insert(esp.id.clone(), esp);
nodes.insert(server.id.clone(), server);
let topo = MeshTopology::detect(&nodes);
// topo.kind == TopologyKind::Star (server is hub)

// Broadcast discovery
let msg = MeshMessage::discovery("esp-01");
assert!(msg.is_broadcast());

// Targeted task request
let task = MeshMessage::new("esp-01", Some("srv-01"), MessageType::TaskRequest, "run inference");
```

## API Reference

### MeshNode

| Method | Description |
|--------|-------------|
| `MeshNode::new(id, device_type, address)` | Create a node |
| `.with_capabilities(caps)` | Add capability tags |
| `.with_parent(id)` | Set parent node |
| `.with_children(ids)` | Set child nodes |
| `.touch()` | Update last-seen timestamp |
| `.last_seen_duration()` | Time since last heartbeat |

### MeshTopology

| Method | Description |
|--------|-------------|
| `MeshTopology::detect(&nodes)` | Auto-detect topology from a node map |
| `.kind` | `Star`, `FullMesh`, `Hierarchical`, or `Custom` |

### MeshMessage

| Variant | Purpose |
|---------|---------|
| `Discovery` | Broadcast node presence |
| `ResourceOffer` | Advertise available resources |
| `TaskRequest` | Ask another node to do work |
| `TaskResult` | Return completed work |
| `Heartbeat` | Keep-alive ping |
| `Tick` | Synchronized clock tick |

## How It Fits

shell-mesh is the networking layer for the [OpenConstruct](https://github.com/SuperInstance/OpenConstruct) fleet system:

- **[sunset-ecosystem](https://github.com/SuperInstance/sunset-ecosystem)** — Agent lifecycle and breeding on top of mesh nodes
- **[OpenConstruct](https://github.com/SuperInstance/OpenConstruct)** — One-command agent onboarding, fleet discovery via mesh

## Testing

```bash
cargo test
```

## Installation

```toml
[dependencies]
shell-mesh = { git = "https://github.com/SuperInstance/shell-mesh" }
```

## License

MIT

Part of the [SuperInstance](https://github.com/SuperInstance) ecosystem.
