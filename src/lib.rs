use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// MeshNode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DeviceType {
    ESP32,
    Jetson,
    Desktop,
    Server,
    Cloud,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Online,
    Offline,
    Busy,
}

#[derive(Debug, Clone, Serialize)]
pub struct MeshNode {
    pub id: String,
    pub device_type: DeviceType,
    pub capabilities: Vec<String>,
    pub address: String,
    pub parent_id: Option<String>,
    pub children_ids: Vec<String>,
    pub status: NodeStatus,
    #[serde(skip)]
    pub last_seen: Arc<Mutex<Instant>>,
}

impl MeshNode {
    pub fn new(id: &str, device_type: DeviceType, address: &str) -> Self {
        Self {
            id: id.to_string(),
            device_type,
            capabilities: Vec::new(),
            address: address.to_string(),
            parent_id: None,
            children_ids: Vec::new(),
            status: NodeStatus::Online,
            last_seen: Arc::new(Mutex::new(Instant::now())),
        }
    }

    // Helper for deserialization tests — not serialized, so reconstruct
    fn init_last_seen(&mut self) {
        self.last_seen = Arc::new(Mutex::new(Instant::now()));
    }

    pub fn with_capabilities(mut self, caps: Vec<&str>) -> Self {
        self.capabilities = caps.into_iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn with_parent(mut self, parent_id: &str) -> Self {
        self.parent_id = Some(parent_id.to_string());
        self
    }

    pub fn with_children(mut self, children: Vec<&str>) -> Self {
        self.children_ids = children.into_iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn touch(&self) {
        if let Ok(mut t) = self.last_seen.lock() {
            *t = Instant::now();
        }
    }

    pub fn last_seen_duration(&self) -> Duration {
        self.last_seen
            .lock()
            .map(|t| t.elapsed())
            .unwrap_or(Duration::from_secs(u64::MAX))
    }
}

// ---------------------------------------------------------------------------
// MeshMessage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MessageType {
    Discovery,
    ResourceOffer,
    TaskRequest,
    TaskResult,
    Tick,
    Heartbeat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshMessage {
    pub from: String,
    pub to: Option<String>, // None = broadcast
    pub message_type: MessageType,
    pub payload: String,
    pub timestamp: i64,
}

impl MeshMessage {
    pub fn new(from: &str, to: Option<&str>, message_type: MessageType, payload: &str) -> Self {
        Self {
            from: from.to_string(),
            to: to.map(|s| s.to_string()),
            message_type,
            payload: payload.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    pub fn discovery(from: &str) -> Self {
        Self::new(from, None, MessageType::Discovery, "discover")
    }

    pub fn heartbeat(from: &str) -> Self {
        Self::new(from, None, MessageType::Heartbeat, "heartbeat")
    }

    pub fn is_broadcast(&self) -> bool {
        self.to.is_none()
    }
}

// ---------------------------------------------------------------------------
// MeshTopology
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TopologyKind {
    Star,
    FullMesh,
    Hierarchical,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshTopology {
    pub kind: TopologyKind,
    pub nodes: Vec<String>,
    pub edges: Vec<(String, String)>,
}

impl MeshTopology {
    pub fn detect(nodes: &HashMap<String, MeshNode>) -> Self {
        let node_ids: Vec<String> = nodes.keys().cloned().collect();
        let n = node_ids.len();

        if n <= 1 {
            return Self {
                kind: TopologyKind::Star,
                nodes: node_ids,
                edges: Vec::new(),
            };
        }

        // Build edge list from parent/children relationships
        let mut edges: Vec<(String, String)> = Vec::new();
        for node in nodes.values() {
            for child in &node.children_ids {
                if nodes.contains_key(child) {
                    edges.push((node.id.clone(), child.clone()));
                }
            }
            if let Some(ref parent) = node.parent_id {
                if nodes.contains_key(parent) {
                    let e = (parent.clone(), node.id.clone());
                    if !edges.contains(&e) {
                        edges.push(e);
                    }
                }
            }
        }

        let kind = if Self::is_star(nodes) {
            TopologyKind::Star
        } else if Self::is_full_mesh(nodes) {
            TopologyKind::FullMesh
        } else if Self::is_hierarchical(nodes) {
            TopologyKind::Hierarchical
        } else {
            TopologyKind::Custom
        };

        Self {
            kind,
            nodes: node_ids,
            edges,
        }
    }

    fn is_star(nodes: &HashMap<String, MeshNode>) -> bool {
        // One hub node with many children, children have no children
        let hubs: Vec<&MeshNode> = nodes
            .values()
            .filter(|n| !n.children_ids.is_empty())
            .collect();

        if hubs.len() != 1 {
            return false;
        }

        let hub = hubs[0];
        // All children should have no children of their own
        for child_id in &hub.children_ids {
            if let Some(child) = nodes.get(child_id) {
                if !child.children_ids.is_empty() {
                    return false;
                }
            }
        }
        true
    }

    fn is_full_mesh(nodes: &HashMap<String, MeshNode>) -> bool {
        // Every node has every other node as a child
        let ids: Vec<&String> = nodes.keys().collect();
        let n = ids.len();
        for node in nodes.values() {
            // Each node should be connected to all others
            let connected: std::collections::HashSet<&String> =
                node.children_ids.iter().chain(node.parent_id.iter()).collect();
            let expected = n - 1;
            if connected.len() < expected {
                return false;
            }
        }
        true
    }

    fn is_hierarchical(nodes: &HashMap<String, MeshNode>) -> bool {
        // Has a root (no parent), intermediate nodes, and leaves
        let roots: Vec<&MeshNode> = nodes
            .values()
            .filter(|n| n.parent_id.is_none() && !n.children_ids.is_empty())
            .collect();

        if roots.is_empty() {
            return false;
        }

        // Check there are intermediate nodes (have both parent and children)
        let intermediates: Vec<&MeshNode> = nodes
            .values()
            .filter(|n| n.parent_id.is_some() && !n.children_ids.is_empty())
            .collect();

        // At least one intermediate for a hierarchy beyond star
        !intermediates.is_empty()
    }
}

// ---------------------------------------------------------------------------
// RoutingTable
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct RoutingTable {
    // destination -> (next_hop, path)
    routes: HashMap<String, (String, Vec<String>)>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn rebuild(&mut self, nodes: &HashMap<String, MeshNode>, local_id: &str) {
        self.routes.clear();

        // Build adjacency list
        let mut adj: HashMap<String, Vec<String>> = HashMap::new();
        for node in nodes.values() {
            adj.entry(node.id.clone()).or_default();
            for child in &node.children_ids {
                if nodes.contains_key(child) {
                    adj.entry(node.id.clone()).or_default().push(child.clone());
                    adj.entry(child.clone()).or_default().push(node.id.clone());
                }
            }
        }

        // BFS from local_id
        let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut queue: std::collections::VecDeque<(String, Vec<String>)> =
            std::collections::VecDeque::new();

        visited.insert(local_id.to_string());
        queue.push_back((local_id.to_string(), vec![local_id.to_string()]));

        while let Some((current, path)) = queue.pop_front() {
            if let Some(neighbors) = adj.get(&current) {
                for neighbor in neighbors {
                    if visited.insert(neighbor.clone()) {
                        let mut new_path = path.clone();
                        new_path.push(neighbor.clone());
                        let next_hop = if path.len() >= 2 {
                            path[1].clone()
                        } else {
                            neighbor.clone()
                        };
                        self.routes
                            .insert(neighbor.clone(), (next_hop, new_path.clone()));
                        queue.push_back((neighbor.clone(), new_path));
                    }
                }
            }
        }
    }

    pub fn route_to(&self, target: &str) -> Option<Vec<String>> {
        self.routes.get(target).map(|(_, path)| path.clone())
    }

    pub fn next_hop(&self, target: &str) -> Option<&str> {
        self.routes.get(target).map(|(hop, _)| hop.as_str())
    }
}

// ---------------------------------------------------------------------------
// MeshEvent
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum MeshEvent {
    NodeJoined {
        node: MeshNode,
    },
    NodeLeft {
        node_id: String,
    },
    TaskCompleted {
        task_id: String,
        result: String,
    },
    ResourceAvailable {
        node_id: String,
        resources: Vec<String>,
    },
    MessageReceived {
        message: MeshMessage,
    },
}

// ---------------------------------------------------------------------------
// ShellMesh
// ---------------------------------------------------------------------------

pub type EventCallback = Box<dyn Fn(MeshEvent) + Send + Sync>;

pub struct ShellMesh {
    local: MeshNode,
    nodes: Arc<Mutex<HashMap<String, MeshNode>>>,
    routing: Arc<Mutex<RoutingTable>>,
    event_callback: Arc<Mutex<Option<EventCallback>>>,
    inbox: Arc<Mutex<Vec<MeshMessage>>>,
    joined: Arc<Mutex<bool>>,
}

impl ShellMesh {
    pub fn new(local: MeshNode) -> Self {
        let id = local.id.clone();
        let mut nodes = HashMap::new();
        nodes.insert(id.clone(), local.clone());

        let mut routing = RoutingTable::new();
        routing.rebuild(&nodes, &id);

        Self {
            local,
            nodes: Arc::new(Mutex::new(nodes)),
            routing: Arc::new(Mutex::new(routing)),
            event_callback: Arc::new(Mutex::new(None)),
            inbox: Arc::new(Mutex::new(Vec::new())),
            joined: Arc::new(Mutex::new(false)),
        }
    }

    pub fn join(&self, bootstrap_addr: &str) -> Result<MeshTopology, String> {
        // Simulate joining a mesh via bootstrap address
        // In a real implementation this would contact the bootstrap node
        *self.joined.lock().unwrap() = true;

        // Simulate discovering the bootstrap node
        let bootstrap_node = MeshNode::new(
            bootstrap_addr,
            DeviceType::Jetson,
            bootstrap_addr,
        );
        self.add_node(bootstrap_node);

        let topology = self.topology();
        Ok(topology)
    }

    pub fn leave(&self) -> Result<(), String> {
        let mut nodes = self.nodes.lock().unwrap();
        let local_id = self.local.id.clone();

        // Remove self from parents' children lists
        if let Some(ref parent_id) = self.local.parent_id {
            if let Some(parent) = nodes.get_mut(parent_id) {
                parent.children_ids.retain(|c| c != &local_id);
            }
        }

        // Remove self from children's parent references
        for child_id in &self.local.children_ids {
            if let Some(child) = nodes.get_mut(child_id) {
                child.parent_id = None;
            }
        }

        nodes.remove(&local_id);
        *self.joined.lock().unwrap() = false;

        self.routing.lock().unwrap().rebuild(&nodes, &local_id);
        Ok(())
    }

    pub fn send(&self, target: &str, message: MeshMessage) -> Result<(), String> {
        let nodes = self.nodes.lock().unwrap();
        if !nodes.contains_key(target) {
            return Err(format!("Target node '{}' not found", target));
        }
        drop(nodes);

        self.inbox.lock().unwrap().push(message);
        Ok(())
    }

    pub fn broadcast(&self, message: MeshMessage) -> Result<(), String> {
        let nodes = self.nodes.lock().unwrap();
        let count = nodes.len();
        drop(nodes);

        self.inbox.lock().unwrap().push(message);
        Ok(())
    }

    pub fn on_event(&self, callback: EventCallback) {
        *self.event_callback.lock().unwrap() = Some(callback);
    }

    fn emit_event(&self, event: MeshEvent) {
        if let Some(cb) = self.event_callback.lock().unwrap().as_ref() {
            cb(event);
        }
    }

    pub fn discover_neighbors(&self) -> Vec<MeshNode> {
        let nodes = self.nodes.lock().unwrap();
        nodes
            .values()
            .filter(|n| n.id != self.local.id)
            .cloned()
            .collect()
    }

    pub fn route_to(&self, target: &str) -> Vec<String> {
        self.routing
            .lock()
            .unwrap()
            .route_to(target)
            .unwrap_or_default()
    }

    pub fn delegate_task(&self, task: &str, requirements: Vec<&str>) -> Option<String> {
        let nodes = self.nodes.lock().unwrap();

        let req_set: std::collections::HashSet<&str> = requirements.into_iter().collect();

        // Find the best node: one that has all required capabilities and is Online
        let mut best: Option<&MeshNode> = None;
        let mut best_score = usize::MAX;

        for node in nodes.values() {
            if node.id == self.local.id || node.status != NodeStatus::Online {
                continue;
            }
            let caps: std::collections::HashSet<&str> =
                node.capabilities.iter().map(|s| s.as_str()).collect();
            if req_set.iter().all(|r| caps.contains(r)) {
                // Prefer fewer extra capabilities (more specialized) as tiebreaker
                let extra = caps.len() - req_set.len();
                if extra < best_score {
                    best_score = extra;
                    best = Some(node);
                }
            }
        }

        best.map(|n| n.id.clone())
    }

    pub fn add_node(&self, node: MeshNode) {
        let id = node.id.clone();
        let mut nodes = self.nodes.lock().unwrap();
        nodes.insert(id.clone(), node.clone());

        // Update parent's children list
        if let Some(ref parent_id) = node.parent_id {
            if let Some(parent) = nodes.get_mut(parent_id) {
                if !parent.children_ids.contains(&id) {
                    parent.children_ids.push(id.clone());
                }
            }
        }

        // Update children's parent
        for child_id in &node.children_ids {
            if let Some(child) = nodes.get_mut(child_id) {
                child.parent_id = Some(id.clone());
            }
        }

        let local_id = self.local.id.clone();
        self.routing.lock().unwrap().rebuild(&nodes, &local_id);
        drop(nodes);

        self.emit_event(MeshEvent::NodeJoined { node });
    }

    pub fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.lock().unwrap();

        // Clean up references
        if let Some(node) = nodes.remove(node_id) {
            if let Some(ref parent_id) = node.parent_id {
                if let Some(parent) = nodes.get_mut(parent_id) {
                    parent.children_ids.retain(|c| c != node_id);
                }
            }
            for child_id in &node.children_ids {
                if let Some(child) = nodes.get_mut(child_id) {
                    child.parent_id = None;
                }
            }
        }

        let local_id = self.local.id.clone();
        self.routing.lock().unwrap().rebuild(&nodes, &local_id);
        drop(nodes);

        self.emit_event(MeshEvent::NodeLeft {
            node_id: node_id.to_string(),
        });
    }

    pub fn topology(&self) -> MeshTopology {
        let nodes = self.nodes.lock().unwrap();
        MeshTopology::detect(&nodes)
    }

    pub fn heartbeat(&self) {
        self.local.touch();

        let nodes = self.nodes.lock().unwrap();
        for node in nodes.values() {
            node.touch();
        }
    }

    pub fn local_node(&self) -> &MeshNode {
        &self.local
    }

    pub fn is_joined(&self) -> bool {
        *self.joined.lock().unwrap()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.lock().unwrap().len()
    }

    pub fn inbox_len(&self) -> usize {
        self.inbox.lock().unwrap().len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_mesh() -> ShellMesh {
        let local = MeshNode::new("local", DeviceType::Desktop, "127.0.0.1:9000");
        ShellMesh::new(local)
    }

    #[test]
    fn test_new_creates_mesh_with_local_node() {
        let mesh = make_test_mesh();
        assert_eq!(mesh.node_count(), 1);
        assert_eq!(mesh.local_node().id, "local");
        assert_eq!(mesh.local_node().device_type, DeviceType::Desktop);
    }

    #[test]
    fn test_join_discovers_existing_mesh() {
        let mesh = make_test_mesh();
        let topo = mesh.join("192.168.1.10:9000").unwrap();
        assert!(mesh.is_joined());
        assert!(topo.nodes.contains(&"192.168.1.10:9000".to_string()));
    }

    #[test]
    fn test_send_delivers_to_specific_node() {
        let mesh = make_test_mesh();
        let target = MeshNode::new("target", DeviceType::Jetson, "10.0.0.1:9000");
        mesh.add_node(target);

        let msg = MeshMessage::new("local", Some("target"), MessageType::TaskRequest, "do work");
        let result = mesh.send("target", msg);
        assert!(result.is_ok());
        assert_eq!(mesh.inbox_len(), 1);
    }

    #[test]
    fn test_send_to_unknown_node_fails() {
        let mesh = make_test_mesh();
        let msg = MeshMessage::new("local", Some("ghost"), MessageType::TaskRequest, "???");
        let result = mesh.send("ghost", msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_broadcast_reaches_all_nodes() {
        let mesh = make_test_mesh();
        mesh.add_node(MeshNode::new("a", DeviceType::ESP32, "10.0.0.2:9000"));
        mesh.add_node(MeshNode::new("b", DeviceType::ESP32, "10.0.0.3:9000"));

        let msg = MeshMessage::new("local", None, MessageType::Discovery, "hello all");
        let result = mesh.broadcast(msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_discover_neighbors_returns_connected() {
        let mesh = make_test_mesh();
        mesh.add_node(MeshNode::new("a", DeviceType::ESP32, "10.0.0.2:9000"));
        mesh.add_node(MeshNode::new("b", DeviceType::Jetson, "10.0.0.3:9000"));

        let neighbors = mesh.discover_neighbors();
        assert_eq!(neighbors.len(), 2);
        let ids: Vec<&str> = neighbors.iter().map(|n| n.id.as_str()).collect();
        assert!(ids.contains(&"a"));
        assert!(ids.contains(&"b"));
    }

    #[test]
    fn test_route_to_finds_path_in_star() {
        let mesh = make_test_mesh();
        // Hub = local, children = esp1, esp2
        let local = mesh.local_node().clone();
        let mut esp1 = MeshNode::new("esp1", DeviceType::ESP32, "10.0.0.2:9000");
        esp1.parent_id = Some("local".to_string());
        let mut esp2 = MeshNode::new("esp2", DeviceType::ESP32, "10.0.0.3:9000");
        esp2.parent_id = Some("local".to_string());

        mesh.add_node(esp1);
        mesh.add_node(esp2);

        // Rebuild with correct parent-child
        let mut nodes = mesh.nodes.lock().unwrap();
        nodes.get_mut("local").unwrap().children_ids = vec!["esp1".into(), "esp2".into()];
        drop(nodes);

        let mut routing = mesh.routing.lock().unwrap();
        routing.rebuild(&mesh.nodes.lock().unwrap(), "local");
        drop(routing);

        let path = mesh.route_to("esp2");
        assert!(!path.is_empty());
        assert_eq!(*path.last().unwrap(), "esp2");
    }

    #[test]
    fn test_route_to_finds_path_in_mesh() {
        let mesh = make_test_mesh();
        // Full mesh: local <-> a <-> b, local <-> b
        let mut a = MeshNode::new("a", DeviceType::Desktop, "10.0.0.2:9000");
        a.children_ids = vec!["local".into(), "b".into()];
        let mut b = MeshNode::new("b", DeviceType::Desktop, "10.0.0.3:9000");
        b.children_ids = vec!["local".into(), "a".into()];

        mesh.add_node(a);
        mesh.add_node(b);

        let mut nodes = mesh.nodes.lock().unwrap();
        nodes
            .get_mut("local")
            .unwrap()
            .children_ids = vec!["a".into(), "b".into()];
        drop(nodes);

        let mut routing = mesh.routing.lock().unwrap();
        routing.rebuild(&mesh.nodes.lock().unwrap(), "local");
        drop(routing);

        let path = mesh.route_to("b");
        assert!(!path.is_empty());
        assert_eq!(*path.last().unwrap(), "b");
    }

    #[test]
    fn test_delegate_task_finds_capable_node() {
        let mesh = make_test_mesh();
        let node = MeshNode::new("worker", DeviceType::Jetson, "10.0.0.2:9000")
            .with_capabilities(vec!["gpu", "ml"]);
        mesh.add_node(node);

        let result = mesh.delegate_task("train model", vec!["gpu", "ml"]);
        assert_eq!(result, Some("worker".to_string()));
    }

    #[test]
    fn test_delegate_task_returns_none_if_no_capable_node() {
        let mesh = make_test_mesh();
        let node = MeshNode::new("weak", DeviceType::ESP32, "10.0.0.2:9000")
            .with_capabilities(vec!["sensor"]);
        mesh.add_node(node);

        let result = mesh.delegate_task("train model", vec!["gpu", "ml"]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_topology_detection_star() {
        let mut nodes = HashMap::new();
        let hub = MeshNode::new("hub", DeviceType::Jetson, "10.0.0.1:9000")
            .with_children(vec!["esp1", "esp2", "esp3"]);
        let esp1 =
            MeshNode::new("esp1", DeviceType::ESP32, "10.0.0.2:9000").with_parent("hub");
        let esp2 =
            MeshNode::new("esp2", DeviceType::ESP32, "10.0.0.3:9000").with_parent("hub");
        let esp3 =
            MeshNode::new("esp3", DeviceType::ESP32, "10.0.0.4:9000").with_parent("hub");

        nodes.insert("hub".to_string(), hub);
        nodes.insert("esp1".to_string(), esp1);
        nodes.insert("esp2".to_string(), esp2);
        nodes.insert("esp3".to_string(), esp3);

        let topo = MeshTopology::detect(&nodes);
        assert_eq!(topo.kind, TopologyKind::Star);
    }

    #[test]
    fn test_topology_detection_mesh() {
        let mut nodes = HashMap::new();
        // 3 nodes all connected to each other
        let a = MeshNode::new("a", DeviceType::Desktop, "10.0.0.1:9000")
            .with_children(vec!["b", "c"]);
        let b = MeshNode::new("b", DeviceType::Desktop, "10.0.0.2:9000")
            .with_children(vec!["a", "c"]);
        let c = MeshNode::new("c", DeviceType::Desktop, "10.0.0.3:9000")
            .with_children(vec!["a", "b"]);

        nodes.insert("a".to_string(), a);
        nodes.insert("b".to_string(), b);
        nodes.insert("c".to_string(), c);

        let topo = MeshTopology::detect(&nodes);
        assert_eq!(topo.kind, TopologyKind::FullMesh);
    }

    #[test]
    fn test_leave_removes_node_from_mesh() {
        let mesh = make_test_mesh();
        mesh.add_node(MeshNode::new("other", DeviceType::Desktop, "10.0.0.2:9000"));
        assert_eq!(mesh.node_count(), 2);

        mesh.leave().unwrap();
        assert!(!mesh.is_joined());
    }

    #[test]
    fn test_heartbeat_updates_last_seen() {
        let mesh = make_test_mesh();
        let node = MeshNode::new("sensor", DeviceType::ESP32, "10.0.0.2:9000");
        mesh.add_node(node);

        // Simulate some time passing
        std::thread::sleep(Duration::from_millis(10));
        mesh.heartbeat();

        let nodes = mesh.nodes.lock().unwrap();
        let sensor = nodes.get("sensor").unwrap();
        assert!(sensor.last_seen_duration() < Duration::from_millis(100));
    }

    #[test]
    fn test_on_event_fires_node_joined() {
        let mesh = make_test_mesh();
        let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let events_clone = events.clone();

        mesh.on_event(Box::new(move |e| match e {
            MeshEvent::NodeJoined { node } => {
                events_clone
                    .lock()
                    .unwrap()
                    .push(format!("joined:{}", node.id));
            }
            _ => {}
        }));

        mesh.add_node(MeshNode::new("newbie", DeviceType::ESP32, "10.0.0.5:9000"));

        let evts = events.lock().unwrap();
        assert_eq!(evts.len(), 1);
        assert_eq!(evts[0], "joined:newbie");
    }
}
