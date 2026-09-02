use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use mongodb::{Client as MongoClient, Collection, bson::Document, bson::doc};
use rand::Rng;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{task, time};
use tokio::sync::Mutex;
use tracing::{info, debug};

use dotenvy::dotenv;

mod raft_logic;
use raft_logic::LogEntry;


#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    id: usize,

    #[arg(long)]
    port: u16,

    /// comma separated peer entries: id:host:port (e.g., "2:127.0.0.1:7002,3:127.0.0.1:7003")
    #[arg(long, default_value_t = String::new())]
    peers: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct RequestVoteArgs {
    term: u64,
    candidate_id: usize,
    last_log_index: i64,
    last_log_term: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct RequestVoteResponse {
    term: u64,
    vote_granted: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct AppendEntriesArgs {
    term: u64,
    leader_id: usize,
    prev_log_index: i64,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: i64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct AppendEntriesResponse {
    term: u64,
    success: bool,
    conflict_index: Option<i64>,
    conflict_term: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct ClientSet {
    key: i64,
    value: String,
}

#[derive(Serialize)]
struct ClientSetResp {
    status: String,
    index: Option<i64>,
}

#[derive(Serialize)]
struct ClientGetResp {
    found: bool,
    value: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug)]
struct PeerInfo {
    id: usize,
    addr: String,
}

struct RaftState {
    id: usize,
    role: Role,
    current_term: u64,
    voted_for: Option<usize>,

    log: Vec<LogEntry>,
    commit_index: i64,
    last_applied: i64,

    // leader volatile state
    next_index: HashMap<usize, i64>,
    match_index: HashMap<usize, i64>,

    // cluster
    peers: Vec<PeerInfo>,
    client: Client,

    // election timeout tracking
    last_heartbeat_ms: u128,

    // database - kv collection
    kv_collection: Collection<Document>,
}

impl RaftState {
    async fn new(id: usize, peers: Vec<PeerInfo>) -> Self {
        let mongo_uri = std::env::var("MONGO_URI").unwrap_or("mongodb://127.0.0.1:27017".into());
        let mongo_client = MongoClient::with_uri_str(&mongo_uri)
            .await
            .expect("Mongo connection failed");

        let db_name = std::env::var("DB_NAME").unwrap_or("rust_raft".into());
        let kv_collection = mongo_client.database(&db_name).collection::<Document>("kvstore");

        Self {
            id,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            peers,
            client: Client::builder()
                .timeout(Duration::from_millis(200))
                .build()
                .unwrap(),
            kv_collection,
            last_heartbeat_ms: current_ms(),
        }
    }

    // These all delegate to raft_logic, which is the part of this codebase
    // that's actually unit tested (see rust/src/raft_logic.rs). Keeping the
    // election-safety and log-matching rules there, separate from the
    // Mutex/MongoDB/HTTP plumbing in this file, is what makes it possible to
    // test them without spinning up a cluster.
    fn last_log_index(&self) -> i64 {
        raft_logic::last_log_index(&self.log)
    }

    fn last_log_term(&self) -> u64 {
        raft_logic::last_log_term(&self.log)
    }

    fn get_log_term(&self, index: i64) -> Option<u64> {
        raft_logic::get_log_term(&self.log, index)
    }

    fn majority(&self) -> usize {
        let total_nodes = self.peers.len() + 1; // peers + self
        raft_logic::majority(total_nodes)
    }

    // Helper function to get value from MongoDB
    async fn get_from_db(&self, key: i64) -> Option<String> {
        let filter = doc! {"key": key};
        match self.kv_collection.find_one(filter, None).await {
            Ok(Some(doc)) => {
                doc.get_str("value").ok().map(|s| s.to_string())
            }
            _ => None,
        }
    }

    // Helper function to save value to MongoDB
    async fn save_to_db(&self, key: i64, value: String) -> Result<(), String> {
        let filter = doc! {"key": key};
        // If key already exists, do NOT update — return an "exists" error
        match self.kv_collection.find_one(filter.clone(), None).await {
            Ok(Some(_)) => return Err("Key already exists".to_string()),
            Ok(None) => {}
            Err(e) => return Err(format!("MongoDB error (find): {}", e)),
        }

        // Insert new key (fail if concurrently inserted)
        let doc = doc! {"key": key, "value": &value};
        match self.kv_collection.insert_one(doc, None).await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("MongoDB error (insert): {}", e)),
        }
    }
}

fn current_ms() -> u128 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

type SharedState = Arc<Mutex<RaftState>>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // load .env
    dotenv().ok();

    let args = Args::parse();
    let peers: Vec<PeerInfo> = if args.peers.trim().is_empty() {
        vec![]
    } else {
        args.peers
            .split(',')
            .filter_map(|s| {
                let parts: Vec<&str> = s.split(':').collect();
                if parts.len() == 3 {
                    Some(PeerInfo {
                        id: parts[0].parse().ok()?,
                        addr: format!("{}:{}", parts[1], parts[2]),
                    })
                } else {
                    None
                }
            })
            .collect()
    };

    let raft_state = RaftState::new(args.id, peers.clone()).await;
    let state = Arc::new(Mutex::new(raft_state));

    // spawn background tasks
    {
        let s = state.clone();
        task::spawn(async move {
            election_loop(s).await;
        });
    }
    {
        let s = state.clone();
        task::spawn(async move {
            apply_committed_loop(s).await;
        });
    }
    {
        let s = state.clone();
        task::spawn(async move {
            leader_heartbeat_loop(s).await;
        });
    }

    // build axum routes
    let app_state = state.clone();
    let app = Router::new()
        .route("/raft/request_vote", post(request_vote_handler))
        .route("/raft/append_entries", post(append_entries_handler))
        .route("/client/set", post(client_set_handler))
        .route("/client/get/{key}", get(client_get_handler))
        .route("/status", get(status_handler))
        .with_state(app_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    info!("Node {} listening on {}", args.id, addr);
    
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// ---------------- Handlers ----------------

async fn request_vote_handler(
    State(st): State<SharedState>,
    Json(args): Json<RequestVoteArgs>,
) -> (StatusCode, Json<RequestVoteResponse>) {
    let mut s = st.lock().await;
    
    if args.term < s.current_term {
        return (
            StatusCode::OK,
            Json(RequestVoteResponse {
                term: s.current_term,
                vote_granted: false,
            }),
        );
    }
    
    if args.term > s.current_term {
        s.current_term = args.term;
        s.voted_for = None;
        s.role = Role::Follower;
    }
    
    let up_to_date = raft_logic::is_log_up_to_date(
        args.last_log_term,
        args.last_log_index,
        s.last_log_term(),
        s.last_log_index(),
    );
    
    let vote_granted = if (s.voted_for.is_none() || s.voted_for == Some(args.candidate_id))
        && up_to_date
    {
        s.voted_for = Some(args.candidate_id);
        s.last_heartbeat_ms = current_ms();
        debug!("Node {} voted for {} in term {}", s.id, args.candidate_id, s.current_term);
        true
    } else {
        false
    };
    
    (
        StatusCode::OK,
        Json(RequestVoteResponse {
            term: s.current_term,
            vote_granted,
        }),
    )
}

async fn append_entries_handler(
    State(st): State<SharedState>,
    Json(args): Json<AppendEntriesArgs>,
) -> (StatusCode, Json<AppendEntriesResponse>) {
    let mut s = st.lock().await;

    if args.term < s.current_term {
        return (
            StatusCode::OK,
            Json(AppendEntriesResponse {
                term: s.current_term,
                success: false,
                conflict_index: None,
                conflict_term: None,
            }),
        );
    }

    if args.term >= s.current_term {
        s.current_term = args.term;
        s.role = Role::Follower;
        s.voted_for = None;
    }
    
    s.last_heartbeat_ms = current_ms();

    let result = raft_logic::apply_append_entries(
        &mut s.log,
        args.prev_log_index,
        args.prev_log_term,
        &args.entries,
    );

    if !result.success {
        debug!(
            "Node {} rejected AppendEntries from leader {} (conflict_index={:?})",
            s.id, args.leader_id, result.conflict_index
        );
        return (
            StatusCode::OK,
            Json(AppendEntriesResponse {
                term: s.current_term,
                success: false,
                conflict_index: result.conflict_index,
                conflict_term: result.conflict_term,
            }),
        );
    }

    if args.leader_commit > s.commit_index {
        s.commit_index = std::cmp::min(args.leader_commit, s.last_log_index());
    }

    (
        StatusCode::OK,
        Json(AppendEntriesResponse {
            term: s.current_term,
            success: true,
            conflict_index: None,
            conflict_term: None,
        }),
    )
}

async fn client_set_handler(
    State(st): State<SharedState>,
    Json(req): Json<ClientSet>,
) -> (StatusCode, Json<ClientSetResp>) {
    // Check if leader
    {
        let s = st.lock().await;
        if s.role != Role::Leader {
            return (
                StatusCode::OK,
                Json(ClientSetResp {
                    status: "not_leader".into(),
                    index: None,
                }),
            );
        }
    }


    {
        let s = st.lock().await;
        if let Some(_) = s.get_from_db(req.key).await {
            return (
                StatusCode::OK,
                Json(ClientSetResp {
                    status: "Key already exists".into(),
                    index: None,
                }),
            );
        }
    }

    // Append to leader's log
    let (index, _term) = {
        let mut s = st.lock().await;
        let idx = s.last_log_index() + 1;
        let t = s.current_term;
        let entry = LogEntry {
            term: t,
            index: idx,
            key: req.key,
            value: req.value.clone(),
        };
        s.log.push(entry);
        (idx, t)
    };

    // Wait for replication and commit
    for _ in 0..20 {
        time::sleep(Duration::from_millis(50)).await;
        
        let s = st.lock().await;
        if s.commit_index >= index {
            return (
                StatusCode::OK,
                Json(ClientSetResp {
                    status: "committed".into(),
                    index: Some(index),
                }),
            );
        }
    }

    (
        StatusCode::OK,
        Json(ClientSetResp {
            status: "timeout".into(),
            index: Some(index),
        }),
    )
}

async fn client_get_handler(
    State(st): State<SharedState>,
    Path(key): Path<i64>,
) -> (StatusCode, Json<ClientGetResp>) {
    // Read directly from MongoDB - any node can serve reads
    let s = st.lock().await;
    let value = s.get_from_db(key).await;
    
    (
        StatusCode::OK,
        Json(ClientGetResp {
            found: value.is_some(),
            value,
        }),
    )
}

async fn status_handler(State(st): State<SharedState>) -> (StatusCode, Json<serde_json::Value>) {
    let s = st.lock().await;
    
    // Get all keys from MongoDB for display
    let mut store = HashMap::new();
    if let Ok(mut cursor) = s.kv_collection.find(None, None).await {
        use futures::stream::StreamExt;
        while let Some(Ok(doc)) = cursor.next().await {
            if let (Ok(key), Ok(value)) = (doc.get_i64("key"), doc.get_str("value")) {
                store.insert(key, value.to_string());
            }
        }
    }
    
    let report = serde_json::json!({
        "id": s.id,
        "role": format!("{:?}", s.role),
        "term": s.current_term,
        "voted_for": s.voted_for,
        "last_log_index": s.last_log_index(),
        "last_log_term": s.last_log_term(),
        "commit_index": s.commit_index,
        "last_applied": s.last_applied,
        "log_length": s.log.len(),
        "store": store,
    });
    (StatusCode::OK, Json(report))
}

// ---------------- Background loops ----------------

async fn election_loop(st: SharedState) {
    loop {
        let timeout_ms = rand::thread_rng().gen_range(150..300);
        time::sleep(Duration::from_millis(timeout_ms)).await;

        let should_start_election = {
            let s = st.lock().await;
            if s.role == Role::Leader {
                false
            } else {
                let elapsed = current_ms().saturating_sub(s.last_heartbeat_ms);
                elapsed >= 300
            }
        };

        if !should_start_election {
            continue;
        }

        let (term_at_start, last_idx, last_term, peers, id, client) = {
            let mut s = st.lock().await;
            s.role = Role::Candidate;
            s.current_term += 1;
            s.voted_for = Some(s.id);
            s.last_heartbeat_ms = current_ms();
            
            info!("Node {} starting election for term {}", s.id, s.current_term);
            
            (
                s.current_term,
                s.last_log_index(),
                s.last_log_term(),
                s.peers.clone(),
                s.id,
                s.client.clone(),
            )
        };

        let mut handles = vec![];
        for peer in peers.iter() {
            let url = format!("http://{}/raft/request_vote", peer.addr);
            let c = client.clone();
            let args = RequestVoteArgs {
                term: term_at_start,
                candidate_id: id,
                last_log_index: last_idx,
                last_log_term: last_term,
            };
            
            handles.push(task::spawn(async move {
                match c.post(&url).json(&args).send().await {
                    Ok(r) => match r.json::<RequestVoteResponse>().await {
                        Ok(rv) => Some(rv),
                        Err(_) => None,
                    },
                    Err(_) => None,
                }
            }));
        }

        let mut votes = 1usize;
        let mut higher_term_seen = None;
        
        for h in handles {
            if let Ok(Some(rv)) = h.await {
                if rv.term > term_at_start {
                    higher_term_seen = Some(rv.term);
                } else if rv.vote_granted {
                    votes += 1;
                }
            }
        }

        let mut s = st.lock().await;
        
        if let Some(ht) = higher_term_seen {
            if ht > s.current_term {
                s.current_term = ht;
                s.role = Role::Follower;
                s.voted_for = None;
            }
            continue;
        }

        if s.role == Role::Candidate && s.current_term == term_at_start {
            let majority = s.majority();
            if votes >= majority {
                s.role = Role::Leader;
                info!("Node {} became leader for term {} with {} votes", s.id, s.current_term, votes);
                
                let next_idx = s.last_log_index() + 1;
                let peer_ids: Vec<usize> = s.peers.iter().map(|p| p.id).collect();
                
                for peer_id in peer_ids {
                    s.next_index.insert(peer_id, next_idx);
                    s.match_index.insert(peer_id, 0);
                }
            } else {
                s.role = Role::Follower;
            }
        }
    }
}

async fn leader_heartbeat_loop(st: SharedState) {
    loop {
        time::sleep(Duration::from_millis(50)).await;
        
        let is_leader = {
            let s = st.lock().await;
            s.role == Role::Leader
        };
        
        if !is_leader {
            continue;
        }
        
        let (term, leader_id, peers, client) = {
            let s = st.lock().await;
            (s.current_term, s.id, s.peers.clone(), s.client.clone())
        };
        
        for peer in peers {
            let st_clone = st.clone();
            let client_clone = client.clone();
            let peer_clone = peer.clone();
            
            task::spawn(async move {
                replicate_to_peer(st_clone, peer_clone, term, leader_id, client_clone).await;
            });
        }
    }
}

async fn replicate_to_peer(
    st: SharedState,
    peer: PeerInfo,
    term: u64,
    leader_id: usize,
    client: Client,
) {
    let (next_idx, prev_log_index, prev_log_term, entries, leader_commit) = {
        let s = st.lock().await;
        if s.role != Role::Leader || s.current_term != term {
            return;
        }
        
        let next_idx = *s.next_index.get(&peer.id).unwrap_or(&1);
        let prev_log_index = next_idx - 1;
        let prev_log_term = s.get_log_term(prev_log_index).unwrap_or(0);
        
        let entries: Vec<LogEntry> = s
            .log
            .iter()
            .filter(|e| e.index >= next_idx)
            .cloned()
            .collect();
        
        (next_idx, prev_log_index, prev_log_term, entries, s.commit_index)
    };
    
    let args = AppendEntriesArgs {
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries: entries.clone(),
        leader_commit,
    };
    
    let url = format!("http://{}/raft/append_entries", peer.addr);
    
    match client.post(&url).json(&args).send().await {
        Ok(resp) => match resp.json::<AppendEntriesResponse>().await {
            Ok(ae_resp) => {
                let mut s = st.lock().await;
                
                if ae_resp.term > s.current_term {
                    s.current_term = ae_resp.term;
                    s.role = Role::Follower;
                    s.voted_for = None;
                    return;
                }
                
                if s.role != Role::Leader || s.current_term != term {
                    return;
                }
                
                if ae_resp.success {
                    if !entries.is_empty() {
                        let last_idx = entries.last().unwrap().index;
                        s.next_index.insert(peer.id, last_idx + 1);
                        s.match_index.insert(peer.id, last_idx);
                    }
                    
                    update_commit_index(&mut s);
                } else {
                    if let Some(conflict_idx) = ae_resp.conflict_index {
                        s.next_index.insert(peer.id, conflict_idx);
                    } else {
                        let new_next = next_idx.saturating_sub(1).max(1);
                        s.next_index.insert(peer.id, new_next);
                    }
                }
            }
            Err(_) => {}
        },
        Err(_) => {}
    }
}

fn update_commit_index(s: &mut RaftState) {
    if s.role != Role::Leader {
        return;
    }

    let match_indices: Vec<i64> = s.match_index.values().copied().collect();
    let new_commit_index = raft_logic::compute_commit_index(
        match_indices,
        s.last_log_index(),
        s.current_term,
        &s.log,
        s.commit_index,
    );

    if new_commit_index > s.commit_index {
        s.commit_index = new_commit_index;
        debug!("Leader {} advanced commit_index to {}", s.id, new_commit_index);
    }
}

async fn apply_committed_loop(st: SharedState) {
    loop {
        time::sleep(Duration::from_millis(50)).await;

        let (_commit_idx, _last_applied, entry_opt) = {
            let mut s = st.lock().await;
            if s.commit_index > s.last_applied {
                let idx = s.last_applied + 1;
                let e = s.log.iter().find(|e| e.index == idx).cloned();
                s.last_applied = idx;
                (s.commit_index, s.last_applied, e)
            } else {
                continue;
            }
        };

        if let Some(entry) = entry_opt {
            // APPLY TO DATABASE HERE
            let st2 = st.clone();
            let key = entry.key;
            let value = entry.value.clone();

            tokio::spawn(async move {
                let s = st2.lock().await;
                match s.save_to_db(key, value).await {
                    Ok(_) => debug!("Applied entry key={} to DB", key),
                    Err(e) if e == "exists" => debug!("Key {} already exists in DB; skipping apply", key),
                    Err(e) => tracing::error!("Failed to persist entry key={} to DB: {}", key, e),
                }
            });
        }
    }
}
