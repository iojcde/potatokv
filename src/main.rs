pub mod handlers;
pub mod kv;
pub mod log;
pub mod message;
pub mod storage;

use async_compat::Compat;

use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{to_vec, Value};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use url::Url;
use warp::Filter;

use serde_json::json;

use warp::reply::WithStatus;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Copy)]
enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

// Define the Raft node

#[derive(Debug)]
struct RaftServer {
    id: String,
    storage: storage::PersistentStorage,
    state: RwLock<RaftServerState>,

    state_machine: Mutex<kv::StateMachine>,
}

#[derive(Debug, Clone)]
struct RaftServerState {
    role: RaftRole,
    log: Vec<log::LogEntry>,

    current_leader: Option<String>,
    current_term: u64,
    voted_for: Option<String>,
    votes_received: Vec<String>,
    last_term: u64,
    election_timeout: Duration,
    last_heartbeat: Instant,

    commit_length: u64,
    peers: HashMap<String, String>,

    sent_length: HashMap<String, u64>,
    acked_length: HashMap<String, u64>,

    timer_override: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VoteResponseArgs {
    id: String,
    term: u64,
    vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RaftMessage {
    RequestVote(RequestVoteArgs),
    VoteResponse(VoteResponseArgs),
    LogRequest(LogRequestArgs),
    LogResponse(LogResponseArgs),

    BroadcastRequest(kv::Command),
}

enum RaftEvent {
    LogRequestEvent(LogRequestArgs),
    LogResponseEvent(LogResponseArgs),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogResponseArgs {
    id: String,
    term: u64,
    ack: u64,
    success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogRequestArgs {
    term: u64,
    leader_id: String,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<log::LogEntry>,
    leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestVoteArgs {
    candidate_id: String,
    term: u64,
    log_length: u64,
    log_term: u64,
}

impl RaftServer {
    // Create a new instance of the Raft state machine
    fn new(
        id: String,
        peers: HashMap<String, String>,
        state_machine: Mutex<kv::StateMachine>,
    ) -> Self {
        let server = RaftServer {
            id: id.clone(),
            storage: storage::PersistentStorage::new(PathBuf::from(format!("data_{}", id)))
                .unwrap(),
            state_machine,
            state: RwLock::new(RaftServerState {
                role: RaftRole::Follower,
                log: Vec::new(),
                current_leader: None,
                current_term: 0,
                voted_for: None,
                votes_received: Vec::new(),
                last_term: 0,
                election_timeout: Duration::from_millis(150),
                last_heartbeat: Instant::now(),
                commit_length: 0,
                peers,
                sent_length: HashMap::new(),
                acked_length: HashMap::new(),
                timer_override: false,
            }),
        };

        server
    }
    async fn run(self: Arc<Self>) {
        println!("Starting Raft server\n");

        {
            let mut state = self.state.write().unwrap();

            if let Ok(Some(pstate)) = self.storage.load_state() {
                state.current_term = pstate.current_term;
                state.voted_for = pstate.voted_for;
                state.commit_length = pstate.commit_length;
            }

            state.log = self.storage.read_log_entries().unwrap();
        }

        Self::main_loop(self).await;
    }

    async fn main_loop(self: Arc<Self>) {
        let mut i = 0;
        loop {
            i += 1;

            if i % 100 == 0 {
                let state = self.state.read().unwrap();
                println!(
                    "I am alive as {:?},  term: {}",
                    state.role, state.current_term
                );
            }

            let (role, last_heartbeat, election_timeout, timer_override) = {
                let state = self.state.read().unwrap();

                (
                    state.role.clone(),
                    state.last_heartbeat,
                    state.election_timeout,
                    state.timer_override,
                )
            };

            match role {
                RaftRole::Follower => {
                    if Instant::now().duration_since(last_heartbeat) > election_timeout * 2 {
                        println!("suspecting leader failure\n");
                        self.start_timer(true);
                        self.start_election().await;
                    }
                }
                RaftRole::Candidate => {
                    if Instant::now().duration_since(last_heartbeat) > election_timeout * 2 {
                        println!("Election timeout occurred\n");
                        // Initiate election process
                        self.reset_timer();
                        self.start_timer(true);
                        self.start_election().await;
                    }
                }
                RaftRole::Leader => {
                    if Instant::now().duration_since(last_heartbeat) > election_timeout {
                        self.start_timer(false);

                        let cloned = self.clone();
                        tokio::spawn(async move {
                            cloned.replicate_logs().await;
                        });
                    }
                }
            }

            let pstate = {
                let state = self.state.read().unwrap();
                storage::PersistentState {
                    current_term: state.current_term,
                    voted_for: state.voted_for.clone(),
                    commit_length: state.commit_length,
                }
            };

            self.storage.save_state(&pstate).unwrap();

            // Small sleep to prevent busy-waiting

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn start_timer(&self, random: bool) {
        let election_timeout = {
            if random {
                Self::random_election_timeout()
            } else {
                Duration::from_millis(150)
            }
        };

        let mut state = self.state.write().unwrap();

        state.election_timeout = election_timeout;
        state.last_heartbeat = Instant::now();
    }

    fn reset_timer(&self) {
        let mut state = self.state.write().unwrap();

        state.last_heartbeat = Instant::now();
        state.election_timeout = Self::random_election_timeout();
    }

    fn random_election_timeout() -> Duration {
        let mut rng = rand::thread_rng();
        Duration::from_millis(rng.gen_range(300..1200))
    }

    async fn start_election(&self) {
        let (msg, peer_ids) = {
            let mut state = self.state.write().unwrap();

            state.current_term += 1;

            state.role = RaftRole::Candidate;
            state.voted_for = Some(self.id.clone());
            state.votes_received = vec![self.id.clone()];
            state.last_term = state.log.last().map_or(0, |entry| entry.term);

            let msg = RequestVoteArgs {
                candidate_id: self.id.clone(),
                term: state.current_term,
                log_length: state.log.len() as u64,
                log_term: state.last_term,
            };

            let peer_ids: Vec<_> = state.peers.keys().cloned().collect();

            (msg, peer_ids)
        };

        for id in &peer_ids {
            let res = self
                .send_message_to_peer(id.clone(), RaftMessage::RequestVote(msg.clone()))
                .await;

            match res {
                Ok(msg) => {
                    let vote_response =
                        serde_json::from_str(msg.text().await.unwrap().as_str()).unwrap();

                    self.handle_vote(vote_response).await;
                }
                Err(e) => {
                    // println!("Error sending vote request to {}: {:?}", id, e);
                }
            }
        }
    }

    fn append_entries(
        &self,
        prev_log_index: u64,
        leader_commit: u64,
        entries: &[log::LogEntry],
    ) -> bool {
        let mut state = self.state.write().unwrap();

        // Check if we have enough entries to compare
        if !entries.is_empty() && state.log.len() > prev_log_index as usize {
            let index = std::cmp::min(state.log.len(), prev_log_index as usize + entries.len());
            if state.log[index - 1].term != entries[index - prev_log_index as usize - 1].term {
                // Truncate the log
                state.log.truncate(prev_log_index as usize);
            }
        }

        // Append new entries

        self.storage.append_log_entries(entries).unwrap();
        if prev_log_index as usize + entries.len() > state.log.len() {
            for i in (state.log.len() - prev_log_index as usize)..entries.len() {
                state.log.push(entries[i].clone());
            }
        }

        // Update commit length and deliver messages
        if leader_commit > state.commit_length {
            for i in state.commit_length as usize..leader_commit as usize {
                let mut machine = self.state_machine.lock().unwrap();

                machine.apply(&state.log[i], i as u64);

                println!("Delivered message: {:?}", state.log[i].command);
            }
            state.commit_length = leader_commit;
        }

        true
    }

    async fn broadcast_message(&self, cmd: kv::Command) {
        let (role, current_term) = {
            let state = self.state.read().unwrap();
            println!(
                "leader: {:?},peers: {:?}",
                state.current_leader, state.peers
            );

            (state.role.clone(), state.current_term)
        };

        println!("Broadcasting message {:?}", cmd);

        match role {
            RaftRole::Leader => {
                println!("yo leader here");
                {
                    let mut state = self.state.write().unwrap();

                    let new_entry = log::LogEntry {
                        term: current_term,
                        command: cmd,
                    };
                    state.log.push(new_entry.clone());

                    self.storage.append_log_entries(&[new_entry]);

                    state.timer_override = true;
                }
            }
            _ => {
                let leader_addr = {
                    let state = self.state.read().unwrap();

                    state
                        .peers
                        .get(&state.current_leader.clone().unwrap())
                        .unwrap()
                        .clone()
                };

                let msg = RaftMessage::BroadcastRequest(cmd);

                Self::send_message(&leader_addr, msg).await;
            }
        }
    }

    async fn send_message_to_peer(
        &self,
        peer_id: String,
        message: RaftMessage,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let peer_addr = {
            let state = self.state.read().unwrap();
            state.peers.get(&peer_id).unwrap().clone()
        };

        let res = Self::send_message(&peer_addr, message).await;

        return res;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum KVMessage {
    Put(KVPutRequest),
    Get(KVGetRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KVPutRequest {
    key: String,
    value: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct KVGetRequest {
    key: String,
}

#[tokio::main]
async fn main() {
    // get port from command line
    let port = std::env::args()
        .nth(1)
        .expect("Usage: raft-server <port>")
        .parse::<u16>()
        .expect("Invalid port");

    // get peer port from command line
    let peer_port1 = std::env::args()
        .nth(2)
        .expect("Usage: raft-server <port> <peer_ports>")
        .parse::<u16>()
        .expect("Invalid port");

    let peer_port2 = std::env::args()
        .nth(3)
        .expect("Usage: raft-server <port> <peer_ports>")
        .parse::<u16>()
        .expect("Invalid port");

    let mut peers = HashMap::new();

    peers.insert(
        peer_port1.to_string(),
        format!("127.0.0.1:{}", peer_port1).to_string(),
    );

    peers.insert(
        peer_port2.to_string(),
        format!("127.0.0.1:{}", peer_port2).to_string(),
    );

    let state_machine = Mutex::new(kv::StateMachine::new());

    let server = Arc::new(RaftServer::new(port.to_string(), peers, state_machine));

    server.storage.load_kv_store().unwrap();

    println!("my port is {}", port);
    println!("my id is {}", server.id);

    let server_clone = Arc::clone(&server);
    let server_filter = warp::any().map(move || Arc::clone(&server_clone));

    let request_vote_route = warp::path("request_vote")
        .and(warp::post())
        .and(warp::body::json())
        .and(server_filter.clone())
        .and_then(|req: RaftMessage, server: Arc<RaftServer>| async move {
            if let RaftMessage::RequestVote(args) = req {
                // Now you have access to the RequestVoteArgs
                // You can use 'args' here

                let response = server.handle_request_vote(args);

                Ok::<_, warp::Rejection>(warp::reply::json(&response))
            } else {
                println!("Invalid message!!!!!!!!!!!!!!!!!!!!!!!");
                Ok::<_, warp::Rejection>(warp::reply::json(&json!({"error": "Invalid message"})))
            }
        });

    let append_entries_route = warp::path("append_entries")
        .and(warp::post())
        .and(warp::body::json())
        .and(server_filter.clone())
        .and_then(|req: RaftMessage, server: Arc<RaftServer>| async move {
            if let RaftMessage::LogRequest(args) = req {
                let response = server.handle_log_request(args);

                Ok::<_, warp::Rejection>(warp::reply::json(&response))
            } else {
                Ok::<_, warp::Rejection>(warp::reply::json(&json!({"error": "Invalid message"})))
            }
        });

    let broadcast_request_route = warp::path("broadcast")
        .and(warp::post())
        .and(warp::body::json())
        .and(server_filter.clone())
        .and_then(|req: RaftMessage, server: Arc<RaftServer>| async move {
            println!("Received broadcast request wowowow {:?}", req);

            if server.state.read().unwrap().role == RaftRole::Leader {
                if let RaftMessage::BroadcastRequest(cmd) = req {
                    println!("Received broadcast request {:?}", cmd);

                    server.broadcast_message(cmd).await;
                    Ok::<WithStatus<&str>, warp::Rejection>(warp::reply::with_status(
                        "ok",
                        warp::http::StatusCode::OK,
                    ))
                } else {
                    Ok(warp::reply::with_status(
                        "Bad Request",
                        warp::http::StatusCode::BAD_REQUEST,
                    ))
                }
            } else {
                Ok(warp::reply::with_status(
                    "Bad Request",
                    warp::http::StatusCode::BAD_REQUEST,
                ))
            }
        });

    let kv_put_route = warp::path!("kv" / "put")
        .and(warp::post())
        .and(warp::body::json())
        .and(server_filter.clone())
        .and_then(|cmd: KVMessage, server: Arc<RaftServer>| async move {
            if let KVMessage::Put(req) = cmd {
                server
                    .broadcast_message(kv::Command::Put {
                        key: req.key,
                        value: json!(req.value),
                    })
                    .await;

                Ok::<WithStatus<&str>, warp::Rejection>(warp::reply::with_status(
                    "ok",
                    warp::http::StatusCode::OK,
                ))
            } else {
                Ok::<WithStatus<&str>, warp::Rejection>(warp::reply::with_status(
                    "shit",
                    warp::http::StatusCode::OK,
                ))
            }
        });

    let kv_get_route = warp::path!("kv" / "get")
        .and(warp::post())
        .and(warp::body::json())
        .and(server_filter.clone())
        .and_then(|cmd: KVMessage, server: Arc<RaftServer>| async move {
            if let KVMessage::Get(req) = cmd {
                let state_machine: std::sync::MutexGuard<'_, kv::StateMachine> =
                    server.state_machine.lock().unwrap();
                let response = state_machine.get(&req.key);

                match response {
                    Some(value) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                        warp::reply::json(&value),
                        warp::http::StatusCode::OK,
                    )),
                    None => Ok::<_, warp::Rejection>(warp::reply::with_status(
                        warp::reply::json(&json!({"error": "Key not found"})),
                        warp::http::StatusCode::NOT_FOUND,
                    )),
                }
            } else {
                Ok::<_, warp::Rejection>(warp::reply::with_status(
                    warp::reply::json(&json!("Bad Request")),
                    warp::http::StatusCode::BAD_REQUEST,
                ))
            }
        });

    let routes = request_vote_route
        .or(append_entries_route)
        .or(broadcast_request_route)
        .or(kv_put_route)
        .or(kv_get_route);

    // let executor = Executor::new();

    let main_loop = tokio::spawn(async move {
        server.run().await;
    });

    tokio::select! {
        _ = async {
            warp::serve(routes)
                .run(([127, 0, 0, 1], port))
                .await;
        } => {},
        _ = main_loop => {},
    }
}
