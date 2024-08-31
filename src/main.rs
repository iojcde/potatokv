use async_recursion::async_recursion;
use core::panic;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{to_vec, Value};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use url::Url;

use serde_json::json;
use warp::Filter;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Copy)]
enum RaftState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogEntry {
    term: u64,
    command: String,
}

#[derive(Debug)]
struct RaftServer {
    id: String,
    storage: PersistentStorage,

    state: RwLock<RaftServerState>,
}

#[derive(Debug, Clone)]
struct RaftServerState {
    role: RaftState,
    log: Vec<LogEntry>,

    current_leader: Option<String>,
    current_term: u64,
    voted_for: Option<String>,
    votes_received: Vec<String>,
    last_term: u64,
    election_timeout: Duration,
    last_heartbeat: Instant,
    timer_cancel: Option<mpsc::Sender<()>>,

    commit_length: u64,
    peers: HashMap<String, String>,

    sent_length: HashMap<String, u64>,
    acked_length: HashMap<String, u64>,
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
    entries: Vec<LogEntry>,
    leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestVoteArgs {
    candidate_id: String,
    term: u64,
    log_length: u64,
    log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistentStorage {
    path: PathBuf,
}

impl PersistentStorage {
    fn new(path: PathBuf) -> Self {
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn save_state(&self, term: u64, voted_for: Option<String>) -> std::io::Result<()> {
        let state_path = self.path.join("state");
        let mut file = File::create(state_path)?;
        serde_json::to_writer(&mut file, &(term, voted_for))?;
        file.sync_all()?;
        Ok(())
    }

    fn load_state(&self) -> std::io::Result<(u64, Option<String>)> {
        let state_path = self.path.join("state");
        let file = File::open(state_path)?;
        let state: (u64, Option<String>) = serde_json::from_reader(file)?;
        Ok(state)
    }

    fn append_log_entries(&self, entries: &[LogEntry]) -> std::io::Result<()> {
        let log_path = self.path.join("log");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?;
        for entry in entries {
            serde_json::to_writer(&mut file, entry)?;
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
        Ok(())
    }

    fn read_log_entries(&self) -> std::io::Result<Vec<LogEntry>> {
        let log_path = self.path.join("log");
        let file = File::open(log_path)?;
        let reader = std::io::BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let entry: LogEntry = serde_json::from_str(&line?)?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftServer {
    // Create a new instance of the Raft state machine
    fn new(id: String, peers: HashMap<String, String>) -> Self {
        let server = RaftServer {
            id,
            storage: PersistentStorage::new(PathBuf::from("data")),
            state: RwLock::new(RaftServerState {
                role: RaftState::Follower,
                log: Vec::new(),
                current_leader: None,
                current_term: 0,
                voted_for: None,
                votes_received: Vec::new(),
                last_term: 0,
                election_timeout: Duration::from_millis(150),
                last_heartbeat: Instant::now(),
                timer_cancel: None,
                commit_length: 0,
                peers,
                sent_length: HashMap::new(),
                acked_length: HashMap::new(),
            }),
        };

        server
    }
    async fn run(&self) {
        println!("Starting Raft server\n");

        let mut i = 0;

        loop {
            i += 1;

            if i % 100 == 0 {
                let state = self.state.read().unwrap();
                println!("I am alive as {:?}", state.role);
            }

            let (role, last_heartbeat, election_timeout) = {
                let state = self.state.read().unwrap();

                (
                    state.role.clone(),
                    state.last_heartbeat,
                    state.election_timeout,
                )
            };

            match role {
                RaftState::Follower => {
                    if Instant::now().duration_since(last_heartbeat) > election_timeout {
                        println!("suspecting leader failure\n");
                        self.start_election_timer();
                        self.start_election().await;
                    }
                }
                RaftState::Candidate => {
                    if Instant::now().duration_since(last_heartbeat) > election_timeout {
                        println!("Election timeout occurred\n");
                        // Initiate election process
                        self.reset_election_timer();
                        self.start_election_timer();
                        self.start_election().await;    
                    }
                }
                RaftState::Leader => {
                    self.replicate_logs().await;
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
            }

            // Small sleep to prevent busy-waiting
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn start_election_timer(&self) {
        let election_timeout = Self::random_election_timeout();
        let (tx, _) = mpsc::channel(1);

        let mut state = self.state.write().unwrap();

        state.election_timeout = election_timeout;
        state.last_heartbeat = Instant::now();
        state.timer_cancel = Some(tx);
    }

    fn cancel_election_timer(&self) {
        let mut state = self.state.write().unwrap();
        if let Some(cancel) = state.timer_cancel.take() {
            let _ = cancel.send(());
        }
    }

    fn reset_election_timer(&self) {
        let mut state = self.state.write().unwrap();

        state.last_heartbeat = Instant::now();
        state.election_timeout = Self::random_election_timeout();
    }

    fn random_election_timeout() -> Duration {
        let mut rng = rand::thread_rng();
        Duration::from_millis(rng.gen_range(300..600))
    }

    fn handle_request_vote(&self, req: RequestVoteArgs) -> VoteResponseArgs {
        let mut state = self.state.write().unwrap();
        println!("Received vote request");

        if req.term > state.current_term {
            state.current_term = req.term;
            state.role = RaftState::Follower;
            state.voted_for = None;
        }

        state.last_term = state.log.last().map_or(0, |entry| entry.term);

        let log_ok = (req.log_term > state.last_term)
            || (req.log_term == state.last_term && req.log_length >= state.log.len() as u64);

        // if cTerm = currentTerm ∧ logOk ∧ votedFor ∈ {cId, null} then

        if req.term == state.current_term
            && log_ok
            && (state.voted_for.is_none() || state.voted_for == Some(req.candidate_id.clone()))
        {
            state.voted_for = Some(req.candidate_id.clone());
            let msg = VoteResponseArgs {
                id: self.id.clone(),
                term: state.current_term,
                vote_granted: true,
            };

            println!(" voted for {}", req.candidate_id);

            return msg;
        } else {
            let msg = VoteResponseArgs {
                id: self.id.clone(),
                term: state.current_term,
                vote_granted: false,
            };
            println!(
                "Didn't vote for {}, logok: {}, req.term: {}, current_term: {}, voted_for: {:?}",
                req.candidate_id, log_ok, req.term, state.current_term, state.voted_for
            );

            drop(state);

            return msg;
        }
    }

    #[async_recursion]
    async fn replicate_logs(&self) {
        let peer_ids = {
            let state = self.state.read().unwrap();
            state.peers.keys().cloned().collect::<Vec<_>>()
        };

        println!("Replicating logs");

        for id in &peer_ids {
            
            let msg = {
                let state = self.state.read().unwrap();

                let prev_log_index = state.sent_length[id];
                let prev_log_term = {
                    if prev_log_index == 0 {
                        0
                    } else {
                        state.log[prev_log_index as usize - 1].term
                    }
                };

                let entries = state.log[prev_log_index as usize..].to_vec();

                let msg = LogRequestArgs {
                    term: state.current_term,
                    leader_id: self.id.clone(),
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: state.commit_length,
                };

                msg
            };
            

            let res = self
                .send_message_to_peer(id.clone(), RaftMessage::LogRequest(msg))
                .await;

            match res {
                Ok(msg) => {
                    let log_response =
                        serde_json::from_str(msg.text().await.unwrap().as_str()).unwrap();

                    self.handle_log_ack(log_response).await;
                }
                Err(e) => {
                    // println!("Error sending log request to {}: {:?}", id, e);
                }
            }
        }
    }

    async fn start_election(&self) {
        let (msg, peer_ids) = {
            let mut state = self.state.write().unwrap();

            state.current_term += 1;

            state.role = RaftState::Candidate;
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

    fn handle_log_request(&self, req: LogRequestArgs) -> LogResponseArgs {
        let mut state = self.state.write().unwrap();

        state.last_heartbeat = Instant::now();

        // If the leader's term is greater, update our term and become a follower
        if req.term > state.current_term {
            state.current_term = req.term;
            state.role = RaftState::Follower;
            state.voted_for = None;
            state.current_leader = Some(req.leader_id.clone());

            drop(state);
            self.cancel_election_timer();

            state = self.state.write().unwrap();
        }

        // If we're in the same term, recognize the leader
        if req.term == state.current_term {
            state.role = RaftState::Follower;
            state.current_leader = Some(req.leader_id.clone());
        }

        // Check if our log is consistent with the leader's
        let log_ok = state.log.len() as u64 >= req.prev_log_index
            && (req.prev_log_index == 0
                || state.log[req.prev_log_index as usize].term == req.prev_log_term);

        if req.term == state.current_term && log_ok {
            // Our log is consistent, so append the new entries

            drop(state);
            let success = self.append_entries(req.prev_log_index, req.leader_commit, &req.entries);
            let state = self.state.write().unwrap();

            let ack = req.prev_log_index + req.entries.len() as u64;

            LogResponseArgs {
                id: self.id.clone(),
                term: state.current_term,
                ack,
                success,
            }
        } else {
            println!("shit!!!");

            // Our log is inconsistent or the term doesn't match
            LogResponseArgs {
                id: self.id.clone(),
                term: state.current_term,
                ack: state.log.len() as u64,
                success: false,
            }
        }
    }

    fn append_entries(
        &self,
        prev_log_index: u64,
        leader_commit: u64,
        entries: &[LogEntry],
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
        if prev_log_index as usize + entries.len() > state.log.len() {
            for i in (state.log.len() - prev_log_index as usize - 1)..entries.len() {
                state.log.push(entries[i].clone());
            }
        }

        // Update commit length and deliver messages
        if leader_commit > state.commit_length {
            for i in state.commit_length as usize..leader_commit as usize {
                // self.deliver_to_application(&state.log[i].command);

                println!("Delivered message: {}", state.log[i].command);
            }
            state.commit_length = leader_commit;
        }

        true
    }

    async fn handle_log_ack(&self, msg: LogResponseArgs) -> &str {
        let (current_term, role, sent_length, acked_length) = {
            let state = self.state.read().unwrap();

            (
                state.current_term,
                state.role.clone(),
                state.sent_length.clone(),
                state.acked_length.clone(),
            )
        };

        if msg.term == current_term && role == RaftState::Leader {
            if msg.success && msg.ack > acked_length[&msg.id] {
                let mut state = self.state.write().unwrap();
                *state.sent_length.get_mut(&msg.id).unwrap() = msg.ack;
                *state.acked_length.get_mut(&msg.id).unwrap() = msg.ack;

                // commit log entries

                return "ok";
            } else if sent_length[&msg.id] > 0 {
                {
                    let mut state = self.state.write().unwrap();
                    *state.sent_length.get_mut(&msg.id).unwrap() -= 1;
                }

                self.replicate_logs().await;

                return "ok";
            } else {
                return "shit";
            }
        } else if msg.term > current_term {
            let mut state = self.state.write().unwrap();
            state.current_term = msg.term;
            state.role = RaftState::Follower;
            state.voted_for = None;
            self.cancel_election_timer();

            return "ok";
        } else {
            return "shit";
        }
    }

    async fn handle_vote(&self, req: VoteResponseArgs) -> &str {
        let (role, current_term) = {
            let state = self.state.read().unwrap();

            (state.role, state.current_term)
        };

        println!("Received vote response {:?}", req);

        // if state is candidate
        if role == RaftState::Candidate && req.term == current_term && req.vote_granted {
            let (votes_received, peers) = {
                let mut state = self.state.write().unwrap();

                state.votes_received.push(req.id.clone());
                println!(
                    "votes_received: {} peers: {}",
                    state.votes_received.len(),
                    state.peers.len()
                );

                (state.votes_received.clone(), state.peers.clone())
            };

            if votes_received.len() as u64 > ((peers.len() + 1) / 2) as u64 {
                {
                    let mut state = self.state.write().unwrap();

                    state.role = RaftState::Leader;
                    state.voted_for = None;
                    state.votes_received = Vec::new();
                    state.current_leader = Some(self.id.clone());

                    println!("Elected as leader");

                    let peer_ids: Vec<_> = state.peers.keys().cloned().collect();

                    for id in &peer_ids {
                        let log_length = state.log.len();
                        state.sent_length.insert(id.clone(), log_length as u64);
                        state.acked_length.insert(id.clone(), 0);
                    }
                }

                self.cancel_election_timer();

                self.replicate_logs().await;

                return "ok";
            }
        } else if req.term > current_term {
            {
                let mut state = self.state.write().unwrap();

                state.current_term = req.term;
                state.role = RaftState::Follower;
                state.voted_for = None;
                state.current_leader = Some(req.id.clone());
            }

            self.cancel_election_timer();
            // cancel election timer

            return "ok";
        }

        println!("shit");
        return "shit";
    }

    async fn broadcast_message(&self, message: RaftMessage) {
        let (role, current_term, peers) = {
            let state = self.state.read().unwrap();

            (state.role.clone(), state.current_term, state.peers.clone())
        };

        println!("Broadcasting message {:?}", message);

        match role {
            RaftState::Leader => {
                {
                    let mut state = self.state.write().unwrap();
                    state.log.push(LogEntry {
                        term: current_term,
                        command: serde_json::to_string(&message).unwrap(),
                    });
                }

                for (_, peer_addr) in &peers {
                    self.send_message(peer_addr, message.clone()).await;
                }
            }
            RaftState::Candidate => {
                // use tokio to asyncronously send msgs

                for (_, peer_addr) in &peers {
                    self.send_message(peer_addr, message.clone()).await;
                }
            }
            _ => {
                if let Some(leader_id) = peers.get(&self.id) {
                    self.send_message(leader_id, message).await;
                } else {
                    println!("No leader found");
                }
            }
        }
    }

    async fn send_message(
        &self,
        addr: &str,
        message: RaftMessage,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(Duration::new(5, 0))
            .build()
            .unwrap();

        let path = match message {
            RaftMessage::RequestVote(_) => "/request_vote",
            RaftMessage::VoteResponse(_) => "/handle_vote",
            RaftMessage::LogRequest(_) => "/append_entries",
            RaftMessage::LogResponse(_) => "/log_ack",
        };

        let uri: Url = format!("http://{}{}", addr, path).parse().unwrap();

        let json = to_vec(&message).unwrap();

        let req = client.post(uri).body(json).build().unwrap();

        return client.execute(req).await;
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

        let res = self.send_message(&peer_addr, message).await;

        return res;
    }
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

    let server = Arc::new(RaftServer::new(port.to_string(), peers));

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

    // let log_ack_route = warp::path("log_ack")
    //     .and(warp::post())
    //     .and(warp::body::json())
    //     .and(server_filter.clone())
    //     .and_then(|req: RaftMessage, server: Arc<RaftServer>| async move {
    //         if let RaftMessage::LogResponse(args) = req {
    //             let response = server.handle_log_ack(args);

    //             Ok::<_, warp::Rejection>(warp::reply::json(&response))
    //         } else {
    //             Ok::<_, warp::Rejection>(warp::reply::json(&json!({"error": "Invalid message"})))
    //         }
    //     });

    let routes = request_vote_route
        .or(append_entries_route)
        .with(warp::log("raft_server"));

    let raft_task = tokio::spawn(async move {
        server.run().await;
    });

    let server = warp::serve(routes).run(([127, 0, 0, 1], port));

    tokio::select! {
        _ = server => println!("Server task completed"),
        _ = raft_task => println!("Raft task completed"),
    }
}
