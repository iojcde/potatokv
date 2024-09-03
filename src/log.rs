use async_recursion::async_recursion;
use serde::{Deserialize, Serialize};

use crate::{kv, LogRequestArgs, RaftMessage, RaftServer};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub command: kv::Command,
}

impl RaftServer { 
    pub async fn replicate_logs(&self) {
        let peer_ids = {
            let mut state = self.state.write().unwrap();
            state.timer_override = false;

            state.peers.keys().cloned().collect::<Vec<_>>()
        };

        println!("Replicating logs");

        for id in peer_ids.into_iter() {
            let msg = {
                let state = self.state.read().unwrap();

                let prev_log_index = state.sent_length[&id];
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

            let addr = {
                let state = self.state.read().unwrap();
                state.peers.get(&id).unwrap().clone()
            };

            let res = Self::send_message(&addr, RaftMessage::LogRequest(msg)).await;

            match res {
                Ok(msg) => {
                    let log_response =
                        serde_json::from_str(msg.text().await.unwrap().as_str()).unwrap();

                    async move {
                        self.handle_log_ack(log_response).await;
                    };
                }
                Err(e) => {
                    // println!("Error sending log request to {}: {:?}", id, e);
                }
            }
        }
    }

    fn acks(&self, length: u64) -> usize {
        let state = self.state.read().unwrap();
        state
            .peers
            .iter()
            .filter(|(&ref id, _)| *state.acked_length.get(id).unwrap() >= length)
            .count()
    }

    pub fn commit_log_entries(&self) {
        let ready = {
            let state = self.state.read().unwrap();
            let min_acks = (state.peers.len() + 1) / 2 + 1;
            let ready = (1..=state.log.len() as u64)
                .filter(|&len| self.acks(len) >= min_acks)
                .collect::<Vec<u64>>();

            ready
        };

        if !ready.is_empty() {
            let max_ready = *ready.iter().max().unwrap();
            let mut state = self.state.write().unwrap();
            if max_ready > state.commit_length
                && state.log[(max_ready - 1) as usize].term == state.current_term
            {
                for i in state.commit_length..max_ready {
                    self.state_machine
                        .lock()
                        .unwrap()
                        .apply(&state.log[i as usize], i);
                }
                state.commit_length = max_ready;

                self.storage
                    .save_kv_store(&self.state_machine.lock().unwrap().data);
            }
        }
    }
}
