use std::time::Instant;

use crate::{
    LogRequestArgs, LogResponseArgs, RaftRole, RaftServer, RequestVoteArgs, VoteResponseArgs,
};

impl RaftServer {
    pub fn handle_request_vote(&self, req: RequestVoteArgs) -> VoteResponseArgs {
        let mut state = self.state.write().unwrap();
        println!("Received vote request");

        if req.term > state.current_term {
            state.current_term = req.term;
            state.role = RaftRole::Follower;
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

    pub fn handle_log_request(&self, req: LogRequestArgs) -> LogResponseArgs {
        let mut state = self.state.write().unwrap();

        println!("Received log request");

        state.last_heartbeat = Instant::now();

        // If the leader's term is greater, update our term and become a follower
        if req.term > state.current_term {
            state.current_term = req.term;
            state.role = RaftRole::Follower;
            state.voted_for = None;
            state.current_leader = Some(req.leader_id.clone());

            drop(state);

            state = self.state.write().unwrap();
        }

        // If we're in the same term, recognize the leader
        if req.term == state.current_term {
            state.role = RaftRole::Follower;
            state.current_leader = Some(req.leader_id.clone());
        }

        // Check if our log is consistent with the leader's
        let log_ok = state.log.len() as u64 >= req.prev_log_index
            && (req.prev_log_index == 0
                || state.log[req.prev_log_index as usize - 1].term == req.prev_log_term);

        if req.term == state.current_term && log_ok {
            // Our log is consistent, so append the new entries

            state.log.append(&mut req.entries.clone());

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
            println!("shit!!!"); // we should never end up here

            // Our log is inconsistent or the term doesn't match
            LogResponseArgs {
                id: self.id.clone(),
                term: state.current_term,
                ack: state.log.len() as u64,
                success: false,
            }
        }
    }

    pub async fn handle_vote(&self, req: VoteResponseArgs) -> &str {
        let (role, current_term) = {
            let state = self.state.read().unwrap();

            (state.role, state.current_term)
        };

        println!("Received vote response {:?}", req);

        // if state is candidate
        if role == RaftRole::Candidate && req.term == current_term && req.vote_granted {
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

                    state.role = RaftRole::Leader;
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

                {
                    let mut state = self.state.write().unwrap();
                    state.timer_override = true;
                }

                return "ok";
            }
        } else if req.term > current_term {
            {
                let mut state = self.state.write().unwrap();

                state.current_term = req.term;
                state.role = RaftRole::Follower;
                state.voted_for = None;
                state.current_leader = Some(req.id.clone());
            }

            return "ok";
        }

        println!("shit");
        return "shit";
    }

    pub async fn handle_log_ack(&self, msg: LogResponseArgs) -> &str {
        let (current_term, role, sent_length, acked_length) = {
            let state = self.state.read().unwrap();

            (
                state.current_term,
                state.role.clone(),
                state.sent_length.clone(),
                state.acked_length.clone(),
            )
        };

        if msg.term == current_term && role == RaftRole::Leader {
            if msg.success && msg.ack >= acked_length[&msg.id] {
                let mut state = self.state.write().unwrap();
                *state.sent_length.get_mut(&msg.id).unwrap() = msg.ack;
                *state.acked_length.get_mut(&msg.id).unwrap() = msg.ack;

                drop(state);

                self.commit_log_entries();

                return "ok";
            } else if sent_length[&msg.id] > 0 {
                {
                    let mut state = self.state.write().unwrap();
                    *state.sent_length.get_mut(&msg.id).unwrap() -= 1;

                    state.timer_override = true;
                }

                return "ok";
            } else {
                // bad request
                return "sibal";
            }
        } else if msg.term > current_term {
            let mut state = self.state.write().unwrap();
            state.current_term = msg.term;
            state.role = RaftRole::Follower;
            state.voted_for = None;

            return "ok";
        } else {
            // bad request
            return "fuck!!!!";
        }
    }
}
