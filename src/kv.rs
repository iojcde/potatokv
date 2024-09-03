use serde::{Deserialize, Serialize};

use serde_json::{to_vec, Value};
use std::collections::HashMap;

use crate::log::LogEntry;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    Put { key: String, value: Value },
    Get { key: Value },
    Delete { key: String },
}

// Define the state machine for our KV store
#[derive(Debug)]
pub struct StateMachine {
    pub data: HashMap<String, Value>,
    last_applied_index: u64,
}

impl StateMachine {
    pub fn new() -> Self {
        StateMachine {
            data: HashMap::new(),
            last_applied_index: 0,
        }
    }

    pub fn apply(&mut self, entry: &LogEntry, index: u64) {
        match &entry.command {
            Command::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
            }
            Command::Delete { key } => {
                self.data.remove(key);
            }
            Command::Get { .. } => {} // Get doesn't modify state
        }
        self.last_applied_index = index;
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        self.data.get(key).cloned()
    }
}
