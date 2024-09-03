use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};

use crate::log::LogEntry;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub commit_length: u64,
}

#[derive(Debug)]
pub struct PersistentStorage {
    path: PathBuf,
    log_writer: Mutex<BufWriter<File>>,
}

impl PersistentStorage {
    pub fn new(path: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&path)?;
        let log_path = path.join("log");
        let log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&log_path)?;
        let log_writer = BufWriter::new(log_file);

        Ok(Self {
            path,
            log_writer: Mutex::new(log_writer),
        })
    }

    pub fn save_state(&self, state: &PersistentState) -> std::io::Result<()> {
        let state_path = self.path.join("state");
        let file = File::create(state_path)?;
        let mut writer = BufWriter::new(file);

        serde_json::to_writer(&mut writer, state)?;
        writer.flush()?;
        Ok(())
    }

    pub fn load_state(&self) -> std::io::Result<Option<PersistentState>> {
        let state_path = self.path.join("state");
        match File::open(state_path) {
            Ok(file) => {
                let reader = BufReader::new(file);
                let state = serde_json::from_reader(reader)?;
                Ok(Some(state))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn append_log_entries(&self, entries: &[LogEntry]) -> std::io::Result<()> {
        let mut writer = self.log_writer.lock().unwrap();

        for entry in entries {
            serde_json::to_writer(&mut *writer, entry)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        Ok(())
    }

    pub fn read_log_entries(&self) -> std::io::Result<Vec<LogEntry>> {
        let mut writer = self.log_writer.lock().unwrap();

        writer.flush()?; // Ensure all writes are flushed before reading
        writer.get_mut().seek(SeekFrom::Start(0))?;

        let reader = BufReader::new(writer.get_ref());
        reader
            .lines()
            .map(|line| -> std::io::Result<LogEntry> {
                let line = line?;
                Ok(serde_json::from_str(&line)?)
            })
            .collect()
    }

    pub fn save_kv_store(&self, store: &HashMap<String, Value>) -> std::io::Result<()> {
        let kv_path = self.path.join("kv");
        let file = File::create(kv_path)?;
        let mut writer = BufWriter::new(file);

        serde_json::to_writer(&mut writer, store)?;
        writer.flush()?;
        Ok(())
    }

    pub fn load_kv_store(&self) -> std::io::Result<HashMap<String, Value>> {
        let kv_path = self.path.join("kv");
        match File::open(kv_path) {
            Ok(file) => {
                let reader = BufReader::new(file);
                let store = serde_json::from_reader(reader)?;
                Ok(store)
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
            Err(e) => Err(e),
        }
    }

}
