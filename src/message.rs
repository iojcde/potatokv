use std::time::Duration;

use serde_json::to_vec;
use url::Url;

use crate::{RaftMessage, RaftServer};

impl RaftServer {
    pub async fn send_message(
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
            RaftMessage::BroadcastRequest(_) => "/broadcast",
        };

        let uri: Url = format!("http://{}{}", addr, path).parse().unwrap();

        let json = to_vec(&message).unwrap();

        let req = client.post(uri).body(json).build().unwrap();

        return client.execute(req).await;
    }
}
