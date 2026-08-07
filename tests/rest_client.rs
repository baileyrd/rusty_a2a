//! `RestClient` driven against a real `AgentServer` over a real socket.
//!
//! `tests/integration.rs` already exercises the REST *binding* with a bare
//! `reqwest::Client`, which is what proves the server's routes and error
//! shapes. These tests are about the *client*: that it addresses those routes
//! correctly and reads what comes back — including the `google.rpc.Status`
//! error body, which is the one thing it has to decode differently from the
//! JSON-RPC client.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use rusty_a2a::client::{ClientError, RestClient};
use rusty_a2a::error::{A2aError, Result};
use rusty_a2a::server::{AgentExecutor, AgentServer, EventSink, RequestContext};
use rusty_a2a::types::{
    AgentCard, AgentInterface, Artifact, ListTasksRequest, Message, Part, SendMessageConfiguration,
    StreamResponse, TaskPushNotificationConfig, TaskState,
};

struct TestAgent;

#[async_trait]
impl AgentExecutor for TestAgent {
    async fn execute(&self, ctx: RequestContext, events: EventSink) -> Result<()> {
        let text = ctx.message.text();
        events.status(TaskState::Working);

        if text.contains("wait") {
            ctx.cancellation.cancelled().await;
            events.status(TaskState::Canceled);
            return Ok(());
        }
        if text.contains("clarify") {
            events.message(Message::agent_text("what did you mean?"));
            return Ok(());
        }

        events.artifact(Artifact::new("result", vec![Part::text("42")]));
        events.status_with_message(TaskState::Completed, Some(Message::agent_text("done")));
        Ok(())
    }
}

/// Serves an agent declaring an `HTTP+JSON` interface, and returns a client.
async fn client() -> (RestClient, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());

    let card = AgentCard::new(
        "REST Test Agent",
        "An A2A agent used for RestClient's tests.",
        "0.0.0",
        AgentInterface::http_json(&base_url),
    )
    .with_streaming(true)
    .with_push_notifications(true);

    let router = AgentServer::new(card, Arc::new(TestAgent)).into_router();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    for _ in 0..100 {
        if rusty_a2a::client::A2aClient::fetch_agent_card(&base_url)
            .await
            .is_ok()
        {
            let (client, _) = RestClient::discover(&base_url).await.expect("discover");
            return (client, base_url);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server never became ready");
}

#[tokio::test]
async fn discovery_picks_the_http_json_interface() {
    let (_client, base_url) = client().await;
    let card = rusty_a2a::client::A2aClient::fetch_agent_card(&base_url)
        .await
        .unwrap();
    // The card declares only HTTP+JSON, so the JSON-RPC client must decline it
    // — which is what makes picking a binding meaningful rather than incidental.
    assert!(matches!(
        rusty_a2a::client::A2aClient::from_agent_card(&card),
        Err(ClientError::NoJsonRpcInterface)
    ));
    assert!(RestClient::from_agent_card(&card).is_ok());
}

#[tokio::test]
async fn send_message_and_get_task_round_trip() {
    let (client, _) = client().await;

    let result = client
        .send_message(Message::user_text("please compute"), None)
        .await
        .unwrap();
    let task = result.as_task().expect("expected a task");
    assert_eq!(task.status.state, TaskState::Completed);
    assert_eq!(task.status.message.as_ref().unwrap().text(), "done");
    assert_eq!(task.artifacts.len(), 1);

    let fetched = client.get_task(&task.id, None).await.unwrap();
    assert_eq!(fetched.id, task.id);
}

#[tokio::test]
async fn a_bare_message_reply_creates_no_task() {
    let (client, _) = client().await;
    let result = client
        .send_message(Message::user_text("clarify please"), None)
        .await
        .unwrap();
    assert_eq!(
        result.as_message().map(|m| m.text()).as_deref(),
        Some("what did you mean?")
    );
}

#[tokio::test]
async fn streaming_yields_events_ending_in_a_terminal_status() {
    let (client, _) = client().await;

    let mut stream = client
        .send_streaming_message(Message::user_text("go"), None)
        .await
        .unwrap();

    let mut states = Vec::new();
    let mut artifacts = 0;
    while let Some(event) = stream.next().await {
        match event.unwrap() {
            StreamResponse::StatusUpdate { status_update } => states.push(status_update.status.state),
            StreamResponse::ArtifactUpdate { .. } => artifacts += 1,
            _ => {}
        }
    }
    assert_eq!(states, vec![TaskState::Working, TaskState::Completed]);
    assert_eq!(artifacts, 1);
}

#[tokio::test]
async fn a_missing_task_comes_back_as_the_right_error_variant() {
    let (client, _) = client().await;
    // REST answers with a google.rpc.Status body and a 404; the client has to
    // read the ErrorInfo reason to recover the specific variant rather than
    // collapsing everything under the HTTP status.
    let error = client.get_task("no-such-task", None).await.unwrap_err();
    match error {
        ClientError::Protocol(A2aError::TaskNotFound(_)) => {}
        other => panic!("expected TaskNotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn cancel_stops_a_waiting_task() {
    let (client, _) = client().await;

    let result = client
        .send_message(
            Message::user_text("wait for me"),
            Some(SendMessageConfiguration {
                return_immediately: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let task = result.as_task().unwrap();

    let canceled = client.cancel_task(&task.id).await.unwrap();
    assert_eq!(canceled.status.state, TaskState::Canceled);
}

#[tokio::test]
async fn list_tasks_sees_what_was_created() {
    let (client, _) = client().await;
    client
        .send_message(Message::user_text("one"), None)
        .await
        .unwrap();

    let listed = client.list_tasks(ListTasksRequest::default()).await.unwrap();
    assert!(!listed.tasks.is_empty());
}

#[tokio::test]
async fn push_notification_config_crud() {
    let (client, _) = client().await;
    let result = client.send_message(Message::user_text("go"), None).await.unwrap();
    let task_id = result.as_task().unwrap().id.clone();

    let created = client
        .create_push_notification_config(
            &task_id,
            TaskPushNotificationConfig::new("https://example.com/hook"),
        )
        .await
        .unwrap();
    let config_id = created.id.clone().expect("server assigns an id");
    assert_eq!(created.task_id.as_deref(), Some(task_id.as_str()));

    let fetched = client
        .get_push_notification_config(&task_id, &config_id)
        .await
        .unwrap();
    assert_eq!(fetched.url, "https://example.com/hook");

    let listed = client.list_push_notification_configs(&task_id).await.unwrap();
    assert_eq!(listed.configs.len(), 1);

    // Delete answers 204 No Content, so there is no body to decode.
    client
        .delete_push_notification_config(&task_id, &config_id)
        .await
        .unwrap();
    assert!(client
        .list_push_notification_configs(&task_id)
        .await
        .unwrap()
        .configs
        .is_empty());
}

#[tokio::test]
async fn subscribing_to_a_terminal_task_is_refused_before_streaming() {
    let (client, _) = client().await;
    let result = client.send_message(Message::user_text("go"), None).await.unwrap();
    let task_id = result.as_task().unwrap().id.clone();

    // The task already completed, so the server refuses with an ordinary JSON
    // error rather than opening a stream. Handing that to an SSE parser would
    // look like a stream that produced nothing.
    // (The stream type is not `Debug`, so match rather than `unwrap_err`.)
    match client.subscribe_to_task(&task_id).await {
        Err(ClientError::Protocol(_)) => {}
        Err(other) => panic!("expected a protocol error, got {other:?}"),
        Ok(_) => panic!("expected the subscription to be refused"),
    }
}
