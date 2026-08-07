//! `GrpcClient` driven against a real `AgentServices::serve_grpc`.
//!
//! `tests/grpc_integration.rs` drives the same server with the raw generated
//! `tonic` client, which is what proves the service wiring. These tests are
//! about the *client*: that it builds the proto requests correctly and — the
//! part with the most surface — converts every response back into this crate's
//! own types.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use rusty_a2a::client::{ClientError, GrpcClient};
use rusty_a2a::error::{A2aError, Result};
use rusty_a2a::server::{AgentExecutor, AgentServer, EventSink, RequestContext};
use rusty_a2a::types::{
    AgentCard, AgentInterface, AgentSkill, Artifact, ListTasksRequest, Message, Part,
    SendMessageConfiguration, StreamResponse, TaskPushNotificationConfig, TaskState,
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

        events.artifact(Artifact::new("result", vec![Part::text("42")]).with_name("the result"));
        events.status_with_message(TaskState::Completed, Some(Message::agent_text("done")));
        Ok(())
    }
}

/// The card served over gRPC, rich enough that converting it back exercises
/// the nested shapes — capabilities, skills, signatures — not just the
/// scalar fields.
fn card(url: &str) -> AgentCard {
    AgentCard::new(
        "gRPC Test Agent",
        "An A2A agent used for GrpcClient's tests.",
        "0.0.0",
        AgentInterface::json_rpc(url),
    )
    .with_streaming(true)
    .with_push_notifications(true)
    .with_skill(AgentSkill::new("compute", "Compute", "Computes things.").with_tags(["math", "demo"]))
}

/// The base card plus the capability flag that makes `GetExtendedAgentCard`
/// available; the server refuses the call without it.
fn card_advertising_an_extended_one(url: &str) -> AgentCard {
    let mut card = card(url);
    card.capabilities.extended_agent_card = Some(true);
    card
}

async fn client() -> GrpcClient {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let url = format!("http://127.0.0.1:{port}");

    let base = card_advertising_an_extended_one(&url);
    let services = AgentServer::new(base.clone(), Arc::new(TestAgent))
        .with_extended_card(base)
        .build();
    let bind_url = url.clone();
    tokio::spawn(async move {
        let _ = services.serve_grpc(([127, 0, 0, 1], port)).await;
        let _ = bind_url;
    });

    for _ in 0..100 {
        if let Ok(client) = GrpcClient::connect(url.clone()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("gRPC server never became ready");
}

#[tokio::test]
async fn send_message_and_get_task_round_trip() {
    let client = client().await;

    let result = client
        .send_message(Message::user_text("please compute"), None)
        .await
        .unwrap();
    let task = result.as_task().expect("expected a task");
    assert_eq!(task.status.state, TaskState::Completed);
    assert_eq!(task.status.message.as_ref().unwrap().text(), "done");

    // Nested conversion: the artifact and its parts survived proto and back.
    assert_eq!(task.artifacts.len(), 1);
    assert_eq!(task.artifacts[0].artifact_id, "result");
    assert_eq!(task.artifacts[0].name.as_deref(), Some("the result"));
    assert_eq!(task.artifacts[0].parts[0].as_text(), Some("42"));

    let fetched = client.get_task(&task.id, None).await.unwrap();
    assert_eq!(fetched.id, task.id);
    assert_eq!(fetched.status.state, TaskState::Completed);
}

#[tokio::test]
async fn a_bare_message_reply_creates_no_task() {
    let client = client().await;
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
    let client = client().await;

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
async fn a_missing_task_comes_back_as_an_error() {
    let client = client().await;
    let error = client.get_task("no-such-task", None).await.unwrap_err();
    // gRPC carries a code but no ErrorInfo detail, and NOT_FOUND is
    // unambiguous, so this one variant does survive the trip.
    match error {
        ClientError::Protocol(A2aError::TaskNotFound(_)) => {}
        other => panic!("expected TaskNotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn cancel_stops_a_waiting_task() {
    let client = client().await;

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
    let client = client().await;
    client
        .send_message(Message::user_text("one"), None)
        .await
        .unwrap();

    let listed = client.list_tasks(ListTasksRequest::default()).await.unwrap();
    assert!(!listed.tasks.is_empty());
    assert_eq!(listed.tasks[0].status.state, TaskState::Completed);
}

#[tokio::test]
async fn push_notification_config_crud() {
    let client = client().await;
    let result = client.send_message(Message::user_text("go"), None).await.unwrap();
    let task_id = result.as_task().unwrap().id.clone();

    let mut config = TaskPushNotificationConfig::new("https://example.com/hook");
    config.task_id = Some(task_id.clone());
    let created = client.create_push_notification_config(config).await.unwrap();
    let config_id = created.id.clone().expect("server assigns an id");

    let fetched = client
        .get_push_notification_config(&task_id, &config_id)
        .await
        .unwrap();
    assert_eq!(fetched.url, "https://example.com/hook");

    let listed = client.list_push_notification_configs(&task_id).await.unwrap();
    assert_eq!(listed.configs.len(), 1);

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

/// The largest conversion in the crate, and the only one that had no inbound
/// direction before: a whole `AgentCard` decoded back out of proto.
#[tokio::test]
async fn the_extended_agent_card_converts_back_whole() {
    let client = client().await;
    let card = client.get_extended_agent_card().await.unwrap();

    assert_eq!(card.name, "gRPC Test Agent");
    assert_eq!(card.description, "An A2A agent used for GrpcClient's tests.");
    assert_eq!(card.version, "0.0.0");
    assert_eq!(card.capabilities.streaming, Some(true));
    assert_eq!(card.capabilities.push_notifications, Some(true));
    assert_eq!(card.capabilities.extended_agent_card, Some(true));

    assert_eq!(card.supported_interfaces.len(), 1);
    assert_eq!(
        card.supported_interfaces[0].protocol_binding,
        AgentInterface::JSONRPC
    );
    // A proto empty string is "unset", not an empty tenant.
    assert!(card.supported_interfaces[0].tenant.is_none());

    assert_eq!(card.skills.len(), 1);
    assert_eq!(card.skills[0].id, "compute");
    assert_eq!(card.skills[0].tags, vec!["math", "demo"]);
    assert_eq!(card.default_input_modes, vec!["text/plain"]);
}
