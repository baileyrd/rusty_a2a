//! A client for the gRPC protocol binding (spec Section 10).
//!
//! The same eleven operations as [`A2aClient`](super::A2aClient) and
//! [`RestClient`](super::RestClient), over the `A2AService` generated from the
//! vendored `spec/a2a.proto`. Building this — like anything else with the
//! `grpc` feature — needs a `protoc` binary on `PATH`.
//!
//! ```no_run
//! # async fn run() -> rusty_a2a::client::Result<()> {
//! use rusty_a2a::client::GrpcClient;
//! use rusty_a2a::types::Message;
//!
//! let client = GrpcClient::connect("http://localhost:50051").await?;
//! let result = client.send_message(Message::user_text("hello!"), None).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Errors
//!
//! A remote failure arrives as a [`tonic::Status`], which carries a code and a
//! message but no `ErrorInfo` detail — so the A2A error is reconstructed from
//! the status code alone. That is lossy in one direction the other bindings
//! are not: `FAILED_PRECONDITION` covers five distinct A2A errors, and they
//! all come back as [`A2aError::UnsupportedOperation`]. The message is
//! preserved verbatim, so nothing is lost that a human reads — only the
//! variant a `match` would pick.
//!
//! [`A2aError::UnsupportedOperation`]: crate::error::A2aError::UnsupportedOperation

use std::pin::Pin;

use futures_core::Stream;
use futures_util::StreamExt;
use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::error::A2aError;
use crate::grpc::convert::{
    our_cancel_task_request_to_pb, our_delete_push_config_request_to_pb, our_get_push_config_request_to_pb,
    our_get_task_request_to_pb, our_list_push_configs_request_to_pb, our_list_tasks_request_to_pb,
    our_push_config_to_pb, our_send_message_request_to_pb, our_subscribe_request_to_pb,
    pb_agent_card_to_ours, pb_list_push_configs_response_to_ours, pb_list_tasks_response_to_ours,
    pb_push_config_to_ours, pb_send_response_to_ours, pb_stream_response_to_ours, pb_task_to_ours,
};
use crate::grpc::pb;
use crate::types::{
    AgentCard, CancelTaskRequest, DeleteTaskPushNotificationConfigRequest,
    GetTaskPushNotificationConfigRequest, GetTaskRequest, ListTaskPushNotificationConfigsRequest,
    ListTaskPushNotificationConfigsResponse, ListTasksRequest, ListTasksResponse, Message,
    SendMessageConfiguration, SendMessageRequest, SendMessageResult, StreamResponse, SubscribeToTaskRequest,
    Task, TaskPushNotificationConfig,
};

use super::{ClientError, Result};

type Inner = pb::a2a_service_client::A2aServiceClient<Channel>;

/// A client for one A2A agent interface, speaking the gRPC binding.
#[derive(Clone)]
pub struct GrpcClient {
    inner: Inner,
    tenant: Option<String>,
    bearer_token: Option<String>,
    protocol_version: String,
    extensions: Vec<String>,
}

impl GrpcClient {
    /// Connects to a gRPC endpoint.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let inner = Inner::connect(endpoint.into())
            .await
            .map_err(|e| ClientError::Stream(format!("gRPC connect failed: {e}")))?;
        Ok(Self::with_channel(inner))
    }

    /// Wraps an already-built generated client.
    ///
    /// Use this when the channel needs TLS, timeouts, or an interceptor.
    pub fn with_channel(inner: Inner) -> Self {
        GrpcClient {
            inner,
            tenant: None,
            bearer_token: None,
            protocol_version: crate::PROTOCOL_VERSION.to_string(),
            extensions: Vec::new(),
        }
    }

    /// Sets a bearer token sent with every request.
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    /// Sets the `tenant` sent with every request.
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Sets the `a2a-extensions` metadata entry (spec Section 3.2.6).
    pub fn with_extensions(mut self, extensions: Vec<String>) -> Self {
        self.extensions = extensions;
        self
    }

    /// Overrides the `a2a-version` metadata entry.
    pub fn with_protocol_version(mut self, version: impl Into<String>) -> Self {
        self.protocol_version = version.into();
        self
    }

    /// Wraps a message with this client's service-parameter metadata.
    ///
    /// gRPC metadata keys are ASCII-lowercase by convention, and the server
    /// looks them up that way.
    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        let metadata = request.metadata_mut();
        if let Ok(value) = self.protocol_version.parse() {
            metadata.insert("a2a-version", value);
        }
        if !self.extensions.is_empty() {
            if let Ok(value) = self.extensions.join(",").parse() {
                metadata.insert("a2a-extensions", value);
            }
        }
        if let Some(token) = &self.bearer_token {
            if let Ok(value) = format!("Bearer {token}").parse() {
                metadata.insert("authorization", value);
            }
        }
        request
    }

    /// `SendMessage` (spec Section 10).
    pub async fn send_message(
        &self,
        message: Message,
        configuration: Option<SendMessageConfiguration>,
    ) -> Result<SendMessageResult> {
        let req = our_send_message_request_to_pb(SendMessageRequest {
            tenant: self.tenant.clone(),
            message,
            configuration,
            metadata: None,
        });
        let response = self
            .inner
            .clone()
            .send_message(self.request(req))
            .await
            .map_err(status_to_error)?;
        pb_send_response_to_ours(response.into_inner()).map_err(status_to_error)
    }

    /// `SendStreamingMessage` (spec Section 10).
    pub async fn send_streaming_message(
        &self,
        message: Message,
        configuration: Option<SendMessageConfiguration>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamResponse>> + Send>>> {
        let req = our_send_message_request_to_pb(SendMessageRequest {
            tenant: self.tenant.clone(),
            message,
            configuration,
            metadata: None,
        });
        let response = self
            .inner
            .clone()
            .send_streaming_message(self.request(req))
            .await
            .map_err(status_to_error)?;
        Ok(map_stream(response.into_inner()))
    }

    /// `GetTask` (spec Section 10).
    pub async fn get_task(&self, id: impl Into<String>, history_length: Option<i32>) -> Result<Task> {
        let req = our_get_task_request_to_pb(GetTaskRequest {
            tenant: self.tenant.clone(),
            id: id.into(),
            history_length,
        });
        let response = self
            .inner
            .clone()
            .get_task(self.request(req))
            .await
            .map_err(status_to_error)?;
        pb_task_to_ours(response.into_inner()).map_err(status_to_error)
    }

    /// `ListTasks` (spec Section 10).
    pub async fn list_tasks(&self, mut req: ListTasksRequest) -> Result<ListTasksResponse> {
        req.tenant = self.tenant.clone();
        let response = self
            .inner
            .clone()
            .list_tasks(self.request(our_list_tasks_request_to_pb(req)))
            .await
            .map_err(status_to_error)?;
        pb_list_tasks_response_to_ours(response.into_inner()).map_err(status_to_error)
    }

    /// `CancelTask` (spec Section 10).
    pub async fn cancel_task(&self, id: impl Into<String>) -> Result<Task> {
        let req = our_cancel_task_request_to_pb(CancelTaskRequest {
            tenant: self.tenant.clone(),
            id: id.into(),
            metadata: None,
        });
        let response = self
            .inner
            .clone()
            .cancel_task(self.request(req))
            .await
            .map_err(status_to_error)?;
        pb_task_to_ours(response.into_inner()).map_err(status_to_error)
    }

    /// `SubscribeToTask` (spec Section 10).
    pub async fn subscribe_to_task(
        &self,
        id: impl Into<String>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamResponse>> + Send>>> {
        let req = our_subscribe_request_to_pb(SubscribeToTaskRequest {
            tenant: self.tenant.clone(),
            id: id.into(),
        });
        let response = self
            .inner
            .clone()
            .subscribe_to_task(self.request(req))
            .await
            .map_err(status_to_error)?;
        Ok(map_stream(response.into_inner()))
    }

    /// `CreateTaskPushNotificationConfig` (spec Section 10).
    pub async fn create_push_notification_config(
        &self,
        mut config: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig> {
        config.tenant = self.tenant.clone();
        let response = self
            .inner
            .clone()
            .create_task_push_notification_config(self.request(our_push_config_to_pb(config)))
            .await
            .map_err(status_to_error)?;
        Ok(pb_push_config_to_ours(response.into_inner()))
    }

    /// `GetTaskPushNotificationConfig` (spec Section 10).
    pub async fn get_push_notification_config(
        &self,
        task_id: impl Into<String>,
        id: impl Into<String>,
    ) -> Result<TaskPushNotificationConfig> {
        let req = our_get_push_config_request_to_pb(GetTaskPushNotificationConfigRequest {
            tenant: self.tenant.clone(),
            task_id: task_id.into(),
            id: id.into(),
        });
        let response = self
            .inner
            .clone()
            .get_task_push_notification_config(self.request(req))
            .await
            .map_err(status_to_error)?;
        Ok(pb_push_config_to_ours(response.into_inner()))
    }

    /// `ListTaskPushNotificationConfigs` (spec Section 10).
    pub async fn list_push_notification_configs(
        &self,
        task_id: impl Into<String>,
    ) -> Result<ListTaskPushNotificationConfigsResponse> {
        let req = our_list_push_configs_request_to_pb(ListTaskPushNotificationConfigsRequest {
            tenant: self.tenant.clone(),
            task_id: task_id.into(),
            page_size: None,
            page_token: None,
        });
        let response = self
            .inner
            .clone()
            .list_task_push_notification_configs(self.request(req))
            .await
            .map_err(status_to_error)?;
        Ok(pb_list_push_configs_response_to_ours(response.into_inner()))
    }

    /// `DeleteTaskPushNotificationConfig` (spec Section 10). Returns
    /// `google.protobuf.Empty`, so there is nothing to decode.
    pub async fn delete_push_notification_config(
        &self,
        task_id: impl Into<String>,
        id: impl Into<String>,
    ) -> Result<()> {
        let req = our_delete_push_config_request_to_pb(DeleteTaskPushNotificationConfigRequest {
            tenant: self.tenant.clone(),
            task_id: task_id.into(),
            id: id.into(),
        });
        self.inner
            .clone()
            .delete_task_push_notification_config(self.request(req))
            .await
            .map_err(status_to_error)?;
        Ok(())
    }

    /// `GetExtendedAgentCard` (spec Section 10).
    pub async fn get_extended_agent_card(&self) -> Result<AgentCard> {
        let req = pb::GetExtendedAgentCardRequest {
            tenant: self.tenant.clone().unwrap_or_default(),
        };
        let response = self
            .inner
            .clone()
            .get_extended_agent_card(self.request(req))
            .await
            .map_err(status_to_error)?;
        pb_agent_card_to_ours(response.into_inner()).map_err(status_to_error)
    }
}

/// Turns a streamed `pb::StreamResponse` into this crate's own type.
fn map_stream(
    stream: tonic::Streaming<pb::StreamResponse>,
) -> Pin<Box<dyn Stream<Item = Result<StreamResponse>> + Send>> {
    Box::pin(stream.map(|item| {
        item.map_err(status_to_error)
            .and_then(|r| pb_stream_response_to_ours(r).map_err(status_to_error))
    }))
}

/// Reconstructs an [`A2aError`] from a gRPC status.
///
/// The code is all there is to go on — gRPC carries no `ErrorInfo` detail —
/// so this is coarser than the other two bindings; see the module docs.
fn status_to_error(status: Status) -> ClientError {
    let name = match status.code() {
        tonic::Code::NotFound => "NOT_FOUND",
        tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
        tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
        tonic::Code::Unauthenticated => "UNAUTHENTICATED",
        tonic::Code::PermissionDenied => "PERMISSION_DENIED",
        tonic::Code::Unimplemented => "UNIMPLEMENTED",
        _ => "INTERNAL",
    };
    ClientError::Protocol(A2aError::from_grpc_status_name(
        name,
        status.message().to_string(),
    ))
}
