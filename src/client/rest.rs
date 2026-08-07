//! A client for the HTTP+JSON/REST protocol binding (spec Section 11).
//!
//! The same eleven operations as [`A2aClient`](super::A2aClient), addressed
//! as REST resources instead of JSON-RPC methods. Which binding to use is the
//! agent's choice to declare and the caller's to pick: an `AgentCard` may list
//! several, and [`RestClient::from_agent_card`] takes the `HTTP+JSON` one.
//!
//! Two things differ from the JSON-RPC binding and are visible here:
//!
//! - Errors arrive as [`google.rpc.Status`][status] JSON with a real HTTP
//!   status code, not a JSON-RPC error object. They are decoded back into
//!   [`A2aError`] through the `ErrorInfo.reason` detail, so a caller matches
//!   on the same variants either way.
//! - Server-sent events carry a bare `StreamResponse` in `data:`, with no
//!   JSON-RPC envelope around it (spec Section 11.7).
//!
//! [status]: https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto
//!
//! ```no_run
//! # async fn run() -> rusty_a2a::client::Result<()> {
//! use rusty_a2a::client::RestClient;
//! use rusty_a2a::types::Message;
//!
//! let (client, _card) = RestClient::discover("https://agent.example.com").await?;
//! let result = client.send_message(Message::user_text("hello!"), None).await?;
//! # Ok(())
//! # }
//! ```

use std::pin::Pin;

use eventsource_stream::Eventsource;
use futures_core::Stream;
use futures_util::StreamExt;
use reqwest::{Method, RequestBuilder};
use serde::de::DeserializeOwned;

use crate::error::A2aError;
use crate::types::{
    AgentCard, AgentInterface, ListTaskPushNotificationConfigsResponse, ListTasksRequest, ListTasksResponse,
    Message, SendMessageConfiguration, SendMessageRequest, SendMessageResult, StreamResponse, Task,
    TaskPushNotificationConfig,
};

use super::{ClientError, Result};

/// A client for one A2A agent interface, speaking the HTTP+JSON/REST binding.
pub struct RestClient {
    http: reqwest::Client,
    base_url: String,
    tenant: Option<String>,
    bearer_token: Option<String>,
    protocol_version: String,
    extensions: Vec<String>,
}

impl RestClient {
    /// Builds a client targeting a REST base URL directly.
    ///
    /// Prefer [`RestClient::discover`] or [`RestClient::from_agent_card`]
    /// when the agent's card is available.
    pub fn new(base_url: impl Into<String>) -> Self {
        RestClient {
            http: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            tenant: None,
            bearer_token: None,
            protocol_version: crate::PROTOCOL_VERSION.to_string(),
            extensions: Vec::new(),
        }
    }

    /// Like [`RestClient::new`], with a caller-provided [`reqwest::Client`].
    pub fn with_http_client(base_url: impl Into<String>, http: reqwest::Client) -> Self {
        RestClient {
            http,
            ..RestClient::new(base_url)
        }
    }

    /// Builds a client for the first `HTTP+JSON` interface the card declares.
    pub fn from_agent_card(card: &AgentCard) -> Result<Self> {
        let interface = card
            .interface_for_binding(AgentInterface::HTTP_JSON)
            .ok_or(ClientError::NoRestInterface)?;
        let mut client = RestClient::new(interface.url.clone());
        client.tenant = interface.tenant.clone();
        Ok(client)
    }

    /// Fetches the agent's card and builds a client from it.
    pub async fn discover(base_url: &str) -> Result<(Self, AgentCard)> {
        let card = super::A2aClient::fetch_agent_card(base_url).await?;
        let client = Self::from_agent_card(&card)?;
        Ok((client, card))
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

    /// Sets the `A2A-Extensions` service parameter (spec Section 3.2.6).
    pub fn with_extensions(mut self, extensions: Vec<String>) -> Self {
        self.extensions = extensions;
        self
    }

    /// Overrides the `A2A-Version` service parameter.
    pub fn with_protocol_version(mut self, version: impl Into<String>) -> Self {
        self.protocol_version = version.into();
        self
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        let mut builder = self
            .http
            .request(method, self.url(path))
            .header("A2A-Version", &self.protocol_version);
        if !self.extensions.is_empty() {
            builder = builder.header("A2A-Extensions", self.extensions.join(","));
        }
        if let Some(token) = &self.bearer_token {
            builder = builder.bearer_auth(token);
        }
        builder
    }

    /// Reads a `google.rpc.Status` error body back into an [`A2aError`].
    ///
    /// The `ErrorInfo.reason` detail is what names the A2A error precisely;
    /// the HTTP status alone would collapse several distinct errors into one.
    /// A body that is not shaped like a `Status` at all is reported as an
    /// unexpected response rather than guessed at.
    async fn error_from(resp: reqwest::Response) -> ClientError {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) else {
            return ClientError::UnexpectedResponse {
                status: status.as_u16(),
                body,
            };
        };
        let Some(error) = value.get("error") else {
            return ClientError::UnexpectedResponse {
                status: status.as_u16(),
                body,
            };
        };
        let message = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or_default()
            .to_string();
        let reason = error
            .get("details")
            .and_then(|d| d.as_array())
            .and_then(|d| d.first())
            .and_then(|d| d.get("reason"))
            .and_then(|r| r.as_str());

        match reason {
            Some(reason) => ClientError::Protocol(A2aError::from_reason(reason, message)),
            // No ErrorInfo detail: fall back to the status name, which is
            // coarser but still better than discarding the error.
            None => {
                let name = error.get("status").and_then(|s| s.as_str()).unwrap_or("INTERNAL");
                ClientError::Protocol(A2aError::from_grpc_status_name(name, message))
            }
        }
    }

    async fn send<T: DeserializeOwned>(&self, builder: RequestBuilder) -> Result<T> {
        let resp = builder.send().await?;
        if !resp.status().is_success() {
            return Err(Self::error_from(resp).await);
        }
        let bytes = resp.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn send_no_content(&self, builder: RequestBuilder) -> Result<()> {
        let resp = builder.send().await?;
        if !resp.status().is_success() {
            return Err(Self::error_from(resp).await);
        }
        Ok(())
    }

    /// Opens an SSE stream, or reports the error the server answered with.
    async fn stream(
        &self,
        builder: RequestBuilder,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamResponse>> + Send>>> {
        let resp = builder.send().await?;
        if !resp.status().is_success() {
            return Err(Self::error_from(resp).await);
        }
        // A request refused before streaming starts answers with an ordinary
        // JSON body. Handing that to an SSE parser would look like an empty
        // stream, which is indistinguishable from a run that produced nothing.
        let status = resp.status();
        let is_event_stream = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with("text/event-stream"));
        if !is_event_stream {
            let body = resp.text().await.unwrap_or_default();
            return Err(ClientError::UnexpectedResponse {
                status: status.as_u16(),
                body: format!("expected an SSE stream, got: {body}"),
            });
        }

        let events = resp.bytes_stream().eventsource();
        Ok(Box::pin(events.filter_map(|event| async move {
            let event = match event {
                Ok(e) => e,
                Err(e) => return Some(Err(ClientError::Stream(e.to_string()))),
            };
            if event.data.is_empty() {
                return None;
            }
            // REST SSE carries the bare object, with no JSON-RPC envelope.
            match serde_json::from_str::<StreamResponse>(&event.data) {
                Ok(response) => Some(Ok(response)),
                Err(e) => Some(Err(ClientError::Json(e))),
            }
        })))
    }

    /// `POST /message:send` (spec Section 11.3.1).
    pub async fn send_message(
        &self,
        message: Message,
        configuration: Option<SendMessageConfiguration>,
    ) -> Result<SendMessageResult> {
        let req = SendMessageRequest {
            tenant: self.tenant.clone(),
            message,
            configuration,
            metadata: None,
        };
        self.send(self.request(Method::POST, "/message:send").json(&req))
            .await
    }

    /// `POST /message:stream` (spec Section 11.3.1).
    pub async fn send_streaming_message(
        &self,
        message: Message,
        configuration: Option<SendMessageConfiguration>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamResponse>> + Send>>> {
        let req = SendMessageRequest {
            tenant: self.tenant.clone(),
            message,
            configuration,
            metadata: None,
        };
        self.stream(self.request(Method::POST, "/message:stream").json(&req))
            .await
    }

    /// `GET /tasks/{id}` (spec Section 11.3.2).
    pub async fn get_task(&self, id: impl AsRef<str>, history_length: Option<i32>) -> Result<Task> {
        let mut builder = self.request(Method::GET, &format!("/tasks/{}", id.as_ref()));
        if let Some(length) = history_length {
            builder = builder.query(&[("historyLength", length)]);
        }
        self.send(builder).await
    }

    /// `GET /tasks` (spec Section 11.3.2).
    pub async fn list_tasks(&self, mut req: ListTasksRequest) -> Result<ListTasksResponse> {
        req.tenant = self.tenant.clone();
        self.send(self.request(Method::GET, "/tasks").query(&req)).await
    }

    /// `POST /tasks/{id}:cancel` (spec Section 11.3.2).
    pub async fn cancel_task(&self, id: impl AsRef<str>) -> Result<Task> {
        self.send(self.request(Method::POST, &format!("/tasks/{}:cancel", id.as_ref())))
            .await
    }

    /// `POST /tasks/{id}:subscribe` (spec Section 11.3.2).
    pub async fn subscribe_to_task(
        &self,
        id: impl AsRef<str>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamResponse>> + Send>>> {
        self.stream(self.request(Method::POST, &format!("/tasks/{}:subscribe", id.as_ref())))
            .await
    }

    /// `POST /tasks/{id}/pushNotificationConfigs` (spec Section 11.3.3).
    ///
    /// The task id comes from the path, so `config.task_id` is set from
    /// `task_id` rather than being trusted to already match it.
    pub async fn create_push_notification_config(
        &self,
        task_id: impl AsRef<str>,
        mut config: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig> {
        config.tenant = self.tenant.clone();
        config.task_id = Some(task_id.as_ref().to_string());
        self.send(
            self.request(
                Method::POST,
                &format!("/tasks/{}/pushNotificationConfigs", task_id.as_ref()),
            )
            .json(&config),
        )
        .await
    }

    /// `GET /tasks/{id}/pushNotificationConfigs/{configId}` (spec Section 11.3.3).
    pub async fn get_push_notification_config(
        &self,
        task_id: impl AsRef<str>,
        id: impl AsRef<str>,
    ) -> Result<TaskPushNotificationConfig> {
        self.send(self.request(
            Method::GET,
            &format!(
                "/tasks/{}/pushNotificationConfigs/{}",
                task_id.as_ref(),
                id.as_ref()
            ),
        ))
        .await
    }

    /// `GET /tasks/{id}/pushNotificationConfigs` (spec Section 11.3.3).
    pub async fn list_push_notification_configs(
        &self,
        task_id: impl AsRef<str>,
    ) -> Result<ListTaskPushNotificationConfigsResponse> {
        self.send(self.request(
            Method::GET,
            &format!("/tasks/{}/pushNotificationConfigs", task_id.as_ref()),
        ))
        .await
    }

    /// `DELETE /tasks/{id}/pushNotificationConfigs/{configId}` (spec Section
    /// 11.3.3). The server answers `204 No Content`, so there is nothing to
    /// decode.
    pub async fn delete_push_notification_config(
        &self,
        task_id: impl AsRef<str>,
        id: impl AsRef<str>,
    ) -> Result<()> {
        self.send_no_content(self.request(
            Method::DELETE,
            &format!(
                "/tasks/{}/pushNotificationConfigs/{}",
                task_id.as_ref(),
                id.as_ref()
            ),
        ))
        .await
    }

    /// `GET /extendedAgentCard` (spec Section 11.3.4).
    pub async fn get_extended_agent_card(&self) -> Result<AgentCard> {
        self.send(self.request(Method::GET, "/extendedAgentCard")).await
    }
}
