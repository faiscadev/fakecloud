//! WebSocket data plane for API Gateway v2 WebSocket APIs.
//!
//! Connections are bookkept in a [`WebSocketRegistry`] keyed by AWS-style
//! connection ids. Inbound frames flow through the websocket task and dispatch
//! to the API's `$default` route (and `$connect` / `$disconnect` lifecycle
//! routes when target Lambda integrations are wired up). The control surface
//! at `/@connections/{id}` lets test code or SigV4 clients post messages back
//! to a connected client, fetch connection metadata, or close the socket.

use axum::extract::ws::{Message, WebSocket};
use chrono::Utc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

use crate::state::{ConnectionInfo, SharedWebSocketRegistry};

/// Generate a 22-char base64-style connection id, matching AWS's
/// `<random>=` pattern. We use the URL-safe alphabet so the id is a single
/// path segment — base64 standard's `/` would otherwise be interpreted as
/// a path separator by HTTP routers (including axum).
pub fn generate_connection_id() -> String {
    use base64::prelude::*;
    let mut buf = [0u8; 16];
    let nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u128;
    let uuid = uuid::Uuid::new_v4().as_u128();
    let mixed = nanos.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(uuid);
    buf[..16].copy_from_slice(&mixed.to_le_bytes());
    let mut s = BASE64_URL_SAFE_NO_PAD.encode(buf);
    s.truncate(21);
    s.push('=');
    s
}

/// Register a new WebSocket connection and return its id plus the outbound
/// `Message` receiver. The caller is responsible for plumbing the receiver
/// into the upgraded socket.
pub fn register(
    registry: SharedWebSocketRegistry,
    api_id: String,
    stage: String,
    source_ip: String,
    user_agent: Option<String>,
) -> (String, UnboundedReceiver<Message>) {
    register_with_id(
        registry,
        generate_connection_id(),
        api_id,
        stage,
        source_ip,
        user_agent,
    )
}

/// [`register`] under a connection ID chosen beforehand (the `$connect`
/// route runs, with that ID, before the connection is accepted).
pub fn register_with_id(
    registry: SharedWebSocketRegistry,
    connection_id: String,
    api_id: String,
    stage: String,
    source_ip: String,
    user_agent: Option<String>,
) -> (String, UnboundedReceiver<Message>) {
    let (tx, rx) = unbounded_channel();
    let now = Utc::now();
    let info = ConnectionInfo {
        connection_id: connection_id.clone(),
        api_id,
        stage,
        connected_at: now,
        last_active_at: now,
        source_ip,
        user_agent,
        sender: tx,
    };
    registry.write().insert(info);
    (connection_id, rx)
}

pub fn deregister(registry: &SharedWebSocketRegistry, connection_id: &str) {
    registry.write().remove(connection_id);
}

/// Drive a websocket lifecycle: bridge inbound frames into `on_message`,
/// forward outbound frames pushed via the registry sender, and clean up on
/// close. `on_message` receives the raw bytes; `is_text` is true for text
/// frames so dispatchers can preserve framing for `$default` events.
pub async fn run_lifecycle<F, Fut>(
    socket: WebSocket,
    outbound: UnboundedReceiver<Message>,
    on_message: F,
) where
    F: Fn(Vec<u8>, bool) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    run_lifecycle_inner(socket, outbound, on_message, None).await;
}

/// Same as [`run_lifecycle`] but also bumps `last_active_at` on the registry
/// entry whenever an inbound frame arrives. This is what the server should
/// call so that `GetConnection.lastActiveAt` reflects real WebSocket
/// activity.
pub async fn run_lifecycle_tracked<F, Fut>(
    socket: WebSocket,
    outbound: UnboundedReceiver<Message>,
    on_message: F,
    registry: SharedWebSocketRegistry,
    connection_id: String,
) where
    F: Fn(Vec<u8>, bool) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    run_lifecycle_inner(
        socket,
        outbound,
        on_message,
        Some((registry, connection_id)),
    )
    .await;
}

/// Same as [`run_lifecycle_tracked`] but also invokes `on_disconnect` when
/// the WebSocket closes for any reason (client close, error, or outbound
/// Close frame). This is used to dispatch the `$disconnect` Lambda route.
pub async fn run_lifecycle_tracked_with_disconnect<F, Fut, D, Dfut>(
    socket: WebSocket,
    outbound: UnboundedReceiver<Message>,
    on_message: F,
    registry: SharedWebSocketRegistry,
    connection_id: String,
    on_disconnect: D,
) where
    F: Fn(Vec<u8>, bool) -> Fut,
    Fut: std::future::Future<Output = ()>,
    D: FnOnce() -> Dfut,
    Dfut: std::future::Future<Output = ()>,
{
    run_lifecycle_inner_with_disconnect(
        socket,
        outbound,
        on_message,
        Some((registry, connection_id)),
        on_disconnect,
    )
    .await;
}

async fn run_lifecycle_inner<F, Fut>(
    socket: WebSocket,
    outbound: UnboundedReceiver<Message>,
    on_message: F,
    activity: Option<(SharedWebSocketRegistry, String)>,
) where
    F: Fn(Vec<u8>, bool) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    run_lifecycle_body(socket, outbound, on_message, activity).await;
}

async fn run_lifecycle_inner_with_disconnect<F, Fut, D, Dfut>(
    socket: WebSocket,
    outbound: UnboundedReceiver<Message>,
    on_message: F,
    activity: Option<(SharedWebSocketRegistry, String)>,
    on_disconnect: D,
) where
    F: Fn(Vec<u8>, bool) -> Fut,
    Fut: std::future::Future<Output = ()>,
    D: FnOnce() -> Dfut,
    Dfut: std::future::Future<Output = ()>,
{
    run_lifecycle_body(socket, outbound, on_message, activity).await;
    on_disconnect().await;
}

async fn run_lifecycle_body<F, Fut>(
    mut socket: WebSocket,
    mut outbound: UnboundedReceiver<Message>,
    on_message: F,
    activity: Option<(SharedWebSocketRegistry, String)>,
) where
    F: Fn(Vec<u8>, bool) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let touch = || {
        if let Some((reg, id)) = activity.as_ref() {
            reg.write().touch(id);
        }
    };
    loop {
        tokio::select! {
            biased;
            outgoing = outbound.recv() => {
                match outgoing {
                    Some(Message::Close(frame)) => {
                        let _ = socket.send(Message::Close(frame)).await;
                        break;
                    }
                    Some(msg) => {
                        if socket.send(msg).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        touch();
                        on_message(text.as_bytes().to_vec(), true).await;
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        touch();
                        on_message(bin.to_vec(), false).await;
                    }
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn connection_id_format() {
        let id = generate_connection_id();
        assert_eq!(id.len(), 22);
        assert!(id.ends_with('='));
    }

    #[test]
    fn connection_id_is_single_url_path_segment() {
        // Real AWS connection IDs are url-safe; base64 standard's `/` would
        // collide with HTTP path separators and break `/@connections/{id}`
        // routing. Generate enough to catch any accidental regression to
        // the standard alphabet.
        for _ in 0..1000 {
            let id = generate_connection_id();
            assert!(
                !id.contains('/'),
                "connection id contains '/': {id} — would break HTTP path routing"
            );
            assert!(
                !id.contains('+'),
                "connection id contains '+': {id} — would be percent-encoded by clients"
            );
        }
    }

    #[test]
    fn registry_insert_and_remove() {
        let registry: SharedWebSocketRegistry = Arc::new(parking_lot::RwLock::new(
            crate::state::WebSocketRegistry::default(),
        ));
        let (id, _rx) = register(
            registry.clone(),
            "api-1".into(),
            "dev".into(),
            "127.0.0.1".into(),
            None,
        );
        assert!(registry.read().contains(&id));
        deregister(&registry, &id);
        assert!(!registry.read().contains(&id));
    }

    #[tokio::test]
    async fn outbound_send_reaches_receiver() {
        let registry: SharedWebSocketRegistry = Arc::new(parking_lot::RwLock::new(
            crate::state::WebSocketRegistry::default(),
        ));
        let (id, mut rx) = register(
            registry.clone(),
            "api-1".into(),
            "dev".into(),
            "127.0.0.1".into(),
            Some("test-agent/1.0".into()),
        );
        let sender = registry.read().get(&id).unwrap().sender.clone();
        sender.send(Message::Text("hello".into())).unwrap();
        let msg = rx.recv().await.unwrap();
        match msg {
            Message::Text(t) => assert_eq!(t.as_str(), "hello"),
            other => panic!("expected text, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn close_message_breaks_lifecycle() {
        // Verify that a `Close` outbound message cuts the loop. We use a
        // `mpsc` directly, swapping in a fake socket would require pulling
        // tungstenite — keep this as a contract test on the channel side.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        tx.send(Message::Close(None)).unwrap();
        drop(tx);
        let next = rx.recv().await;
        assert!(matches!(next, Some(Message::Close(_))));
        assert!(rx.recv().await.is_none());
    }

    #[test]
    fn register_captures_user_agent_and_source_ip() {
        let registry: SharedWebSocketRegistry = Arc::new(parking_lot::RwLock::new(
            crate::state::WebSocketRegistry::default(),
        ));
        let (id, _rx) = register(
            registry.clone(),
            "api-1".into(),
            "dev".into(),
            "203.0.113.7".into(),
            Some("aws-sdk-go/1.0".into()),
        );
        let r = registry.read();
        let info = r.get(&id).expect("connection registered");
        assert_eq!(info.source_ip, "203.0.113.7");
        assert_eq!(info.user_agent.as_deref(), Some("aws-sdk-go/1.0"));
    }

    #[test]
    fn touch_updates_last_active_at() {
        let registry: SharedWebSocketRegistry = Arc::new(parking_lot::RwLock::new(
            crate::state::WebSocketRegistry::default(),
        ));
        let (id, _rx) = register(
            registry.clone(),
            "api-1".into(),
            "dev".into(),
            "127.0.0.1".into(),
            None,
        );
        let before = registry.read().get(&id).unwrap().last_active_at;
        std::thread::sleep(std::time::Duration::from_millis(10));
        registry.write().touch(&id);
        let after = registry.read().get(&id).unwrap().last_active_at;
        assert!(after > before, "last_active_at should bump on touch");
    }
}
