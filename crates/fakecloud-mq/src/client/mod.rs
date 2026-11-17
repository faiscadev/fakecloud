//! Wire-protocol clients for the brokers fakecloud runs: STOMP 1.2 for
//! ActiveMQ and AMQP 0-9-1 for RabbitMQ.
//!
//! They speak to the real engine containers behind a broker's data plane, so
//! fakecloud can consume a broker queue on a caller's behalf (a Lambda event
//! source mapping) the way AWS's pollers do. Both clients buffer what they read
//! and parse whole frames out of that buffer, so every receive is cancel safe:
//! a receive dropped by a timeout loses no bytes and the next receive picks up
//! where it left off.

pub mod amqp;
pub mod stomp;

/// A broker client failure.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("broker connection failed: {0}")]
    Io(#[from] std::io::Error),
    /// The broker refused the credentials (or the virtual host).
    #[error("broker refused the credentials: {0}")]
    Auth(String),
    /// The broker answered with an error frame or closed the channel.
    #[error("broker error: {0}")]
    Broker(String),
    /// The broker sent something the protocol does not allow here.
    #[error("broker protocol error: {0}")]
    Protocol(String),
    #[error("broker closed the connection")]
    Closed,
}
