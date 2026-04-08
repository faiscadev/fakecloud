use std::fmt;

/// Errors returned by the fakecloud SDK client.
#[derive(Debug)]
pub enum Error {
    /// HTTP transport error from reqwest.
    Http(reqwest::Error),
    /// The server returned a non-success HTTP status.
    Api { status: u16, body: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Http(e) => write!(f, "HTTP error: {e}"),
            Error::Api { status, body } => write!(f, "API error (HTTP {status}): {body}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(e) => Some(e),
            Error::Api { .. } => None,
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Http(e)
    }
}
