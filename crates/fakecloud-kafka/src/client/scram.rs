//! The client side of SASL/SCRAM (RFC 5802) with SHA-256 or SHA-512, as a
//! Kafka broker runs it over `SaslAuthenticate`: client-first, server-first,
//! client-final with the proof, then the server's signature checked.

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256, Sha512};

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// A server asking for more iterations than this is refused.
const MAX_ITERATIONS: u32 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramAlgorithm {
    Sha256,
    Sha512,
}

impl ScramAlgorithm {
    pub fn mechanism(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }

    fn hmac(self, key: &[u8], data: &[u8]) -> Vec<u8> {
        match self {
            Self::Sha256 => {
                let mut mac =
                    <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC takes any key length");
                mac.update(data);
                mac.finalize().into_bytes().to_vec()
            }
            Self::Sha512 => {
                let mut mac =
                    <Hmac<Sha512> as Mac>::new_from_slice(key).expect("HMAC takes any key length");
                mac.update(data);
                mac.finalize().into_bytes().to_vec()
            }
        }
    }

    fn hash(self, data: &[u8]) -> Vec<u8> {
        match self {
            Self::Sha256 => Sha256::digest(data).to_vec(),
            Self::Sha512 => Sha512::digest(data).to_vec(),
        }
    }

    /// `Hi(password, salt, i)`: PBKDF2 with this HMAC, one block.
    fn hi(self, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut first = salt.to_vec();
        first.extend_from_slice(&1u32.to_be_bytes());
        let mut u = self.hmac(password, &first);
        let mut out = u.clone();
        for _ in 1..iterations {
            u = self.hmac(password, &u);
            for (o, b) in out.iter_mut().zip(&u) {
                *o ^= b;
            }
        }
        out
    }
}

/// One SCRAM exchange.
pub struct ScramClient {
    algorithm: ScramAlgorithm,
    password: String,
    nonce: String,
    client_first_bare: String,
    /// Set by [`Self::client_final`]: the salted password and auth message
    /// the server signature is checked against.
    exchange: Option<(Vec<u8>, String)>,
}

/// The password and the salted password never reach a log line.
impl std::fmt::Debug for ScramClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramClient")
            .field("algorithm", &self.algorithm)
            .field("client_first_bare", &self.client_first_bare)
            .field("password", &"<redacted>")
            .finish_non_exhaustive()
    }
}

/// `saslname`: `=` and `,` escaped as `=3D` and `=2C`.
fn sasl_name(name: &str) -> String {
    name.replace('=', "=3D").replace(',', "=2C")
}

impl ScramClient {
    pub fn new(algorithm: ScramAlgorithm, username: &str, password: &str, nonce: String) -> Self {
        let client_first_bare = format!("n={},r={nonce}", sasl_name(username));
        Self {
            algorithm,
            password: password.to_string(),
            nonce,
            client_first_bare,
            exchange: None,
        }
    }

    /// The client-first message (no channel binding, no authzid).
    pub fn client_first(&self) -> String {
        format!("n,,{}", self.client_first_bare)
    }

    /// The client-final message answering `server_first`.
    pub fn client_final(&mut self, server_first: &str) -> Result<String, String> {
        let attr = |key: char| {
            server_first.split(',').find_map(|part| {
                let mut chars = part.chars();
                (chars.next() == Some(key) && chars.next() == Some('='))
                    .then(|| part[2..].to_string())
            })
        };
        if let Some(err) = attr('e') {
            return Err(err);
        }
        let nonce = attr('r').ok_or("server-first message has no nonce")?;
        if !nonce.starts_with(&self.nonce) || nonce.len() == self.nonce.len() {
            return Err("server nonce does not extend the client nonce".to_string());
        }
        let salt = attr('s')
            .and_then(|s| B64.decode(s).ok())
            .ok_or("server-first message has no valid salt")?;
        let iterations: u32 = attr('i')
            .and_then(|i| i.parse().ok())
            .filter(|i| (1..=MAX_ITERATIONS).contains(i))
            .ok_or("server-first message has no valid iteration count")?;
        let alg = self.algorithm;
        let salted = alg.hi(self.password.as_bytes(), &salt, iterations);
        let client_key = alg.hmac(&salted, b"Client Key");
        let stored_key = alg.hash(&client_key);
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("{},{server_first},{without_proof}", self.client_first_bare);
        let signature = alg.hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(&signature)
            .map(|(k, s)| k ^ s)
            .collect();
        self.exchange = Some((salted, auth_message));
        Ok(format!("{without_proof},p={}", B64.encode(proof)))
    }

    /// Check the server-final message's signature.
    pub fn verify_server_final(&self, server_final: &str) -> Result<(), String> {
        if let Some(err) = server_final.strip_prefix("e=") {
            return Err(err.to_string());
        }
        let (salted, auth_message) = self
            .exchange
            .as_ref()
            .ok_or("server-final message before client-final")?;
        let got = server_final
            .strip_prefix("v=")
            .and_then(|v| B64.decode(v.split(',').next().unwrap_or(v)).ok())
            .ok_or("server-final message has no verifier")?;
        let server_key = self.algorithm.hmac(salted, b"Server Key");
        let expected = self.algorithm.hmac(&server_key, auth_message.as_bytes());
        if got != expected {
            return Err("server signature does not match".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677 section 3's SCRAM-SHA-256 example exchange.
    #[test]
    fn rfc7677_sha256_exchange() {
        let mut client = ScramClient::new(
            ScramAlgorithm::Sha256,
            "user",
            "pencil",
            "rOprNGfwEbeRWgbNEkqO".to_string(),
        );
        assert_eq!(client.client_first(), "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        assert_eq!(
            client.client_final(server_first).unwrap(),
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        client
            .verify_server_final("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .unwrap();
        assert!(client.verify_server_final("v=AAAA").is_err());
    }

    #[test]
    fn server_errors_and_foreign_nonces_are_refused() {
        let mut client = ScramClient::new(ScramAlgorithm::Sha512, "a=b,c", "pw", "abc".to_string());
        assert_eq!(client.client_first(), "n,,n=a=3Db=2Cc,r=abc");
        assert_eq!(
            client.client_final("e=unknown-user").unwrap_err(),
            "unknown-user"
        );
        assert!(client.client_final("r=xyz123,s=AAAA,i=4096").is_err());
        assert!(client.client_final("r=abc123,s=AAAA,i=0").is_err());
        assert!(client.client_final("r=abc123,s=AAAA,i=4096").is_ok());
    }
}
