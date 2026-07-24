//! Server-side SRP6a for Cognito `USER_SRP_AUTH`, matching the wire protocol
//! `amazon-cognito-identity-js` (and the AWS SDK SRP helpers) implement, so a
//! real Amplify client authenticates against fakecloud.
//!
//! Flow: `InitiateAuth(USER_SRP_AUTH)` returns a `PASSWORD_VERIFIER` challenge
//! carrying `SRP_B`, `SALT`, and an opaque `SECRET_BLOCK`. The client computes
//! its session key from its `SRP_A`/password and returns a
//! `PASSWORD_CLAIM_SIGNATURE` (an HMAC over `poolName + userId + secretBlock +
//! timestamp` keyed by the HKDF-derived session key). The server recomputes the
//! same key from `A`, the stored verifier `v`, and its private `b`, and checks
//! the signature. We never store the plaintext password's verifier-equivalent;
//! the verifier is derived on demand from the stored password.

use hmac::{Hmac, Mac};
use num_bigint::BigUint;
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Maximum accepted hex length for a client-supplied SRP public value (`SRP_A`).
/// The 3072-bit modulus is 768 hex chars; a legitimate `A < N` never exceeds
/// that (plus a small margin). Anything larger is malformed and would only
/// serve to inflate the cost of the `num-bigint` modpow/multiply that verifies
/// the proof, so we reject it up front (CPU-DoS guard against an attacker-sized
/// `A` blocking the executor thread).
pub const MAX_PUBLIC_HEX_LEN: usize = 1024;

/// Constant-time equality for two equal-length byte slices, used for the SRP
/// `PASSWORD_CLAIM_SIGNATURE` comparison so the check does not short-circuit on
/// the first differing byte (no timing side-channel on the proof). The length
/// branch is not secret: the signature is a fixed-width base64 HMAC-SHA256.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// The 3072-bit MODP group (RFC 5054) that AWS Cognito uses, g = 2.
const N_HEX: &str = "\
FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74\
020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F1437\
4FE1356D6D51C245E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED\
EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3DC2007CB8A163BF05\
98DA48361C55D39A69163FA8FD24CF5F83655D23DCA3AD961C62F356208552BB\
9ED529077096966D670C354E4ABC9804F1746C08CA18217C32905E462E36CE3B\
E39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9DE2BCBF695581718\
3995497CEA956AE515D2261898FA051015728E5A8AAAC42DAD33170D04507A33\
A85521ABDF1CBA64ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7\
ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6BF12FFA06D98A0864\
D87602733EC86A64521F2B18177B200CBBE117577A615D6C770988C0BAD946E2\
08E24FA074E5AB3143DB5BFCE0FD108E4B82D120A93AD2CAFFFFFFFFFFFFFFFF";

fn modulus() -> BigUint {
    BigUint::parse_bytes(N_HEX.as_bytes(), 16).expect("valid N")
}

fn sha256(parts: &[&[u8]]) -> Vec<u8> {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p);
    }
    h.finalize().to_vec()
}

/// Big-endian bytes of `n` with the `amazon-cognito-identity-js` `padHex`
/// convention: a leading `0x00` is prepended when the high bit of the first
/// byte is set (signed-magnitude), matching the hashes both sides feed to SHA.
fn pad_bytes(n: &BigUint) -> Vec<u8> {
    let bytes = n.to_bytes_be();
    if bytes.is_empty() {
        return vec![0];
    }
    if bytes[0] & 0x80 != 0 {
        let mut out = Vec::with_capacity(bytes.len() + 1);
        out.push(0);
        out.extend_from_slice(&bytes);
        out
    } else {
        bytes
    }
}

fn hex(n: &BigUint) -> String {
    n.to_str_radix(16)
}

fn from_hex(s: &str) -> Option<BigUint> {
    BigUint::parse_bytes(s.as_bytes(), 16)
}

/// `k = H(PAD(N) | PAD(g))`.
fn k_factor(n: &BigUint) -> BigUint {
    let g = BigUint::from(2u8);
    let padded_n = pad_bytes(n);
    BigUint::from_bytes_be(&sha256(&[&padded_n, &pad_bytes(&g)]))
}

/// Derive the SRP verifier `v = g^x mod N` from the user's password, where
/// `x = H(PAD(salt) | H(poolName | userId | ":" | password))`.
/// `pool_name` is the user-pool id with its region prefix stripped (the part
/// after `_`), exactly as the client computes it.
pub fn compute_verifier(pool_name: &str, user_id: &str, password: &str, salt: &BigUint) -> BigUint {
    let n = modulus();
    let g = BigUint::from(2u8);
    let inner = sha256(&[format!("{pool_name}{user_id}:{password}").as_bytes()]);
    let x = BigUint::from_bytes_be(&sha256(&[&pad_bytes(salt), &inner]));
    g.modpow(&x, &n)
}

/// Server state stashed between `InitiateAuth` and `RespondToAuthChallenge`.
pub struct ServerHandshake {
    pub salt: BigUint,
    pub server_public_b: BigUint,
    pub server_private_b: BigUint,
}

/// Pick a random private `b` and compute the public `B = (k*v + g^b) mod N`.
/// Retries (vanishingly unlikely) if `B % N == 0`.
pub fn server_keys(verifier: &BigUint, salt: BigUint) -> ServerHandshake {
    use rand::RngCore;
    let n = modulus();
    let g = BigUint::from(2u8);
    let k = k_factor(&n);
    loop {
        let mut buf = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut buf);
        let b = BigUint::from_bytes_be(&buf) % (&n - 1u8) + 1u8;
        let big_b = (&k * verifier + g.modpow(&b, &n)) % &n;
        if big_b != BigUint::from(0u8) {
            return ServerHandshake {
                salt,
                server_public_b: big_b,
                server_private_b: b,
            };
        }
    }
}

/// `u = H(PAD(A) | PAD(B))`.
fn scrambler(a: &BigUint, b: &BigUint) -> BigUint {
    BigUint::from_bytes_be(&sha256(&[&pad_bytes(a), &pad_bytes(b)]))
}

/// AWS HKDF: `prk = HMAC(salt=PAD(u), ikm=PAD(S))`, then
/// `okm = HMAC(prk, "Caldera Derived Key"||0x01)[:16]`.
fn derive_key(s: &BigUint, u: &BigUint) -> Vec<u8> {
    let ikm = pad_bytes(s);
    let salt = pad_bytes(u);
    let mut extract = HmacSha256::new_from_slice(&salt).expect("hmac key");
    extract.update(&ikm);
    let prk = extract.finalize().into_bytes();

    let mut info = b"Caldera Derived Key".to_vec();
    info.push(0x01);
    let mut expand = HmacSha256::new_from_slice(&prk).expect("hmac key");
    expand.update(&info);
    expand.finalize().into_bytes()[..16].to_vec()
}

/// Compute the expected `PASSWORD_CLAIM_SIGNATURE` (base64) for the given
/// client `SRP_A`, returning `None` if `A` is malformed or `A % N == 0`.
/// `secret_block` is the raw (base64-decoded) bytes; `timestamp` is the exact
/// string the client signed.
#[allow(clippy::too_many_arguments)]
pub fn expected_signature(
    handshake: &ServerHandshake,
    verifier: &BigUint,
    client_a_hex: &str,
    pool_name: &str,
    user_id: &str,
    secret_block: &[u8],
    timestamp: &str,
) -> Option<String> {
    use base64::Engine;
    if client_a_hex.len() > MAX_PUBLIC_HEX_LEN {
        return None;
    }
    let n = modulus();
    let a = from_hex(client_a_hex)?;
    if (&a % &n) == BigUint::from(0u8) {
        return None;
    }
    let u = scrambler(&a, &handshake.server_public_b);
    if u == BigUint::from(0u8) {
        return None;
    }
    // S = (A * v^u)^b mod N
    let s = (&a * verifier.modpow(&u, &n)).modpow(&handshake.server_private_b, &n);
    let key = derive_key(&s, &u);

    let mut mac = HmacSha256::new_from_slice(&key).expect("hmac key");
    mac.update(pool_name.as_bytes());
    mac.update(user_id.as_bytes());
    mac.update(secret_block);
    mac.update(timestamp.as_bytes());
    Some(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

/// Strip the region prefix from a user-pool id (`us-east-1_AbCdEf` -> `AbCdEf`).
/// The SRP hashes use this short name, matching the client.
pub fn pool_short_name(pool_id: &str) -> &str {
    pool_id.split_once('_').map(|(_, n)| n).unwrap_or(pool_id)
}

/// Hex of a `BigUint`, for stashing handshake values in the session.
pub fn to_hex(n: &BigUint) -> String {
    hex(n)
}

/// Parse a hex `BigUint` previously produced by [`to_hex`].
pub fn parse_hex(s: &str) -> Option<BigUint> {
    from_hex(s)
}

/// A fresh random salt for a handshake.
pub fn random_salt() -> BigUint {
    use rand::RngCore;
    let mut buf = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut buf);
    BigUint::from_bytes_be(&buf)
}

/// Test-only: generate a client SRP keypair `(a_priv, A_pub)`.
#[cfg(test)]
pub(crate) fn test_client_keypair() -> (BigUint, BigUint) {
    use rand::RngCore;
    let n = modulus();
    let g = BigUint::from(2u8);
    let mut buf = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut buf);
    let a = BigUint::from_bytes_be(&buf) % (&n - 1u8) + 1u8;
    let a_pub = g.modpow(&a, &n);
    (a, a_pub)
}

/// Test-only: compute the client's `PASSWORD_CLAIM_SIGNATURE`, mirroring the
/// amazon-cognito-identity-js client so the service test proves the real
/// handler accepts a faithful proof.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn test_client_signature(
    pool_name: &str,
    user_id: &str,
    password: &str,
    salt: &BigUint,
    server_b: &BigUint,
    a_priv: &BigUint,
    secret_block: &[u8],
    timestamp: &str,
) -> String {
    use base64::Engine;
    let n = modulus();
    let g = BigUint::from(2u8);
    let k = k_factor(&n);
    let a_pub = g.modpow(a_priv, &n);
    let u = scrambler(&a_pub, server_b);
    let inner = sha256(&[format!("{pool_name}{user_id}:{password}").as_bytes()]);
    let x = BigUint::from_bytes_be(&sha256(&[&pad_bytes(salt), &inner]));
    let gx = g.modpow(&x, &n);
    let base = (server_b + &n * &k - (&k * &gx) % &n) % &n;
    let exp = a_priv + &u * &x;
    let s = base.modpow(&exp, &n);
    let key = derive_key(&s, &u);
    let mut mac = HmacSha256::new_from_slice(&key).unwrap();
    mac.update(pool_name.as_bytes());
    mac.update(user_id.as_bytes());
    mac.update(secret_block);
    mac.update(timestamp.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn k_factor_uses_cognito_signed_magnitude_encoding() {
        let expected = BigUint::parse_bytes(
            b"538282c4354742d7cbbde2359fcf67f9f5b3a6b08791e5011b43b8a5b66d9ee6",
            16,
        )
        .expect("valid Cognito SRP multiplier");

        assert_eq!(k_factor(&modulus()), expected);
    }

    /// Full client+server SRP exchange following the amazon-cognito-identity-js
    /// client algorithm, proving the server accepts a faithful client proof.
    #[test]
    fn full_srp_handshake_round_trips() {
        let pool_id = "us-east-1_TestPool";
        let pool_name = pool_short_name(pool_id);
        let user_id = "alice";
        let password = "Sup3rSecret!";

        let n = modulus();
        let g = BigUint::from(2u8);
        let k = k_factor(&n);
        let salt = random_salt();

        // Server provisions a verifier from the password.
        let v = compute_verifier(pool_name, user_id, password, &salt);
        let hs = server_keys(&v, salt.clone());

        // --- Client side (independent of server math above) ---
        use rand::RngCore;
        let mut abuf = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut abuf);
        let a_priv = BigUint::from_bytes_be(&abuf) % (&n - 1u8) + 1u8;
        let a_pub = g.modpow(&a_priv, &n);
        let u = scrambler(&a_pub, &hs.server_public_b);
        // x identical to compute_verifier's derivation
        let inner = sha256(&[format!("{pool_name}{user_id}:{password}").as_bytes()]);
        let x = BigUint::from_bytes_be(&sha256(&[&pad_bytes(&salt), &inner]));
        // S_client = (B - k * g^x) ^ (a + u*x) mod N
        let gx = g.modpow(&x, &n);
        let base = (&hs.server_public_b + &n * &k - (&k * &gx) % &n) % &n;
        let exp = &a_priv + &u * &x;
        let s_client = base.modpow(&exp, &n);
        let key = derive_key(&s_client, &u);

        let secret_block = b"opaque-secret-block-bytes";
        let timestamp = "Wed Mar 6 12:34:56 UTC 2024";
        let mut mac = HmacSha256::new_from_slice(&key).unwrap();
        mac.update(pool_name.as_bytes());
        mac.update(user_id.as_bytes());
        mac.update(secret_block);
        mac.update(timestamp.as_bytes());
        let client_sig =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

        // --- Server verifies ---
        let server_sig = expected_signature(
            &hs,
            &v,
            &to_hex(&a_pub),
            pool_name,
            user_id,
            secret_block,
            timestamp,
        )
        .expect("server computes signature");
        assert_eq!(client_sig, server_sig, "SRP proof must match");

        // A wrong password must NOT verify.
        let wrong_v = compute_verifier(pool_name, user_id, "wrong", &salt);
        let hs2 = server_keys(&wrong_v, salt);
        let server_sig_wrong = expected_signature(
            &hs2,
            &wrong_v,
            &to_hex(&a_pub),
            pool_name,
            user_id,
            secret_block,
            timestamp,
        )
        .unwrap();
        assert_ne!(client_sig, server_sig_wrong);
    }
}
