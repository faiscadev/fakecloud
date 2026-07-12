//! Primitives shared across the AWS FIS (`fis`) handlers: ARN synthesis,
//! id generation, timestamps, and client-token minting. Kept in one place so the
//! create/get/start paths cannot diverge on wire format.

use uuid::Uuid;

/// The FIS experiment-template ARN
/// (`arn:aws:fis:{region}:{account}:experiment-template/{id}`).
pub fn experiment_template_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:fis:{region}:{account}:experiment-template/{id}")
}

/// The FIS experiment ARN (`arn:aws:fis:{region}:{account}:experiment/{id}`).
pub fn experiment_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:fis:{region}:{account}:experiment/{id}")
}

/// The FIS safety-lever ARN
/// (`arn:aws:fis:{region}:{account}:safety-lever/{id}`).
pub fn safety_lever_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:fis:{region}:{account}:safety-lever/{id}")
}

/// The FIS action ARN. Actions are AWS-owned, so the account field is empty:
/// `arn:aws:fis:{region}::action/{actionId}`.
pub fn action_arn(region: &str, action_id: &str) -> String {
    format!("arn:aws:fis:{region}::action/{action_id}")
}

/// The experiment-template `id` embedded in an experiment-template ARN (the
/// segment after `experiment-template/`). Returns `None` for another resource.
pub fn template_id_from_arn(arn: &str) -> Option<&str> {
    arn.rsplit_once(":experiment-template/").map(|(_, n)| n)
}

/// The experiment `id` embedded in an experiment ARN (the segment after
/// `experiment/`). Returns `None` for another resource.
pub fn experiment_id_from_arn(arn: &str) -> Option<&str> {
    arn.rsplit_once(":experiment/").map(|(_, n)| n)
}

/// A fresh experiment-template id of AWS's `EXT<20-alnum>` shape.
pub fn new_template_id() -> String {
    format!("EXT{}", rand_alnum(20))
}

/// A fresh experiment id of AWS's `EXP<20-alnum>` shape.
pub fn new_experiment_id() -> String {
    format!("EXP{}", rand_alnum(20))
}

/// A run of uppercase-alphanumeric characters, derived from a fresh UUID so ids
/// are unique per call and match AWS's opaque-id shape.
fn rand_alnum(len: usize) -> String {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut out = String::with_capacity(len);
    let mut src = Uuid::new_v4().as_u128();
    for _ in 0..len {
        let idx = (src % alphabet.len() as u128) as usize;
        out.push(alphabet[idx] as char);
        src /= alphabet.len() as u128;
        if src == 0 {
            src = Uuid::new_v4().as_u128();
        }
    }
    out
}

/// Current time as restJson1 epoch-seconds (a floating-point number). FIS's
/// timestamp members (`creationTime`, `startTime`, ...) carry no
/// `@timestampFormat`, so restJson1's default epoch-seconds applies.
pub fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// Mint an opaque idempotency / pagination-adjacent token. Not used to key state
/// here, but returned where AWS returns one.
pub fn mint_token() -> String {
    Uuid::new_v4().simple().to_string()
}
