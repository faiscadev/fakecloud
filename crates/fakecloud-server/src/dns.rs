//! Real DNS resolver over the Route 53 records created in fakecloud (issue
//! #2219).
//!
//! With `--dns`, fakecloud binds UDP + TCP on a DNS address (port 53 by default,
//! which needs root) and answers the record types it can wire-encode
//! (`A`/`AAAA`/`CNAME`/`MX`/`TXT`/`NS`/`PTR`/`SPF`/`CAA`) from the Route 53 zones
//! created in it. A name that falls in no local zone is forwarded to an upstream
//! resolver, so a container can point its `/etc/resolv.conf` (or compose `dns:`)
//! at fakecloud as its sole resolver and still reach the outside world. Route 53
//! becomes the one source of truth for local service discovery.
//!
//! The wire layer is hand-rolled (the workspace has no DNS library, and
//! `fakecloud_route53::dnssec` already hand-rolls RDATA encoding, which this
//! reuses). It parses a single-question query, resolves it via
//! [`fakecloud_route53::resolver`], and builds an answer with uncompressed owner
//! names; forwarding relays the raw query bytes untouched.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use fakecloud_route53::dnssec::{encode_rdata, type_code};
use fakecloud_route53::resolver::{self, ResolveStatus};
use fakecloud_route53::SharedRoute53State;
use tokio::net::{TcpListener, UdpSocket};

/// Timeout for a single upstream forward round-trip.
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(5);
/// Idle timeout for a client-facing DNS-over-TCP connection (RFC 7766).
const TCP_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
/// Cap on a UDP datagram we read (EDNS0 can raise the classic 512-byte limit;
/// 4 KiB covers typical answers without unbounded allocation).
const MAX_UDP: usize = 4096;
/// Cap on a DNS-over-TCP message. TCP messages are length-prefixed and may be up
/// to 65535 bytes (the fallback specifically for large answers).
const MAX_TCP: usize = 65535;
/// Classic (non-EDNS) UDP answer size limit. A larger answer sets TC so the
/// client retries over TCP.
const CLASSIC_UDP: usize = 512;

// DNS header flag bits / rcodes.
const FLAG_QR: u16 = 0x8000;
const FLAG_AA: u16 = 0x0400;
const FLAG_TC: u16 = 0x0200;
const FLAG_RD: u16 = 0x0100;
const FLAG_RA: u16 = 0x0080;
const RCODE_FORMERR: u16 = 1;
const RCODE_SERVFAIL: u16 = 2;
const RCODE_NXDOMAIN: u16 = 3;
const CLASS_IN: u16 = 1;
const TYPE_OPT: u16 = 41;

/// Shared state for the DNS listener.
#[derive(Clone)]
pub struct DnsConfig {
    pub route53: SharedRoute53State,
    /// Upstream resolver for names in no local zone. `None` disables forwarding
    /// (non-authoritative names then get SERVFAIL).
    pub upstream: Option<SocketAddr>,
}

/// A parsed single-question DNS query.
struct Query {
    id: u16,
    rd: bool,
    qname: String,
    qtype: u16,
    /// The raw question section (qname + qtype + qclass), echoed verbatim into
    /// the response.
    question: Vec<u8>,
}

/// Bring up the DNS listeners on `addr`. Detached by `main`, so it never delays
/// startup. Best-effort: a bind failure (port 53 needs root) is logged and the
/// main server is unaffected.
pub async fn run(cfg: DnsConfig, addr: SocketAddr) {
    let udp = match UdpSocket::bind(addr).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            tracing::warn!(
                "fakecloud could not bind DNS UDP on {addr}: {e}. Binding port 53 needs root; \
                 pass --dns-addr to use a high port. The main server is unaffected."
            );
            return;
        }
    };
    tracing::info!("fakecloud DNS resolver listening on udp://{addr}");

    // UDP: one task per datagram so a slow upstream forward can't head-of-line
    // block other queries.
    let udp_cfg = cfg.clone();
    let udp_sock = udp.clone();
    tokio::spawn(async move {
        let mut buf = vec![0u8; MAX_UDP];
        loop {
            let (n, peer) = match udp_sock.recv_from(&mut buf).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("fakecloud DNS UDP recv error: {e}");
                    continue;
                }
            };
            let query = buf[..n].to_vec();
            let cfg = udp_cfg.clone();
            let sock = udp_sock.clone();
            tokio::spawn(async move {
                // UDP: over-size answers come back truncated with TC set.
                let resp = handle_query(&cfg, &query, true).await;
                if let Some(resp) = resp {
                    let _ = sock.send_to(&resp, peer).await;
                }
            });
        }
    });

    // TCP: RFC 1035 length-prefixed messages.
    match TcpListener::bind(addr).await {
        Ok(tcp) => {
            tracing::info!("fakecloud DNS resolver listening on tcp://{addr}");
            tokio::spawn(async move {
                loop {
                    match tcp.accept().await {
                        Ok((stream, _)) => {
                            let cfg = cfg.clone();
                            tokio::spawn(async move {
                                let _ = handle_tcp(&cfg, stream).await;
                            });
                        }
                        Err(e) => tracing::warn!("fakecloud DNS TCP accept error: {e}"),
                    }
                }
            });
        }
        Err(e) => {
            tracing::warn!("fakecloud could not bind DNS TCP on {addr}: {e} (UDP still serves)")
        }
    }
}

/// Serve one TCP connection: read length-prefixed queries, answer each. An idle
/// (or stalled mid-message) connection is dropped after [`TCP_IDLE_TIMEOUT`] so a
/// client that connects and never sends can't pin a task/FD/buffer forever.
async fn handle_tcp(cfg: &DnsConfig, mut stream: tokio::net::TcpStream) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    loop {
        let mut len_buf = [0u8; 2];
        match tokio::time::timeout(TCP_IDLE_TIMEOUT, stream.read_exact(&mut len_buf)).await {
            Ok(Ok(_)) => {}
            _ => return Ok(()), // client closed, idle-timed-out, or errored
        }
        let len = u16::from_be_bytes(len_buf) as usize;
        if len == 0 || len > MAX_TCP {
            return Ok(());
        }
        let mut msg = vec![0u8; len];
        match tokio::time::timeout(TCP_IDLE_TIMEOUT, stream.read_exact(&mut msg)).await {
            Ok(Ok(_)) => {}
            _ => return Ok(()), // stalled mid-body or errored
        }
        // TCP has no 512-byte limit, so never truncate.
        if let Some(resp) = handle_query(cfg, &msg, false).await {
            // A message must fit the 2-byte length prefix. Our answers never
            // approach 64 KiB; if one somehow did, close rather than write a
            // bogus length that would desync the connection.
            let Ok(rlen) = u16::try_from(resp.len()) else {
                return Ok(());
            };
            stream.write_all(&rlen.to_be_bytes()).await?;
            stream.write_all(&resp).await?;
        }
    }
}

/// Produce the response bytes for one raw query message, or `None` to drop it.
/// `udp` selects the transport: UDP answers over the client's advertised size are
/// truncated with TC (so it retries over TCP); TCP never truncates.
async fn handle_query(cfg: &DnsConfig, raw: &[u8], udp: bool) -> Option<Vec<u8>> {
    let query = match parse_query(raw) {
        Some(q) => q,
        // Unparseable (or multi-question / compressed qname): forward if we can,
        // else answer FORMERR so the client fails fast instead of hanging.
        None => {
            return Some(match forward(cfg, raw, !udp).await {
                Some(resp) => resp,
                None => formerr(raw),
            })
        }
    };

    // The client's EDNS0 advertised size, if any (parsed once; echoed as an OPT
    // on both transports). Only UDP truncates, past the clamped advertised size.
    let edns = opt_payload_size(raw);
    let udp_limit = if udp { Some(edns_udp_size(edns)) } else { None };
    // Only advertise recursion when we actually have an upstream to recurse to.
    let ra = cfg.upstream.is_some();

    let qtype_str = qtype_to_str(query.qtype);
    let resolution = {
        let accounts = cfg.route53.read();
        resolver::resolve(&accounts, &query.qname, qtype_str)
    };

    match resolution.status {
        // Forward over the same transport the client used, so a TCP client (or a
        // UDP client retrying over TCP) gets the untruncated upstream answer.
        ResolveStatus::NotAuthoritative => match forward(cfg, raw, !udp).await {
            Some(resp) => Some(resp),
            None => Some(build_response(
                &query,
                &[],
                RCODE_SERVFAIL,
                false,
                ra,
                udp_limit,
                edns,
            )),
        },
        ResolveStatus::NxDomain => Some(build_response(
            &query,
            &[],
            RCODE_NXDOMAIN,
            true,
            ra,
            udp_limit,
            edns,
        )),
        ResolveStatus::NoData => Some(build_response(&query, &[], 0, true, ra, udp_limit, edns)),
        ResolveStatus::Answered => {
            let mut answers = resolution.answers;
            // The CNAME chain left all local zones: forward-resolve the external
            // target upstream and append its records (true owner names), so a stub
            // client that does not chase CNAMEs still gets an address in one reply.
            let mut authoritative = true;
            if let Some(target) = resolution.external_cname {
                if let Some(records) = forward_resolve_records(cfg, &target, query.qtype).await {
                    // Appended records were fetched recursively, so the answer is
                    // no longer purely authoritative.
                    if !records.is_empty() {
                        authoritative = false;
                    }
                    answers.extend(records);
                }
            }
            Some(build_response(
                &query,
                &answers,
                0,
                authoritative,
                ra,
                udp_limit,
                edns,
            ))
        }
    }
}

/// A minimal FORMERR (rcode 1) response echoing the query id, so a client whose
/// query we can't parse (and can't forward) fails fast instead of hanging.
fn formerr(raw: &[u8]) -> Vec<u8> {
    let id = if raw.len() >= 2 {
        [raw[0], raw[1]]
    } else {
        [0, 0]
    };
    let flags = FLAG_QR | RCODE_FORMERR;
    let mut out = Vec::with_capacity(12);
    out.extend_from_slice(&id);
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(&[0; 8]); // QD/AN/NS/AR all zero
    out
}

/// Forward the raw query to the configured upstream and return its raw reply.
/// `None` when no upstream is set or the round-trip fails. `tcp` selects the
/// transport so a TCP client (or one retrying over TCP) gets an untruncated
/// answer instead of the upstream's UDP-truncated one.
async fn forward(cfg: &DnsConfig, raw: &[u8], tcp: bool) -> Option<Vec<u8>> {
    let upstream = cfg.upstream?;
    if raw.len() < 2 {
        return None;
    }
    let txid = [raw[0], raw[1]];
    if tcp {
        forward_tcp(upstream, raw, txid).await
    } else {
        forward_udp(upstream, raw, txid).await
    }
}

/// UDP forward: `connect`ed socket (only the upstream's datagrams are received)
/// with transaction-ID validation to reject spoofed/stray replies.
async fn forward_udp(upstream: SocketAddr, raw: &[u8], txid: [u8; 2]) -> Option<Vec<u8>> {
    let bind: SocketAddr = if upstream.is_ipv6() {
        "[::]:0".parse().ok()?
    } else {
        "0.0.0.0:0".parse().ok()?
    };
    let sock = UdpSocket::bind(bind).await.ok()?;
    sock.connect(upstream).await.ok()?;
    sock.send(raw).await.ok()?;
    let deadline = tokio::time::Instant::now() + UPSTREAM_TIMEOUT;
    // A UDP datagram can be up to 65535 bytes; size the buffer for the whole
    // reply so a large upstream answer isn't silently cut mid-record.
    let mut buf = vec![0u8; MAX_TCP];
    loop {
        let n = tokio::time::timeout_at(deadline, sock.recv(&mut buf))
            .await
            .ok()?
            .ok()?;
        if n >= 2 && buf[0..2] == txid {
            buf.truncate(n);
            return Some(buf);
        }
    }
}

/// TCP forward: length-prefixed query/reply to the upstream, with the same
/// transaction-ID check.
async fn forward_tcp(upstream: SocketAddr, raw: &[u8], txid: [u8; 2]) -> Option<Vec<u8>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let fut = async {
        let mut stream = tokio::net::TcpStream::connect(upstream).await.ok()?;
        let len = u16::try_from(raw.len()).ok()?;
        stream.write_all(&len.to_be_bytes()).await.ok()?;
        stream.write_all(raw).await.ok()?;
        let mut len_buf = [0u8; 2];
        stream.read_exact(&mut len_buf).await.ok()?;
        let rlen = u16::from_be_bytes(len_buf) as usize;
        let mut reply = vec![0u8; rlen];
        stream.read_exact(&mut reply).await.ok()?;
        if reply.len() >= 2 && reply[0..2] == txid {
            Some(reply)
        } else {
            None
        }
    };
    tokio::time::timeout(UPSTREAM_TIMEOUT, fut).await.ok()?
}

/// Forward-resolve `name`/`qtype` upstream and return the answer-section records
/// (the CNAME chain plus `A`/`AAAA` addresses), so an external CNAME target can
/// be appended to a local answer with each record's true owner name. `None` if
/// there is no upstream or the round-trip fails.
async fn forward_resolve_records(
    cfg: &DnsConfig,
    name: &str,
    qtype: u16,
) -> Option<Vec<resolver::AnswerRecord>> {
    // Build a minimal recursive query for the target (fixed id; the reply is
    // validated by txid inside forward()). It carries an EDNS0 OPT advertising a
    // large buffer so the upstream's address set for the target isn't
    // UDP-truncated before we can append it.
    let mut q = Vec::new();
    q.extend_from_slice(&0x7a7au16.to_be_bytes());
    q.extend_from_slice(&FLAG_RD.to_be_bytes());
    q.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    q.extend_from_slice(&[0, 0, 0, 0]); // AN/NS
    q.extend_from_slice(&1u16.to_be_bytes()); // ARCOUNT (OPT)
    write_name(&mut q, name);
    q.extend_from_slice(&qtype.to_be_bytes());
    q.extend_from_slice(&CLASS_IN.to_be_bytes());
    // EDNS0 OPT: root name, TYPE=OPT, CLASS=advertised UDP size, TTL 0, RDLEN 0.
    let sz = (MAX_TCP as u16).to_be_bytes();
    let ty = TYPE_OPT.to_be_bytes();
    q.extend_from_slice(&[0, ty[0], ty[1], sz[0], sz[1], 0, 0, 0, 0, 0, 0]);

    let reply = forward(cfg, &q, false).await?;
    Some(extract_answers(&reply, qtype))
}

/// Read a (possibly compression-using) DNS name at `start`, returning the dotted
/// name (with trailing dot) and the offset just past the name field at `start`
/// (following the first pointer's two bytes). Bounded to reject pointer loops.
fn read_name(buf: &[u8], start: usize) -> Option<(String, usize)> {
    let mut labels: Vec<String> = Vec::new();
    let mut pos = start;
    let mut next = start;
    let mut jumped = false;
    let mut hops = 0;
    loop {
        let len = *buf.get(pos)? as usize;
        if len & 0xc0 == 0xc0 {
            let ptr = ((len & 0x3f) << 8) | *buf.get(pos + 1)? as usize;
            if !jumped {
                next = pos + 2;
            }
            jumped = true;
            hops += 1;
            if hops > 64 {
                return None; // pointer loop
            }
            pos = ptr;
            continue;
        }
        if len == 0 {
            if !jumped {
                next = pos + 1;
            }
            break;
        }
        pos += 1;
        let label = buf.get(pos..pos + len)?;
        labels.push(String::from_utf8_lossy(label).to_string());
        pos += len;
    }
    let name = if labels.is_empty() {
        ".".to_string()
    } else {
        format!("{}.", labels.join("."))
    };
    Some((name, next))
}

/// Extract the `A`/`AAAA`/`CNAME` answer records from an upstream reply,
/// preserving each record's true owner name (so an appended external chain is
/// not mislabeled). Only the requested address type is kept alongside CNAMEs.
fn extract_answers(buf: &[u8], qtype: u16) -> Vec<resolver::AnswerRecord> {
    let mut out = Vec::new();
    if buf.len() < 12 {
        return out;
    }
    let qd = u16::from_be_bytes([buf[4], buf[5]]);
    let an = u16::from_be_bytes([buf[6], buf[7]]);
    let mut pos = 12;
    for _ in 0..qd {
        pos = match skip_name(buf, pos) {
            Some(p) => p + 4, // qtype + qclass
            None => return out,
        };
    }
    for _ in 0..an {
        let (owner, p) = match read_name(buf, pos) {
            Some(v) => v,
            None => return out,
        };
        pos = p;
        if pos + 10 > buf.len() {
            return out;
        }
        let rtype = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
        let ttl = u32::from_be_bytes([buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]]);
        let rdlen = u16::from_be_bytes([buf[pos + 8], buf[pos + 9]]) as usize;
        let rdata_start = pos + 10;
        if rdata_start + rdlen > buf.len() {
            return out;
        }
        let rdata = &buf[rdata_start..rdata_start + rdlen];
        pos = rdata_start + rdlen;

        let value = match rtype {
            1 if rdlen == 4 => {
                std::net::Ipv4Addr::new(rdata[0], rdata[1], rdata[2], rdata[3]).to_string()
            }
            28 if rdlen == 16 => {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(rdata);
                std::net::Ipv6Addr::from(octets).to_string()
            }
            // CNAME target may use compression, so decode it against the whole
            // message rather than copying raw RDATA.
            5 => match read_name(buf, rdata_start) {
                Some((target, _)) => target,
                None => continue,
            },
            _ => continue,
        };
        // `value` is only set for A/AAAA/CNAME; keep CNAMEs (the chain) and, of
        // the address records, only the requested type.
        if (rtype == 1 || rtype == 28) && rtype != qtype {
            continue;
        }
        out.push(resolver::AnswerRecord {
            name: owner,
            rtype: qtype_to_str(rtype).to_string(),
            ttl,
            value,
        });
    }
    out
}

/// The client's advertised UDP answer size: the EDNS0 OPT record's UDP payload
/// size (its CLASS field) if present, otherwise the classic 512-byte limit.
fn edns_udp_size(opt: Option<u16>) -> usize {
    // Honor the client's advertised buffer, bounded by our own UDP ceiling
    // (MAX_UDP) so a larger answer gets TC and falls back to TCP rather than
    // shipping an oversized UDP datagram; never below the classic 512.
    (opt.unwrap_or(CLASSIC_UDP as u16) as usize).clamp(CLASSIC_UDP, MAX_UDP)
}

/// The UDP payload size from an EDNS0 OPT record in the additional section, if
/// the query carries one.
fn opt_payload_size(buf: &[u8]) -> Option<u16> {
    if buf.len() < 12 {
        return None;
    }
    let qd = u16::from_be_bytes([buf[4], buf[5]]);
    let an = u16::from_be_bytes([buf[6], buf[7]]);
    let ns = u16::from_be_bytes([buf[8], buf[9]]);
    let ar = u16::from_be_bytes([buf[10], buf[11]]);
    let mut pos = 12;
    // Skip question + answer + authority sections to reach additional.
    for _ in 0..qd {
        pos = skip_name(buf, pos)? + 4;
    }
    for _ in 0..(an as u32 + ns as u32) {
        pos = skip_name(buf, pos)?;
        let rdlen = u16::from_be_bytes([*buf.get(pos + 8)?, *buf.get(pos + 9)?]) as usize;
        pos += 10 + rdlen;
    }
    for _ in 0..ar {
        pos = skip_name(buf, pos)?;
        let rtype = u16::from_be_bytes([*buf.get(pos)?, *buf.get(pos + 1)?]);
        if rtype == TYPE_OPT {
            // OPT re-uses the CLASS field (the 2 octets after TYPE) as the
            // requestor's UDP payload size, regardless of how the owner name was
            // encoded.
            let class_pos = pos + 2;
            return Some(u16::from_be_bytes([
                *buf.get(class_pos)?,
                *buf.get(class_pos + 1)?,
            ]));
        }
        let rdlen = u16::from_be_bytes([*buf.get(pos + 8)?, *buf.get(pos + 9)?]) as usize;
        pos += 10 + rdlen;
    }
    None
}

/// Advance past a (possibly compressed) DNS name; `None` on a malformed name.
fn skip_name(buf: &[u8], mut pos: usize) -> Option<usize> {
    loop {
        let len = *buf.get(pos)? as usize;
        if len == 0 {
            return Some(pos + 1);
        }
        if len & 0xc0 == 0xc0 {
            return Some(pos + 2); // compression pointer ends the name
        }
        pos += 1 + len;
    }
}

/// Parse a single-question query. Returns `None` for anything we won't answer
/// directly (not a query, not exactly one question, or a compressed/oversized
/// qname); the caller forwards those.
fn parse_query(buf: &[u8]) -> Option<Query> {
    if buf.len() < 12 {
        return None;
    }
    let id = u16::from_be_bytes([buf[0], buf[1]]);
    let flags = u16::from_be_bytes([buf[2], buf[3]]);
    // Must be a query (QR=0), standard opcode (0).
    if flags & FLAG_QR != 0 || (flags >> 11) & 0x0f != 0 {
        return None;
    }
    let qdcount = u16::from_be_bytes([buf[4], buf[5]]);
    if qdcount != 1 {
        return None;
    }
    let rd = flags & FLAG_RD != 0;

    // Read the QNAME labels starting at offset 12.
    let mut pos = 12;
    let mut name = String::new();
    loop {
        let len = *buf.get(pos)? as usize;
        if len == 0 {
            pos += 1;
            break;
        }
        // Compression pointer in a question is not something we build for; let
        // the upstream handle it.
        if len & 0xc0 != 0 {
            return None;
        }
        pos += 1;
        let label = buf.get(pos..pos + len)?;
        if !name.is_empty() {
            name.push('.');
        }
        name.push_str(&String::from_utf8_lossy(label));
        pos += len;
    }
    let qtype = u16::from_be_bytes([*buf.get(pos)?, *buf.get(pos + 1)?]);
    let _qclass = u16::from_be_bytes([*buf.get(pos + 2)?, *buf.get(pos + 3)?]);
    let question = buf.get(12..pos + 4)?.to_vec();
    Some(Query {
        id,
        rd,
        qname: name,
        qtype,
        question,
    })
}

/// Types whose textual value `encode_rdata` turns into correct wire RDATA. Any
/// type NOT listed here (e.g. DS, NAPTR) falls through to raw bytes in
/// `encode_rdata`, so it must be kept out of the answer or the client would parse
/// garbage RDATA.
fn wire_encodable(rtype: &str) -> bool {
    matches!(
        rtype.to_ascii_uppercase().as_str(),
        "A" | "AAAA" | "CNAME" | "NS" | "PTR" | "TXT" | "SPF" | "MX" | "SRV" | "SOA" | "CAA"
    )
}

/// Build a response for `query` carrying `answers` with the given rcode. `aa`
/// sets the authoritative-answer bit; `ra` the recursion-available bit (only when
/// an upstream is configured). `udp_limit` is `Some(n)` for UDP: if the full
/// message exceeds `n` octets the answer section is dropped and TC is set, so the
/// client retries over TCP (which passes `None`, never truncating). `edns` is the
/// client's advertised UDP size when its query carried EDNS0, so a truncated (or
/// any) reply can echo an OPT record.
fn build_response(
    query: &Query,
    answers: &[resolver::AnswerRecord],
    rcode: u16,
    aa: bool,
    ra: bool,
    udp_limit: Option<usize>,
    edns: Option<u16>,
) -> Vec<u8> {
    // Deduplicate (a merged cross-account zone or a CNAME chase can surface the
    // same record twice), preserving order, then keep only records we can
    // wire-encode correctly. The HTTP introspection endpoint shares the same
    // resolver::dedup_answers so both report the same record set.
    let encodable: Vec<(&resolver::AnswerRecord, u16)> = resolver::dedup_answers(answers)
        .into_iter()
        .filter(|a| wire_encodable(&a.rtype))
        .filter_map(|a| type_code(&a.rtype).map(|tc| (a, tc)))
        .collect();

    let mut flags = FLAG_QR | rcode;
    if aa {
        flags |= FLAG_AA;
    }
    if ra {
        flags |= FLAG_RA;
    }
    if query.rd {
        flags |= FLAG_RD;
    }

    // An EDNS0 OPT record echoed in the additional section (root owner, type OPT,
    // CLASS = our advertised UDP size, empty RDATA). Present iff the query used
    // EDNS0.
    let opt: Option<[u8; 11]> = edns.map(|_| {
        let ty = TYPE_OPT.to_be_bytes();
        // Advertise the UDP payload size we can actually receive (MAX_UDP).
        let sz = (MAX_UDP as u16).to_be_bytes();
        // root NAME (1 byte = 0) | TYPE=OPT (2) | CLASS=UDP size (2) | TTL=0 (4)
        // | RDLEN=0 (2) = 11 bytes.
        [0, ty[0], ty[1], sz[0], sz[1], 0, 0, 0, 0, 0, 0]
    });
    let opt_len = opt.map_or(0, |o| o.len());

    let header_and_question = 12 + query.question.len();
    let mut body = Vec::with_capacity(encodable.len() * 32);
    for (ans, tc) in &encodable {
        write_name(&mut body, &ans.name);
        body.extend_from_slice(&tc.to_be_bytes());
        body.extend_from_slice(&CLASS_IN.to_be_bytes());
        body.extend_from_slice(&ans.ttl.to_be_bytes());
        let rdata = encode_rdata(&ans.rtype, &ans.value);
        body.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
        body.extend_from_slice(&rdata);
    }

    // UDP over the client's advertised size: truncate (TC set, no answers) so it
    // retries over TCP rather than getting a datagram its buffer would drop. The
    // OPT record (if any) is retained so an EDNS client still sees a valid reply.
    let (ancount, body) = match udp_limit {
        Some(limit) if header_and_question + body.len() + opt_len > limit => {
            flags |= FLAG_TC;
            (0u16, Vec::new())
        }
        _ => (encodable.len() as u16, body),
    };
    let arcount: u16 = if opt.is_some() { 1 } else { 0 };

    let mut out = Vec::with_capacity(header_and_question + body.len() + opt_len);
    out.extend_from_slice(&query.id.to_be_bytes());
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    out.extend_from_slice(&ancount.to_be_bytes()); // ANCOUNT
    out.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT
    out.extend_from_slice(&arcount.to_be_bytes()); // ARCOUNT
    out.extend_from_slice(&query.question);
    out.extend_from_slice(&body);
    if let Some(opt) = opt {
        out.extend_from_slice(&opt);
    }
    out
}

/// Write `name` as uncompressed DNS labels terminated by the root (0). Owner
/// names in the answer section can differ from the question (CNAME chase), so we
/// always encode them in full rather than using a compression pointer.
fn write_name(out: &mut Vec<u8>, name: &str) {
    for label in name.split('.') {
        if label.is_empty() {
            continue;
        }
        // Labels are <= 63 octets; truncate defensively rather than emit an
        // invalid length.
        let bytes = label.as_bytes();
        let len = bytes.len().min(63);
        out.push(len as u8);
        out.extend_from_slice(&bytes[..len]);
    }
    out.push(0);
}

/// Map a numeric DNS type to the textual type the resolver matches on. Unknown
/// types map to `""`, which matches no record (so an existing name yields
/// NODATA, an absent name NXDOMAIN).
fn qtype_to_str(qtype: u16) -> &'static str {
    match qtype {
        1 => "A",
        2 => "NS",
        5 => "CNAME",
        6 => "SOA",
        12 => "PTR",
        15 => "MX",
        16 => "TXT",
        28 => "AAAA",
        33 => "SRV",
        99 => "SPF",
        257 => "CAA",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_route53::resolver::AnswerRecord;

    /// Build a minimal A query for `name` (single question, RD set).
    fn a_query(id: u16, name: &str) -> Vec<u8> {
        let mut q = Vec::new();
        q.extend_from_slice(&id.to_be_bytes());
        q.extend_from_slice(&FLAG_RD.to_be_bytes()); // RD, QR=0
        q.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
        q.extend_from_slice(&[0, 0, 0, 0, 0, 0]); // AN/NS/AR
        write_name(&mut q, name);
        q.extend_from_slice(&1u16.to_be_bytes()); // QTYPE A
        q.extend_from_slice(&CLASS_IN.to_be_bytes());
        q
    }

    #[test]
    fn parse_roundtrips_name_and_type() {
        let q = parse_query(&a_query(0x1234, "app.example.com")).unwrap();
        assert_eq!(q.id, 0x1234);
        assert_eq!(q.qname, "app.example.com");
        assert_eq!(q.qtype, 1);
        assert!(q.rd);
    }

    #[test]
    fn parse_rejects_response_and_multi_question() {
        let mut resp = a_query(1, "x.example.com");
        resp[2] |= 0x80; // set QR
        assert!(parse_query(&resp).is_none());

        let mut two = a_query(1, "x.example.com");
        two[5] = 2; // QDCOUNT = 2
        assert!(parse_query(&two).is_none());
    }

    #[test]
    fn build_response_encodes_a_answer() {
        let query = parse_query(&a_query(0xABCD, "app.example.com")).unwrap();
        let answers = vec![AnswerRecord {
            name: "app.example.com.".to_string(),
            rtype: "A".to_string(),
            ttl: 60,
            value: "10.0.0.5".to_string(),
        }];
        let resp = build_response(&query, &answers, 0, true, true, None, None);
        // Header: same id, QR+AA+RA set, ANCOUNT = 1.
        assert_eq!(u16::from_be_bytes([resp[0], resp[1]]), 0xABCD);
        let flags = u16::from_be_bytes([resp[2], resp[3]]);
        assert_eq!(flags & FLAG_QR, FLAG_QR);
        assert_eq!(flags & FLAG_AA, FLAG_AA);
        assert_eq!(u16::from_be_bytes([resp[6], resp[7]]), 1); // ANCOUNT
                                                               // RDATA 10.0.0.5 is the last 4 bytes.
        assert_eq!(&resp[resp.len() - 4..], &[10, 0, 0, 5]);
    }

    #[test]
    fn nxdomain_sets_rcode_and_no_answers() {
        let query = parse_query(&a_query(1, "nope.example.com")).unwrap();
        let resp = build_response(&query, &[], RCODE_NXDOMAIN, true, true, None, None);
        assert_eq!(
            u16::from_be_bytes([resp[2], resp[3]]) & 0x000f,
            RCODE_NXDOMAIN
        );
        assert_eq!(u16::from_be_bytes([resp[6], resp[7]]), 0); // ANCOUNT
    }

    #[test]
    fn qtype_mapping() {
        assert_eq!(qtype_to_str(1), "A");
        assert_eq!(qtype_to_str(28), "AAAA");
        assert_eq!(qtype_to_str(15), "MX");
        assert_eq!(qtype_to_str(9999), "");
    }

    #[test]
    fn oversized_udp_answer_is_truncated_with_tc() {
        let query = parse_query(&a_query(1, "big.example.com")).unwrap();
        // Many TXT records so the encoded answer blows past a tiny UDP limit.
        let answers: Vec<AnswerRecord> = (0..50)
            .map(|i| AnswerRecord {
                name: "big.example.com.".to_string(),
                rtype: "TXT".to_string(),
                ttl: 60,
                value: format!("\"chunk number {i} with some padding text\""),
            })
            .collect();
        let resp = build_response(&query, &answers, 0, true, true, Some(512), None);
        let flags = u16::from_be_bytes([resp[2], resp[3]]);
        assert_eq!(flags & FLAG_TC, FLAG_TC, "TC must be set when truncated");
        assert_eq!(
            u16::from_be_bytes([resp[6], resp[7]]),
            0,
            "no answers when truncated"
        );
        assert!(resp.len() <= 512);
        // Over TCP (no limit) the same answer is emitted in full.
        let tcp = build_response(&query, &answers, 0, true, true, None, None);
        assert_eq!(u16::from_be_bytes([tcp[2], tcp[3]]) & FLAG_TC, 0);
        assert_eq!(u16::from_be_bytes([tcp[6], tcp[7]]), 50);
    }

    #[test]
    fn edns_size_defaults_512_and_honors_opt() {
        // No OPT -> classic 512.
        assert_eq!(opt_payload_size(&a_query(1, "x.example.com")), None);
        assert_eq!(edns_udp_size(None), 512);
        // Append an EDNS0 OPT record advertising 1232, ARCOUNT=1.
        let mut q = a_query(2, "x.example.com");
        q[11] = 1; // ARCOUNT = 1
        q.push(0); // OPT owner name = root
        q.extend_from_slice(&TYPE_OPT.to_be_bytes());
        q.extend_from_slice(&1232u16.to_be_bytes()); // CLASS = UDP payload size
        q.extend_from_slice(&[0, 0, 0, 0]); // TTL
        q.extend_from_slice(&0u16.to_be_bytes()); // RDLEN
        assert_eq!(opt_payload_size(&q), Some(1232));
        assert_eq!(edns_udp_size(Some(1232)), 1232);
    }

    #[test]
    fn edns_response_echoes_valid_opt() {
        let query = parse_query(&a_query(1, "app.example.com")).unwrap();
        let answers = vec![AnswerRecord {
            name: "app.example.com.".to_string(),
            rtype: "A".to_string(),
            ttl: 60,
            value: "10.0.0.5".to_string(),
        }];
        // Client advertised EDNS (Some size), plenty of room -> full answer + OPT.
        let resp = build_response(&query, &answers, 0, true, true, Some(4096), Some(1232));
        assert_eq!(u16::from_be_bytes([resp[6], resp[7]]), 1); // ANCOUNT
        assert_eq!(u16::from_be_bytes([resp[10], resp[11]]), 1); // ARCOUNT (OPT)
                                                                 // The OPT is the trailing 11 bytes: root name (0), TYPE=41, then class/ttl/rdlen.
        let opt = &resp[resp.len() - 11..];
        assert_eq!(opt[0], 0, "OPT owner is the root");
        assert_eq!(u16::from_be_bytes([opt[1], opt[2]]), TYPE_OPT);
        assert_eq!(u16::from_be_bytes([opt[9], opt[10]]), 0, "RDLEN 0");
    }

    #[test]
    fn formerr_sets_qr_and_rcode() {
        let resp = formerr(&[0x12, 0x34, 0xff, 0xff]);
        assert_eq!(u16::from_be_bytes([resp[0], resp[1]]), 0x1234); // id echoed
        let flags = u16::from_be_bytes([resp[2], resp[3]]);
        assert_eq!(flags & FLAG_QR, FLAG_QR);
        assert_eq!(flags & 0x000f, RCODE_FORMERR);
    }

    #[test]
    fn extract_answers_decodes_true_owner_and_chain() {
        // Reply: question cdn.example.net, then cdn CNAME edge.example.net and
        // edge.example.net A 203.0.113.7 (owner via full labels, not the query).
        let query = a_query(3, "cdn.example.net");
        let qend = super::skip_name(&query, 12).unwrap() + 4;
        let mut reply = Vec::new();
        reply.extend_from_slice(&query[0..2]);
        reply.extend_from_slice(&0x8180u16.to_be_bytes());
        reply.extend_from_slice(&1u16.to_be_bytes()); // QD
        reply.extend_from_slice(&2u16.to_be_bytes()); // AN = 2
        reply.extend_from_slice(&[0, 0, 0, 0]);
        reply.extend_from_slice(&query[12..qend]); // question

        // Answer 1: cdn.example.net CNAME edge.example.net (compressed owner).
        reply.extend_from_slice(&[0xc0, 0x0c]);
        reply.extend_from_slice(&5u16.to_be_bytes()); // TYPE CNAME
        reply.extend_from_slice(&CLASS_IN.to_be_bytes());
        reply.extend_from_slice(&30u32.to_be_bytes());
        let mut edge = Vec::new();
        write_name(&mut edge, "edge.example.net");
        reply.extend_from_slice(&(edge.len() as u16).to_be_bytes());
        let edge_at = reply.len();
        reply.extend_from_slice(&edge);

        // Answer 2: edge.example.net A 203.0.113.7 (owner points to answer 1's
        // RDATA target via a compression pointer).
        reply.extend_from_slice(&[0xc0, edge_at as u8]);
        reply.extend_from_slice(&1u16.to_be_bytes()); // TYPE A
        reply.extend_from_slice(&CLASS_IN.to_be_bytes());
        reply.extend_from_slice(&42u32.to_be_bytes());
        reply.extend_from_slice(&4u16.to_be_bytes());
        reply.extend_from_slice(&[203, 0, 113, 7]);

        let recs = extract_answers(&reply, 1);
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].rtype, "CNAME");
        assert_eq!(recs[0].name, "cdn.example.net.");
        assert_eq!(recs[0].value, "edge.example.net.");
        assert_eq!(recs[1].rtype, "A");
        // True owner preserved (edge), not stamped as the queried cdn name.
        assert_eq!(recs[1].name, "edge.example.net.");
        assert_eq!(recs[1].value, "203.0.113.7");
        assert_eq!(recs[1].ttl, 42);
    }
}
