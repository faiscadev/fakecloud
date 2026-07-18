//! End-to-end tests for the `--dns` real resolver (#2219): records created in
//! Route 53 are answered by an actual DNS server, and names in no local zone are
//! forwarded to an upstream resolver.
//!
//! The DNS wire helpers here are hand-rolled independently of the server's own
//! codec, so a passing test cross-checks the two implementations.

mod helpers;

use std::time::Duration;

use aws_sdk_route53::types::{
    Change, ChangeAction, ChangeBatch, HostedZoneConfig, ResourceRecord, ResourceRecordSet, RrType,
};
use helpers::TestServer;
use tokio::net::UdpSocket;

// ---- minimal DNS wire helpers (test-side, independent of the server codec) ----

fn write_name(out: &mut Vec<u8>, name: &str) {
    for label in name.split('.') {
        if label.is_empty() {
            continue;
        }
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
}

fn build_query(id: u16, name: &str, qtype: u16) -> Vec<u8> {
    let mut q = Vec::new();
    q.extend_from_slice(&id.to_be_bytes());
    q.extend_from_slice(&0x0100u16.to_be_bytes()); // RD, QR=0
    q.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    q.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
    write_name(&mut q, name);
    q.extend_from_slice(&qtype.to_be_bytes());
    q.extend_from_slice(&1u16.to_be_bytes()); // IN
    q
}

/// Advance `pos` past a (possibly compressed) DNS name in `buf`.
fn skip_name(buf: &[u8], mut pos: usize) -> usize {
    loop {
        let len = buf[pos] as usize;
        if len == 0 {
            return pos + 1;
        }
        if len & 0xc0 == 0xc0 {
            return pos + 2; // compression pointer: 2 bytes, name ends here
        }
        pos += 1 + len;
    }
}

struct Answer {
    rtype: u16,
    rdata: Vec<u8>,
}

struct DnsReply {
    rcode: u16,
    answers: Vec<Answer>,
}

fn parse_reply(buf: &[u8]) -> DnsReply {
    let rcode = u16::from_be_bytes([buf[2], buf[3]]) & 0x000f;
    let qd = u16::from_be_bytes([buf[4], buf[5]]);
    let an = u16::from_be_bytes([buf[6], buf[7]]);
    let mut pos = 12;
    for _ in 0..qd {
        pos = skip_name(buf, pos);
        pos += 4; // qtype + qclass
    }
    let mut answers = Vec::new();
    for _ in 0..an {
        pos = skip_name(buf, pos);
        let rtype = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
        pos += 8; // type(2) + class(2) + ttl(4)
        let rdlen = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        let rdata = buf[pos..pos + rdlen].to_vec();
        pos += rdlen;
        answers.push(Answer { rtype, rdata });
    }
    DnsReply { rcode, answers }
}

/// Reserve a free UDP port on loopback (bind, read the port, drop the socket).
async fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Send `query` to the resolver at `127.0.0.1:port`, retrying briefly because
/// the DNS listener comes up as a detached task just after the HTTP server.
async fn dns_query(port: u16, query: &[u8]) -> DnsReply {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sock.connect(("127.0.0.1", port)).await.unwrap();
    let mut buf = vec![0u8; 4096];
    for attempt in 0..40 {
        sock.send(query).await.unwrap();
        match tokio::time::timeout(Duration::from_millis(250), sock.recv(&mut buf)).await {
            Ok(Ok(n)) => {
                buf.truncate(n);
                return parse_reply(&buf);
            }
            _ => {
                if attempt < 39 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    panic!("no DNS reply from 127.0.0.1:{port}");
}

async fn create_record(server: &TestServer, zone_id: &str, name: &str, rtype: RrType, value: &str) {
    let r53 = server.route53_client().await;
    r53.change_resource_record_sets()
        .hosted_zone_id(zone_id)
        .change_batch(
            ChangeBatch::builder()
                .changes(
                    Change::builder()
                        .action(ChangeAction::Create)
                        .resource_record_set(
                            ResourceRecordSet::builder()
                                .name(name)
                                .r#type(rtype)
                                .ttl(60)
                                .resource_records(
                                    ResourceRecord::builder().value(value).build().unwrap(),
                                )
                                .build()
                                .unwrap(),
                        )
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create record");
}

async fn create_zone(server: &TestServer, name: &str) -> String {
    let r53 = server.route53_client().await;
    let create = r53
        .create_hosted_zone()
        .name(name)
        .caller_reference(format!("dns-e2e-{name}"))
        .hosted_zone_config(HostedZoneConfig::builder().private_zone(false).build())
        .send()
        .await
        .expect("create zone");
    create.hosted_zone().unwrap().id().to_string()
}

const TYPE_A: u16 = 1;
const TYPE_CNAME: u16 = 5;
const TYPE_MX: u16 = 15;
const TYPE_TXT: u16 = 16;
const TYPE_SRV: u16 = 33;

#[tokio::test]
async fn resolves_a_mx_txt_and_cname_from_route53() {
    let port = free_udp_port().await;
    let server =
        TestServer::start_full(&[], &["--dns", "--dns-addr", &format!("127.0.0.1:{port}")]).await;

    let zone = create_zone(&server, "example.com").await;
    create_record(&server, &zone, "app.example.com", RrType::A, "10.0.0.5").await;
    create_record(
        &server,
        &zone,
        "example.com",
        RrType::Mx,
        "10 mail.example.com",
    )
    .await;
    create_record(
        &server,
        &zone,
        "example.com",
        RrType::Txt,
        "\"hello world\"",
    )
    .await;
    create_record(
        &server,
        &zone,
        "www.example.com",
        RrType::Cname,
        "app.example.com",
    )
    .await;
    create_record(
        &server,
        &zone,
        "_sip._tcp.example.com",
        RrType::Srv,
        "10 60 5060 sip.example.com",
    )
    .await;

    // A record resolves to the configured IP.
    let a = dns_query(port, &build_query(1, "app.example.com", TYPE_A)).await;
    assert_eq!(a.rcode, 0);
    let a_rec = a
        .answers
        .iter()
        .find(|r| r.rtype == TYPE_A)
        .expect("A answer");
    assert_eq!(a_rec.rdata, vec![10, 0, 0, 5]);

    // MX: pref (2 bytes) + name; check the preference and that a name follows.
    let mx = dns_query(port, &build_query(2, "example.com", TYPE_MX)).await;
    let mx_rec = mx
        .answers
        .iter()
        .find(|r| r.rtype == TYPE_MX)
        .expect("MX answer");
    assert_eq!(u16::from_be_bytes([mx_rec.rdata[0], mx_rec.rdata[1]]), 10);
    assert!(mx_rec.rdata.len() > 2);

    // TXT: length-prefixed char-string, quotes stripped by the encoder.
    let txt = dns_query(port, &build_query(3, "example.com", TYPE_TXT)).await;
    let txt_rec = txt
        .answers
        .iter()
        .find(|r| r.rtype == TYPE_TXT)
        .expect("TXT answer");
    assert_eq!(txt_rec.rdata[0] as usize, txt_rec.rdata.len() - 1);
    assert_eq!(&txt_rec.rdata[1..], b"hello world");

    // CNAME query returns the CNAME; an A query for the alias chases to the A.
    let cname = dns_query(port, &build_query(4, "www.example.com", TYPE_CNAME)).await;
    assert!(cname.answers.iter().any(|r| r.rtype == TYPE_CNAME));
    let chased = dns_query(port, &build_query(5, "www.example.com", TYPE_A)).await;
    assert!(chased.answers.iter().any(|r| r.rtype == TYPE_CNAME));
    let a2 = chased
        .answers
        .iter()
        .find(|r| r.rtype == TYPE_A)
        .expect("chased A");
    assert_eq!(a2.rdata, vec![10, 0, 0, 5]);

    // SRV: priority(2) weight(2) port(2) + target name. Check the parsed numbers.
    let srv = dns_query(port, &build_query(6, "_sip._tcp.example.com", TYPE_SRV)).await;
    let srv_rec = srv
        .answers
        .iter()
        .find(|r| r.rtype == TYPE_SRV)
        .expect("SRV answer");
    assert_eq!(u16::from_be_bytes([srv_rec.rdata[0], srv_rec.rdata[1]]), 10); // priority
    assert_eq!(u16::from_be_bytes([srv_rec.rdata[2], srv_rec.rdata[3]]), 60); // weight
    assert_eq!(
        u16::from_be_bytes([srv_rec.rdata[4], srv_rec.rdata[5]]),
        5060
    ); // port
    assert!(srv_rec.rdata.len() > 6, "SRV target name follows");
}

#[tokio::test]
async fn nxdomain_for_unknown_name_in_local_zone() {
    let port = free_udp_port().await;
    let server =
        TestServer::start_full(&[], &["--dns", "--dns-addr", &format!("127.0.0.1:{port}")]).await;
    create_zone(&server, "example.com").await;

    let reply = dns_query(port, &build_query(9, "missing.example.com", TYPE_A)).await;
    assert_eq!(
        reply.rcode, 3,
        "expected NXDOMAIN for a name absent from an authoritative zone"
    );
    assert!(reply.answers.is_empty());
}

/// Spawn a tiny upstream resolver stub that answers any query with A 9.9.9.9,
/// echoing the transaction id + question so the reply is well-formed. Returns
/// its port.
async fn spawn_upstream_stub() -> u16 {
    let stub = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let stub_port = stub.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];
        loop {
            let Ok((n, peer)) = stub.recv_from(&mut buf).await else {
                return;
            };
            let query = &buf[..n];
            let qend = skip_name(query, 12) + 4;
            let mut resp = Vec::new();
            resp.extend_from_slice(&query[0..2]); // id (echoed -> passes txid check)
            resp.extend_from_slice(&0x8180u16.to_be_bytes()); // QR + RD + RA
            resp.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
            resp.extend_from_slice(&1u16.to_be_bytes()); // ANCOUNT
            resp.extend_from_slice(&[0, 0, 0, 0]);
            resp.extend_from_slice(&query[12..qend]); // question
            resp.extend_from_slice(&[0xc0, 0x0c]); // name pointer to question
            resp.extend_from_slice(&1u16.to_be_bytes()); // TYPE A
            resp.extend_from_slice(&1u16.to_be_bytes()); // CLASS IN
            resp.extend_from_slice(&60u32.to_be_bytes()); // TTL
            resp.extend_from_slice(&4u16.to_be_bytes()); // RDLENGTH
            resp.extend_from_slice(&[9, 9, 9, 9]); // 9.9.9.9
            let _ = stub.send_to(&resp, peer).await;
        }
    });
    stub_port
}

#[tokio::test]
async fn forwards_non_local_names_to_upstream() {
    let stub_port = spawn_upstream_stub().await;
    let port = free_udp_port().await;
    let server = TestServer::start_full(
        &[],
        &[
            "--dns",
            "--dns-addr",
            &format!("127.0.0.1:{port}"),
            "--dns-upstream",
            &format!("127.0.0.1:{stub_port}"),
        ],
    )
    .await;
    // Only a local zone for example.com; the queried name is outside it.
    create_zone(&server, "example.com").await;

    let reply = dns_query(port, &build_query(7, "registry-1.docker.io", TYPE_A)).await;
    let a = reply
        .answers
        .iter()
        .find(|r| r.rtype == TYPE_A)
        .expect("forwarded A answer");
    assert_eq!(
        a.rdata,
        vec![9, 9, 9, 9],
        "expected the upstream stub's answer to be relayed"
    );
}

#[tokio::test]
async fn external_cname_target_is_chased_via_upstream() {
    let stub_port = spawn_upstream_stub().await;
    let port = free_udp_port().await;
    let server = TestServer::start_full(
        &[],
        &[
            "--dns",
            "--dns-addr",
            &format!("127.0.0.1:{port}"),
            "--dns-upstream",
            &format!("127.0.0.1:{stub_port}"),
        ],
    )
    .await;
    // A local CNAME pointing at an EXTERNAL name (in no Route 53 zone).
    let zone = create_zone(&server, "example.com").await;
    create_record(
        &server,
        &zone,
        "www.example.com",
        RrType::Cname,
        "cdn.cloudfront.net",
    )
    .await;

    // An A query returns the CNAME plus the address forward-resolved from the
    // external target, so a stub client that doesn't chase CNAMEs still gets an IP.
    let reply = dns_query(port, &build_query(11, "www.example.com", TYPE_A)).await;
    assert_eq!(reply.rcode, 0);
    assert!(
        reply.answers.iter().any(|r| r.rtype == TYPE_CNAME),
        "expected the local CNAME in the answer"
    );
    let a = reply
        .answers
        .iter()
        .find(|r| r.rtype == TYPE_A)
        .expect("expected an address chased from the external CNAME target");
    assert_eq!(a.rdata, vec![9, 9, 9, 9]);
}

// The introspection endpoint (Batch 2) returns the same resolution over HTTP so
// a test can assert it without binding a socket. Exercised here via the Rust SDK
// wrapper `dns_resolve`; the server is started WITHOUT `--dns` to prove the
// endpoint is independent of the UDP/TCP listener.
#[tokio::test]
async fn introspection_endpoint_resolves_created_record_via_sdk() {
    let server = TestServer::start_full(&[], &[]).await;
    let zone_id = create_zone(&server, "example.com.").await;
    create_record(&server, &zone_id, "app.example.com.", RrType::A, "10.0.0.5").await;

    let sdk = fakecloud_sdk::FakeCloud::new(server.endpoint());

    // Answered: the created A record comes back with its value + ttl.
    let res = sdk
        .dns_resolve("app.example.com", "A")
        .await
        .expect("dns_resolve");
    assert_eq!(res.status, "ANSWERED");
    assert!(res.authoritative);
    assert_eq!(res.records.len(), 1);
    assert_eq!(res.records[0].value, "10.0.0.5");
    assert_eq!(res.records[0].ttl, 60);
    assert_eq!(res.records[0].record_type, "A");

    // Name in the zone but no AAAA record -> NODATA (still authoritative).
    let nodata = sdk
        .dns_resolve("app.example.com", "AAAA")
        .await
        .expect("dns_resolve aaaa");
    assert_eq!(nodata.status, "NODATA");
    assert!(nodata.authoritative);
    assert!(nodata.records.is_empty());

    // Name outside every local zone -> NOT_AUTHORITATIVE (the resolver forwards).
    let foreign = sdk
        .dns_resolve("registry-1.docker.io", "A")
        .await
        .expect("dns_resolve foreign");
    assert_eq!(foreign.status, "NOT_AUTHORITATIVE");
    assert!(!foreign.authoritative);
}
