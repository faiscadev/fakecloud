mod helpers;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use helpers::TestServer;

/// PutObject for an object larger than the legacy 128 MiB dispatch
/// cap must succeed. The streaming dispatch path skips the cap for
/// S3 PUT object routes; the per-service handler spools chunks to a
/// tempfile while computing MD5 in constant memory and hands the
/// resulting [`BodySource::File`] to the persistence store, so the
/// payload never materializes as a single in-memory `Bytes`.
/// 200 MiB is comfortably above the old 128 MiB rejection threshold
/// while staying small enough to keep the test fast.
#[tokio::test]
async fn s3_streaming_put_object_above_legacy_cap() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    s3.create_bucket()
        .bucket("stream-bucket")
        .send()
        .await
        .unwrap();

    // 200 MiB of zeros — well above the prior 128 MiB cap.
    let size = 200 * 1024 * 1024;
    let body = vec![0u8; size];

    let resp = s3
        .put_object()
        .bucket("stream-bucket")
        .key("big.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .expect("PutObject should succeed for 200 MiB body");
    assert!(resp.e_tag().is_some());

    let head = s3
        .head_object()
        .bucket("stream-bucket")
        .key("big.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_length(), Some(size as i64));

    // Round-trip the bytes via GetObject to confirm full integrity.
    let got = s3
        .get_object()
        .bucket("stream-bucket")
        .key("big.bin")
        .send()
        .await
        .unwrap();
    let got_bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(got_bytes.len(), size);
    assert!(got_bytes.iter().all(|&b| b == 0));
}

/// UploadPart with a >128 MiB part body must succeed. UploadPart goes
/// through the same streaming spool path as PutObject — each part is
/// written to a tempfile chunk-by-chunk, MD5 computed streaming, and
/// the resulting `BodyRef` (Disk in persistent mode, Memory after
/// read-back in memory mode) is what the in-memory MPU state retains.
/// A multipart upload that combines two 80 MiB parts exercises the
/// full pipeline: CreateMultipartUpload -> two streamed UploadPart
/// calls -> CompleteMultipartUpload -> GetObject round-trip.
#[tokio::test]
async fn s3_streaming_upload_part_round_trip() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    s3.create_bucket()
        .bucket("stream-mpu")
        .send()
        .await
        .unwrap();

    let init = s3
        .create_multipart_upload()
        .bucket("stream-mpu")
        .key("big.bin")
        .send()
        .await
        .unwrap();
    let upload_id = init.upload_id().unwrap().to_string();

    // Two 129 MiB parts — each above the historical 128 MiB buffered
    // cap so the test fails if either part regresses to a single
    // in-memory `Bytes` allocation.
    let part_size = 129 * 1024 * 1024;
    let part1 = vec![0xA5u8; part_size];
    let part2 = vec![0x5Au8; part_size];

    let resp1 = s3
        .upload_part()
        .bucket("stream-mpu")
        .key("big.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(part1.clone()))
        .send()
        .await
        .expect("UploadPart 1 should succeed");
    let resp2 = s3
        .upload_part()
        .bucket("stream-mpu")
        .key("big.bin")
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from(part2.clone()))
        .send()
        .await
        .expect("UploadPart 2 should succeed");

    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(resp1.e_tag().unwrap())
                .build(),
        )
        .parts(
            CompletedPart::builder()
                .part_number(2)
                .e_tag(resp2.e_tag().unwrap())
                .build(),
        )
        .build();

    s3.complete_multipart_upload()
        .bucket("stream-mpu")
        .key("big.bin")
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("CompleteMultipartUpload should succeed");

    let got = s3
        .get_object()
        .bucket("stream-mpu")
        .key("big.bin")
        .send()
        .await
        .unwrap();
    let got_bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(got_bytes.len(), part_size * 2);
    assert!(got_bytes[..part_size].iter().all(|&b| b == 0xA5));
    assert!(got_bytes[part_size..].iter().all(|&b| b == 0x5A));
}

/// A request S3 refuses for its URL alone (conflicting resource-type
/// parameters, a part number with no upload id, an upload that does not
/// exist) is refused without its body being read: the answer comes while
/// the body is still being sent, and never ends.
#[tokio::test]
async fn s3_refusals_of_the_url_do_not_wait_for_the_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket().bucket("unread").send().await.unwrap();
    let addr = server
        .endpoint()
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();
    for (target, code) in [
        ("/unread/k?partNumber=1", "InvalidArgument"),
        ("/unread/k?tagging&acl", "InvalidArgument"),
        ("/unread/k?partNumber=1&uploadId=nope", "NoSuchUpload"),
        ("/unread?cors&tagging", "InvalidArgument"),
    ] {
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let head = format!(
            "PUT {target} HTTP/1.1\r\nHost: {addr}\r\n\
             Authorization: AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00\r\n\
             x-amz-content-sha256: UNSIGNED-PAYLOAD\r\n\
             Transfer-Encoding: chunked\r\n\r\n"
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        // One chunk of the body; its end never comes.
        let chunk = vec![b'x'; 64 * 1024];
        stream
            .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
            .await
            .unwrap();
        stream.write_all(&chunk).await.unwrap();
        stream.write_all(b"\r\n").await.unwrap();
        let mut response = String::new();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let mut buf = vec![0u8; 4096];
            while !response.contains("</Error>") {
                let read = stream.read(&mut buf).await.unwrap();
                if read == 0 {
                    break;
                }
                response.push_str(&String::from_utf8_lossy(&buf[..read]));
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{target}: no answer while the body is open: {response}"));
        assert!(
            response.contains(&format!("<Code>{code}</Code>")),
            "{target}: {response}"
        );
    }
}
