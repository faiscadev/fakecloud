//! Decoding a Fetch response's record set: v2 (magic 2) record batches,
//! CRC-32C checked, compressed with any of Kafka's codecs (gzip, Snappy in
//! raw or xerial framing, LZ4 frames, zstd). Control batches (transaction
//! markers) carry no records for a consumer and are skipped, but still move
//! the fetch position past them.

use std::io::Read;

use super::wire::{Decoder, WireError, WireResult};

/// A decompressed batch larger than this is refused rather than buffered.
const MAX_DECOMPRESSED: u64 = 256 * 1024 * 1024;

/// How a record's timestamp was assigned (the batch's attribute bit 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    CreateTime,
    LogAppendTime,
}

impl TimestampType {
    /// The name Lambda's Kafka event uses (`timestampType`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CreateTime => "CREATE_TIME",
            Self::LogAppendTime => "LOG_APPEND_TIME",
        }
    }
}

/// One record read from a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumedRecord {
    pub offset: i64,
    /// Milliseconds since the epoch.
    pub timestamp_ms: i64,
    pub timestamp_type: TimestampType,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

/// The records of a record set at or past `from_offset`, and the offset after
/// the last complete batch (`None` when no batch was complete). A trailing
/// partial batch, which a broker may send when it truncates to the fetch
/// size, is left for the next fetch.
pub fn decode_record_set(
    data: &[u8],
    from_offset: i64,
) -> WireResult<(Vec<ConsumedRecord>, Option<i64>)> {
    decode_committed_record_set(data, from_offset, &[])
}

/// [`decode_record_set`] for a `read_committed` fetch: the records of
/// transactions the partition's `aborted` list (`(producer id, first
/// offset)`, from the Fetch response) names are dropped, as a Java consumer
/// drops them. A producer's transaction is aborted from its first offset
/// until the producer's ABORT marker.
pub fn decode_committed_record_set(
    data: &[u8],
    from_offset: i64,
    aborted: &[(i64, i64)],
) -> WireResult<(Vec<ConsumedRecord>, Option<i64>)> {
    let mut aborted = aborted.to_vec();
    aborted.sort_by_key(|(_, first)| *first);
    let mut pending_aborts = aborted.into_iter().peekable();
    let mut aborting = std::collections::HashSet::new();
    let mut d = Decoder::new(data);
    let mut records = Vec::new();
    let mut next_offset: Option<i64> = None;
    // baseOffset (8) + batchLength (4) + partitionLeaderEpoch (4) + magic (1).
    while d.remaining() >= 17 {
        let base_offset = d.i64()?;
        let batch_len = d.i32()?;
        if batch_len < 0 {
            return Err(WireError(format!("negative batch length {batch_len}")));
        }
        if batch_len as usize > d.remaining() {
            break;
        }
        let batch = decode_batch(base_offset, d.take(batch_len as usize)?, from_offset)?;
        next_offset =
            Some(next_offset.map_or(batch.last_offset + 1, |n: i64| n.max(batch.last_offset + 1)));
        if batch.transactional {
            while let Some((producer, _)) =
                pending_aborts.next_if(|(_, first)| *first <= batch.last_offset)
            {
                aborting.insert(producer);
            }
        }
        match batch.control {
            // A transaction marker is no record; an ABORT ends the abort.
            Some(abort) => {
                if abort {
                    aborting.remove(&batch.producer_id);
                }
            }
            None if batch.transactional && aborting.contains(&batch.producer_id) => {}
            None => records.extend(batch.records),
        }
    }
    Ok((records, next_offset))
}

/// One decoded batch.
struct Batch {
    last_offset: i64,
    producer_id: i64,
    transactional: bool,
    /// For a control batch, whether its marker is an ABORT.
    control: Option<bool>,
    records: Vec<ConsumedRecord>,
}

/// Decode one batch (the bytes after its `batchLength`).
fn decode_batch(base_offset: i64, batch: &[u8], from_offset: i64) -> WireResult<Batch> {
    let mut b = Decoder::new(batch);
    let _partition_leader_epoch = b.i32()?;
    let magic = b.i8()?;
    if magic != 2 {
        return Err(WireError(format!(
            "the partition holds messages in the legacy message format v{magic} (magic {magic}) at offset {base_offset}, which fakecloud's Kafka poller does not read; only record batches (magic 2, Kafka 0.11+) are supported"
        )));
    }
    let crc = b.u32()?;
    if crc32c::crc32c(b.peek_rest()) != crc {
        return Err(WireError(format!(
            "record batch at offset {base_offset} failed its CRC check"
        )));
    }
    let attributes = b.i16()?;
    let last_offset_delta = b.i32()?;
    let base_timestamp = b.i64()?;
    let max_timestamp = b.i64()?;
    let producer_id = b.i64()?;
    let _producer_epoch = b.i16()?;
    let _base_sequence = b.i32()?;
    let count = b.i32()?;
    let mut out = Batch {
        last_offset: base_offset + i64::from(last_offset_delta),
        producer_id,
        transactional: attributes & 0x10 != 0,
        control: None,
        records: Vec::new(),
    };
    let timestamp_type = if attributes & 0x08 != 0 {
        TimestampType::LogAppendTime
    } else {
        TimestampType::CreateTime
    };
    let payload = decompress(attributes & 0x07, b.rest())?;
    let mut r = Decoder::new(&payload);
    if attributes & 0x20 != 0 {
        // A control record's key is `version (i16), type (i16)`: 0 ABORT,
        // 1 COMMIT.
        let mut abort = false;
        if count > 0 {
            let len = r.varint()?;
            let mut rec = Decoder::new(r.take(len.max(0) as usize)?);
            let _attributes = rec.i8()?;
            let _ts = rec.varlong()?;
            let _off = rec.varint()?;
            if let Some(key) = rec.var_bytes()? {
                let mut k = Decoder::new(key);
                let _version = k.i16()?;
                abort = k.i16()? == 0;
            }
        }
        out.control = Some(abort);
        return Ok(out);
    }
    let out_records = &mut out.records;
    for _ in 0..count.max(0) {
        let len = r.varint()?;
        if len < 0 {
            return Err(WireError(format!("negative record length {len}")));
        }
        let mut rec = Decoder::new(r.take(len as usize)?);
        let _attributes = rec.i8()?;
        let timestamp_delta = rec.varlong()?;
        let offset_delta = rec.varint()?;
        let key = rec.var_bytes()?.map(<[u8]>::to_vec);
        let value = rec.var_bytes()?.map(<[u8]>::to_vec);
        let header_count = rec.varint()?;
        let mut headers = Vec::new();
        for _ in 0..header_count.max(0) {
            let name = rec
                .var_bytes()?
                .ok_or_else(|| WireError("null record header key".to_string()))?;
            let name = String::from_utf8_lossy(name).into_owned();
            headers.push((name, rec.var_bytes()?.map(<[u8]>::to_vec)));
        }
        let offset = base_offset + i64::from(offset_delta);
        if offset < from_offset {
            continue;
        }
        out_records.push(ConsumedRecord {
            offset,
            timestamp_ms: match timestamp_type {
                TimestampType::LogAppendTime => max_timestamp,
                TimestampType::CreateTime => base_timestamp + timestamp_delta,
            },
            timestamp_type,
            key,
            value,
            headers,
        });
    }
    Ok(out)
}

/// The xerial framing the Java client writes Snappy batches in.
const XERIAL_MAGIC: &[u8] = b"\x82SNAPPY\x00";

fn decompress(codec: i16, data: &[u8]) -> WireResult<Vec<u8>> {
    let failed = |name: &str, e: &dyn std::fmt::Display| {
        WireError(format!("could not decompress a {name} record batch: {e}"))
    };
    match codec {
        0 => Ok(data.to_vec()),
        1 => read_capped(flate2::read::MultiGzDecoder::new(data)).map_err(|e| failed("gzip", &e)),
        2 => snappy(data).map_err(|e| failed("snappy", &e)),
        3 => read_capped(lz4_flex::frame::FrameDecoder::new(data)).map_err(|e| failed("lz4", &e)),
        4 => zstd::stream::read::Decoder::new(data)
            .and_then(read_capped)
            .map_err(|e| failed("zstd", &e)),
        other => Err(WireError(format!("unknown compression codec {other}"))),
    }
}

fn read_capped(reader: impl Read) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    reader.take(MAX_DECOMPRESSED + 1).read_to_end(&mut out)?;
    if out.len() as u64 > MAX_DECOMPRESSED {
        return Err(std::io::Error::other("decompressed batch is too large"));
    }
    Ok(out)
}

fn snappy(data: &[u8]) -> Result<Vec<u8>, String> {
    let mut decoder = snap::raw::Decoder::new();
    let Some(framed) = data.strip_prefix(XERIAL_MAGIC) else {
        return decoder.decompress_vec(data).map_err(|e| e.to_string());
    };
    // version (4) + compatible version (4), then `i32`-length raw blocks.
    let mut d = Decoder::new(framed);
    d.take(8).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    while d.remaining() > 0 {
        let block = d
            .nullable_bytes()
            .map_err(|e| e.to_string())?
            .unwrap_or_default();
        out.extend(decoder.decompress_vec(block).map_err(|e| e.to_string())?);
        if out.len() as u64 > MAX_DECOMPRESSED {
            return Err("decompressed batch is too large".to_string());
        }
    }
    Ok(out)
}

/// Encoding v2 record batches, as a producer writes them (uncompressed or
/// gzip): what an in-process broker double serves to the decoder.
pub mod encode {
    use super::super::wire::Encoder;

    /// One record to encode: `(timestamp delta, key, value, headers)`.
    pub type TestRecord<'a> = (
        i64,
        Option<&'a [u8]>,
        Option<&'a [u8]>,
        Vec<(&'a str, Option<&'a [u8]>)>,
    );

    fn var_bytes(e: &mut Encoder, b: Option<&[u8]>) {
        match b {
            Some(b) => {
                e.varint(b.len() as i32).raw(b);
            }
            None => {
                e.varint(-1);
            }
        }
    }

    /// The bytes of one batch at `base_offset` (codec 0 = uncompressed, 1 =
    /// gzip), `attributes` extra bits OR'd in (0x08 log-append time, 0x20
    /// control).
    pub fn batch(
        base_offset: i64,
        base_timestamp: i64,
        codec: i16,
        extra_attributes: i16,
        records: &[TestRecord<'_>],
    ) -> Vec<u8> {
        producer_batch(
            base_offset,
            base_timestamp,
            codec,
            extra_attributes,
            -1,
            records,
        )
    }

    /// [`batch`] written by producer `producer_id` (0x10 in
    /// `extra_attributes` makes it transactional).
    pub fn producer_batch(
        base_offset: i64,
        base_timestamp: i64,
        codec: i16,
        extra_attributes: i16,
        producer_id: i64,
        records: &[TestRecord<'_>],
    ) -> Vec<u8> {
        let mut body = Encoder::new();
        for (i, (ts_delta, key, value, headers)) in records.iter().enumerate() {
            let mut rec = Encoder::new();
            rec.i8(0).varlong(*ts_delta).varint(i as i32);
            var_bytes(&mut rec, *key);
            var_bytes(&mut rec, *value);
            rec.varint(headers.len() as i32);
            for (k, v) in headers {
                var_bytes(&mut rec, Some(k.as_bytes()));
                var_bytes(&mut rec, *v);
            }
            let rec = rec.into_bytes();
            body.varint(rec.len() as i32).raw(&rec);
        }
        let mut payload = body.into_bytes();
        if codec == 1 {
            use std::io::Write;
            let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            gz.write_all(&payload).expect("gzip into a Vec cannot fail");
            payload = gz.finish().expect("gzip into a Vec cannot fail");
        }
        let max_ts = base_timestamp
            + records
                .iter()
                .map(|(d, _, _, _)| *d)
                .max()
                .unwrap_or_default();
        let mut after_crc = Encoder::new();
        after_crc
            .i16(codec | extra_attributes)
            .i32(records.len().saturating_sub(1) as i32)
            .i64(base_timestamp)
            .i64(max_ts)
            .i64(producer_id)
            .i16(-1)
            .i32(-1)
            .i32(records.len() as i32)
            .raw(&payload);
        let after_crc = after_crc.into_bytes();
        let mut after_len = Encoder::new();
        after_len
            .i32(0)
            .i8(2)
            .raw(&crc32c::crc32c(&after_crc).to_be_bytes())
            .raw(&after_crc);
        let after_len = after_len.into_bytes();
        let mut out = Encoder::new();
        out.i64(base_offset)
            .i32(after_len.len() as i32)
            .raw(&after_len);
        out.into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::encode::batch;
    use super::*;

    #[test]
    fn decodes_records_keys_values_and_headers() {
        let data = batch(
            40,
            1_700_000_000_000,
            0,
            0,
            &[
                (0, Some(b"k1"), Some(b"v1"), vec![("h", Some(&b"x"[..]))]),
                (5, None, Some(b"v2"), vec![]),
                (9, Some(b"k3"), None, vec![("n", None)]),
            ],
        );
        let (records, next) = decode_record_set(&data, 0).unwrap();
        assert_eq!(next, Some(43));
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset, 40);
        assert_eq!(records[0].key.as_deref(), Some(&b"k1"[..]));
        assert_eq!(
            records[0].headers,
            vec![("h".to_string(), Some(b"x".to_vec()))]
        );
        assert_eq!(records[1].timestamp_ms, 1_700_000_000_005);
        assert_eq!(records[1].key, None);
        assert_eq!(records[2].value, None);
        assert_eq!(records[2].headers, vec![("n".to_string(), None)]);
        assert_eq!(records[2].timestamp_type, TimestampType::CreateTime);
    }

    #[test]
    fn skips_records_before_the_fetch_offset_and_reads_gzip() {
        let data = batch(
            10,
            0,
            1,
            0,
            &[
                (0, None, Some(b"a"), vec![]),
                (0, None, Some(b"b"), vec![]),
                (0, None, Some(b"c"), vec![]),
            ],
        );
        let (records, next) = decode_record_set(&data, 11).unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![11, 12]
        );
        assert_eq!(records[1].value.as_deref(), Some(&b"c"[..]));
        assert_eq!(next, Some(13));
    }

    #[test]
    fn log_append_time_uses_the_batch_max_timestamp() {
        let data = batch(
            0,
            1000,
            0,
            0x08,
            &[(0, None, None, vec![]), (7, None, None, vec![])],
        );
        let (records, _) = decode_record_set(&data, 0).unwrap();
        assert!(records
            .iter()
            .all(|r| r.timestamp_type == TimestampType::LogAppendTime && r.timestamp_ms == 1007));
    }

    #[test]
    fn control_batches_advance_without_records() {
        let mut data = batch(0, 0, 0, 0, &[(0, None, Some(b"a"), vec![])]);
        data.extend(batch(
            1,
            0,
            0,
            0x20,
            &[(0, Some(b"\0\0\0\0"), None, vec![])],
        ));
        let (records, next) = decode_record_set(&data, 0).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(next, Some(2));
    }

    #[test]
    fn read_committed_drops_aborted_transactions() {
        use super::encode::producer_batch;
        let txn = 0x10;
        let control = 0x30;
        let mut data = producer_batch(
            0,
            0,
            0,
            txn,
            7,
            &[(0, None, Some(b"a"), vec![]), (0, None, Some(b"b"), vec![])],
        );
        data.extend(producer_batch(
            2,
            0,
            0,
            control,
            7,
            &[(0, Some(b"\0\0\0\0"), None, vec![])],
        ));
        data.extend(producer_batch(
            3,
            0,
            0,
            txn,
            8,
            &[(0, None, Some(b"c"), vec![])],
        ));
        data.extend(producer_batch(
            4,
            0,
            0,
            control,
            8,
            &[(0, Some(b"\0\0\0\x01"), None, vec![])],
        ));
        data.extend(batch(5, 0, 0, 0, &[(0, None, Some(b"d"), vec![])]));
        let values = |records: &[ConsumedRecord]| -> Vec<Vec<u8>> {
            records.iter().map(|r| r.value.clone().unwrap()).collect()
        };
        let (records, next) = decode_committed_record_set(&data, 0, &[(7, 0)]).unwrap();
        assert_eq!(values(&records), vec![b"c".to_vec(), b"d".to_vec()]);
        assert_eq!(next, Some(6));
        // Without the aborted list (read_uncommitted) every data record reads.
        let (records, _) = decode_record_set(&data, 0).unwrap();
        assert_eq!(records.len(), 4);
    }

    #[test]
    fn legacy_message_sets_are_refused_with_a_clear_reason() {
        let mut data = Vec::new();
        data.extend_from_slice(&0i64.to_be_bytes());
        data.extend_from_slice(&14i32.to_be_bytes());
        // crc, magic 1, attributes, timestamp...
        data.extend_from_slice(&[0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let err = decode_record_set(&data, 0).unwrap_err();
        assert!(err.0.contains("legacy message format v1"), "{err}");
    }

    #[test]
    fn a_trailing_partial_batch_is_left_for_the_next_fetch() {
        let mut data = batch(0, 0, 0, 0, &[(0, None, Some(b"a"), vec![])]);
        let second = batch(1, 0, 0, 0, &[(0, None, Some(b"b"), vec![])]);
        data.extend_from_slice(&second[..second.len() - 3]);
        let (records, next) = decode_record_set(&data, 0).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(next, Some(1));
    }

    #[test]
    fn a_corrupt_batch_fails_its_crc() {
        let mut data = batch(0, 0, 0, 0, &[(0, None, Some(b"abc"), vec![])]);
        let n = data.len();
        data[n - 1] ^= 0xff;
        assert!(decode_record_set(&data, 0).is_err());
    }

    #[test]
    fn snappy_reads_raw_and_xerial_framing() {
        let plain = b"hello hello hello hello".to_vec();
        let block = snap::raw::Encoder::new().compress_vec(&plain).unwrap();
        assert_eq!(snappy(&block).unwrap(), plain);
        let mut framed = XERIAL_MAGIC.to_vec();
        framed.extend_from_slice(&1i32.to_be_bytes());
        framed.extend_from_slice(&1i32.to_be_bytes());
        framed.extend_from_slice(&(block.len() as i32).to_be_bytes());
        framed.extend_from_slice(&block);
        assert_eq!(snappy(&framed).unwrap(), plain);
    }

    #[test]
    fn lz4_and_zstd_frames_decompress() {
        use std::io::Write;
        let plain = b"kafka kafka kafka".to_vec();
        let mut lz = lz4_flex::frame::FrameEncoder::new(Vec::new());
        lz.write_all(&plain).unwrap();
        assert_eq!(decompress(3, &lz.finish().unwrap()).unwrap(), plain);
        let z = zstd::stream::encode_all(&plain[..], 0).unwrap();
        assert_eq!(decompress(4, &z).unwrap(), plain);
    }
}
