use std::io::Write;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use fakecloud_s3::delivery::DeliveredEncryption;
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compression {
    Uncompressed,
    Gzip,
    Snappy,
}

impl Compression {
    /// The single source of truth for which modeled `CompressionFormat`
    /// values delivery implements; admission and output selection both use it.
    pub(crate) fn from_format(format: &str) -> Option<Self> {
        match format {
            "UNCOMPRESSED" => Some(Self::Uncompressed),
            "GZIP" => Some(Self::Gzip),
            "Snappy" => Some(Self::Snappy),
            _ => None,
        }
    }

    pub(crate) fn content_encoding(self) -> Option<&'static str> {
        match self {
            Self::Uncompressed => None,
            Self::Gzip => Some("gzip"),
            Self::Snappy => Some("snappy-java"),
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::Uncompressed => "",
            Self::Gzip => ".gz",
            Self::Snappy => ".snappy",
        }
    }
}

#[derive(Clone, Debug)]
pub struct OutputConfig {
    pub prefix: String,
    pub file_extension: Option<String>,
    pub compression: Compression,
    /// The built-in `AppendDelimiterToRecord` processor: write `\n` after
    /// every record.
    pub append_delimiter: bool,
}

/// Whether an S3 destination's `ProcessingConfiguration` enables the built-in
/// `AppendDelimiterToRecord` processor. Only `Enabled: true` counts. AWS
/// documents only a new line delimiter, so a `Delimiter` parameter is not
/// interpreted. Delivery admission (`config::delivery_config`) rejects any
/// other enabled processor as not implemented rather than skipping it.
pub(crate) fn append_delimiter_enabled(processing_configuration: Option<&Value>) -> bool {
    processing_configuration.is_some_and(|processing| {
        processing["Enabled"].as_bool() == Some(true)
            && processing["Processors"]
                .as_array()
                .is_some_and(|processors| {
                    processors
                        .iter()
                        .any(|processor| processor["Type"] == "AppendDelimiterToRecord")
                })
    })
}

/// The server-side encryption of a delivered object: SSE-KMS under
/// `EncryptionConfiguration.KMSEncryptionConfig.AWSKMSKeyARN`, as Firehose
/// writes it; otherwise (`NoEncryptionConfig`, or none) the bucket default.
pub(crate) fn delivery_encryption(encryption_configuration: Option<&Value>) -> DeliveredEncryption {
    match encryption_configuration
        .and_then(|config| config["KMSEncryptionConfig"]["AWSKMSKeyARN"].as_str())
        .filter(|arn| !arn.is_empty())
    {
        Some(arn) => DeliveredEncryption::SseKms {
            key_id: Some(arn.to_owned()),
        },
        None => DeliveredEncryption::AsDelivered,
    }
}

/// Firehose concatenates the raw record bytes, then compresses the whole
/// payload as one stream. Producers supply any record delimiters unless
/// `AppendDelimiterToRecord` adds `\n` after each record (before compression).
pub(crate) fn encode_records<T: AsRef<[u8]>>(
    records: &[T],
    config: &OutputConfig,
) -> std::io::Result<Bytes> {
    let delimiter: &[u8] = if config.append_delimiter { b"\n" } else { b"" };
    match config.compression {
        Compression::Uncompressed => {
            let mut payload = Vec::with_capacity(
                records
                    .iter()
                    .map(|record| record.as_ref().len() + delimiter.len())
                    .sum(),
            );
            for record in records {
                payload.extend_from_slice(record.as_ref());
                payload.extend_from_slice(delimiter);
            }
            Ok(Bytes::from(payload))
        }
        Compression::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            for record in records {
                encoder.write_all(record.as_ref())?;
                encoder.write_all(delimiter)?;
            }
            Ok(Bytes::from(encoder.finish()?))
        }
        Compression::Snappy => {
            // Xerial's header and big-endian lengths wrap raw Snappy blocks;
            // Snappy's separate framed-stream format is not compatible here.
            const BLOCK_SIZE: usize = 32768;
            let mut payload = b"\x82SNAPPY\0\0\0\0\x01\0\0\0\x01".to_vec();
            let mut encoder = snap::raw::Encoder::new();
            let mut encode_block = |input: &[u8]| -> std::io::Result<()> {
                let compressed = encoder.compress_vec(input).map_err(std::io::Error::other)?;
                let length = u32::try_from(compressed.len()).map_err(std::io::Error::other)?;
                payload.extend_from_slice(&length.to_be_bytes());
                payload.extend_from_slice(&compressed);
                Ok(())
            };
            let mut block = Vec::with_capacity(BLOCK_SIZE);
            // The delimiter is payload like any record byte, so it shares the
            // fixed-size block chunking rather than starting a new block.
            for part in records
                .iter()
                .flat_map(|record| [record.as_ref(), delimiter])
            {
                let mut remaining = part;
                while !remaining.is_empty() {
                    let length = (BLOCK_SIZE - block.len()).min(remaining.len());
                    block.extend_from_slice(&remaining[..length]);
                    remaining = &remaining[length..];
                    if block.len() == BLOCK_SIZE {
                        encode_block(&block)?;
                        block.clear();
                    }
                }
            }
            if !block.is_empty() {
                encode_block(&block)?;
            }
            Ok(Bytes::from(payload))
        }
    }
}

/// S3 keys are logical names, not filesystem paths. Do not normalize the prefix
/// or insert a separator between it and the default UTC time hierarchy.
pub(crate) fn object_key(
    stream: &str,
    version: &str,
    config: &OutputConfig,
    oldest_arrival: DateTime<Utc>,
    delivery_time: DateTime<Utc>,
    uuid: Uuid,
) -> String {
    format!(
        "{}{}/{}",
        config.prefix,
        oldest_arrival.format("%Y/%m/%d/%H"),
        object_name(stream, version, config, delivery_time, uuid),
    )
}

/// The file name under whatever directory a batch's prefix evaluates to.
pub(crate) fn object_name(
    stream: &str,
    version: &str,
    config: &OutputConfig,
    delivery_time: DateTime<Utc>,
    uuid: Uuid,
) -> String {
    format!(
        "{stream}-{version}-{}-{uuid}{}",
        delivery_time.format("%Y-%m-%d-%H-%M-%S"),
        config
            .file_extension
            .as_deref()
            .filter(|extension| !extension.is_empty())
            .unwrap_or_else(|| config.compression.extension()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use std::io::Read;
    use uuid::Uuid;

    #[test]
    fn delivery_encryption_maps_kms_config_to_sse_kms() {
        let arn = "arn:aws:kms:us-east-1:123456789012:key/stream-key";
        assert_eq!(
            delivery_encryption(Some(&serde_json::json!({
                "KMSEncryptionConfig": {"AWSKMSKeyARN": arn}
            }))),
            DeliveredEncryption::SseKms {
                key_id: Some(arn.to_owned())
            }
        );
        for config in [
            None,
            Some(serde_json::json!({"NoEncryptionConfig": "NoEncryption"})),
            Some(serde_json::json!({})),
        ] {
            assert_eq!(
                delivery_encryption(config.as_ref()),
                DeliveredEncryption::AsDelivered
            );
        }
    }

    fn time(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn owned_bytes_records_encode_without_changing_payload() {
        let records = [
            Bytes::from_static(b"abc"),
            Bytes::from_static(b"def\n\x00\xff"),
        ];
        for compression in [
            Compression::Uncompressed,
            Compression::Gzip,
            Compression::Snappy,
        ] {
            for (append_delimiter, expected) in [
                (false, &b"abcdef\n\x00\xff"[..]),
                (true, &b"abc\ndef\n\x00\xff\n"[..]),
            ] {
                let encoded =
                    encode_records(&records, &config(compression, append_delimiter)).unwrap();
                let decoded = match compression {
                    Compression::Uncompressed => encoded.to_vec(),
                    Compression::Gzip => gunzip(&encoded),
                    Compression::Snappy => crate::test_xerial_decoder::decode(&encoded)
                        .unwrap()
                        .concat(),
                };
                assert_eq!(decoded, expected);
            }
        }
    }

    #[test]
    fn snappy_block_windows_span_records_without_changing_bytes() {
        use crate::test_xerial_decoder::{
            binary_input, decode, incompressible_input, multiblock_input,
        };

        for input in [
            binary_input(),
            multiblock_input(),
            vec![b'x'; 70000],
            incompressible_input(70000),
        ] {
            let encoded = encode_records(
                &[input[..17].to_vec(), input[17..].to_vec()],
                &config(Compression::Snappy, false),
            )
            .unwrap();
            let blocks = decode(&encoded).unwrap();
            assert_eq!(
                blocks.iter().map(Vec::len).collect::<Vec<_>>(),
                input.chunks(32768).map(<[u8]>::len).collect::<Vec<_>>()
            );
            assert_eq!(blocks.concat(), input);
        }
    }

    #[test]
    fn snappy_block_boundaries_and_empty_stream_match_xerial() {
        use crate::test_xerial_decoder::{decode, incompressible_input};

        for (size, expected) in [
            (32767, vec![32767]),
            (32768, vec![32768]),
            (32769, vec![32768, 1]),
            (65536, vec![32768, 32768]),
            (65537, vec![32768, 32768, 1]),
        ] {
            let input = incompressible_input(size);
            let encoded = encode_records(
                std::slice::from_ref(&input),
                &config(Compression::Snappy, false),
            )
            .unwrap();
            let blocks = decode(&encoded).unwrap();
            assert_eq!(blocks.iter().map(Vec::len).collect::<Vec<_>>(), expected);
            assert_eq!(blocks.concat(), input);
        }
        assert_eq!(
            encode_records(&[vec![], vec![]], &config(Compression::Snappy, false))
                .unwrap()
                .as_ref(),
            b"\x82SNAPPY\0\0\0\0\x01\0\0\0\x01"
        );
    }

    #[test]
    fn snappy_extensions_replace_the_default() {
        let now = time("2026-10-12T09:00:00Z");
        let uuid = Uuid::parse_str("12345678-1234-4234-8234-123456789abc").unwrap();
        for (extension, suffix) in [
            (None, ".snappy"),
            (Some(""), ".snappy"),
            (Some(".custom.log"), ".custom.log"),
        ] {
            let config = OutputConfig {
                prefix: String::new(),
                file_extension: extension.map(str::to_owned),
                compression: Compression::Snappy,
                append_delimiter: false,
            };
            assert_eq!(object_key("events", "1", &config, now, now, uuid), format!(
                "2026/10/12/09/events-1-2026-10-12-09-00-00-12345678-1234-4234-8234-123456789abc{suffix}"));
        }
    }

    #[test]
    fn xerial_decoder_accepts_independent_java_fixtures() {
        use crate::test_xerial_decoder::{binary_input, decode, multiblock_input};

        let binary = decode(include_bytes!("../tests/fixtures/xerial_binary.snappy")).unwrap();
        assert_eq!(binary.iter().map(Vec::len).collect::<Vec<_>>(), [1024]);
        assert_eq!(binary.concat(), binary_input());
        let multiblock =
            decode(include_bytes!("../tests/fixtures/xerial_multiblock.snappy")).unwrap();
        assert_eq!(
            multiblock.iter().map(Vec::len).collect::<Vec<_>>(),
            [32768, 32768, 4464]
        );
        assert_eq!(multiblock.concat(), multiblock_input());
    }

    #[test]
    fn xerial_decoder_rejects_truncation_bad_lengths_and_large_blocks() {
        use crate::test_xerial_decoder::decode;

        let fixture = include_bytes!("../tests/fixtures/xerial_binary.snappy");
        assert!(decode(&fixture[..fixture.len() - 1]).is_err());
        let mut bad_header = fixture.to_vec();
        bad_header[0] = 0;
        assert!(decode(&bad_header).is_err());
        for tail in [
            vec![0, 0],
            vec![0, 0, 0, 0],
            vec![255, 255, 255, 255],
            vec![0, 0, 0, 3, 128, 128, 4],
        ] {
            let mut invalid = fixture[..16].to_vec();
            invalid.extend(tail);
            assert!(decode(&invalid).is_err());
        }
        // A well-formed raw block is rejected only for expanding past the
        // default 32 KiB block input; one byte less decodes.
        for (size, accepted) in [(32768, true), (32769, false)] {
            let block = snap::raw::Encoder::new()
                .compress_vec(&vec![b'z'; size])
                .unwrap();
            let mut stream = fixture[..16].to_vec();
            stream.extend_from_slice(&u32::try_from(block.len()).unwrap().to_be_bytes());
            stream.extend_from_slice(&block);
            assert_eq!(decode(&stream).is_ok(), accepted, "{size}-byte block");
        }
    }

    fn config(compression: Compression, append_delimiter: bool) -> OutputConfig {
        OutputConfig {
            prefix: String::new(),
            file_extension: None,
            compression,
            append_delimiter,
        }
    }

    #[test]
    fn only_implemented_compression_formats_parse() {
        assert_eq!(
            Compression::from_format("UNCOMPRESSED"),
            Some(Compression::Uncompressed)
        );
        assert_eq!(Compression::from_format("GZIP"), Some(Compression::Gzip));
        assert_eq!(
            Compression::from_format("Snappy"),
            Some(Compression::Snappy)
        );
        for unsupported in ["ZIP", "HADOOP_SNAPPY", "SNAPPY", "gzip", ""] {
            assert_eq!(Compression::from_format(unsupported), None, "{unsupported}");
        }
    }

    /// Decode exactly one GZIP member: a second member or trailing bytes after
    /// the first member's trailer fail the assertion.
    fn gunzip(encoded: &[u8]) -> Vec<u8> {
        assert!(encoded.starts_with(&[0x1f, 0x8b, 8]));
        let mut decoder = flate2::bufread::GzDecoder::new(encoded);
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();
        assert!(decoder.into_inner().is_empty(), "expected one GZIP member");
        decoded
    }

    #[test]
    fn gzip_encodes_a_single_stream_of_verbatim_binary_records() {
        let binary: Vec<u8> = (0..70_000).map(|i| ((i * 31 + 7) % 256) as u8).collect();
        let encoded = encode_records(
            &[b"abc".to_vec(), b"def\n".to_vec(), binary.clone()],
            &config(Compression::Gzip, false),
        )
        .unwrap();
        let decoded = gunzip(&encoded);
        assert_eq!(&decoded[..7], b"abcdef\n");
        assert_eq!(&decoded[7..], binary);
    }

    #[test]
    fn gzip_extensions_replace_the_default() {
        let now = time("2026-10-12T09:00:00Z");
        let uuid = Uuid::parse_str("12345678-1234-4234-8234-123456789abc").unwrap();
        for (extension, suffix) in [
            (None, ".gz"),
            (Some(""), ".gz"),
            (Some(".custom.log"), ".custom.log"),
        ] {
            let config = OutputConfig {
                prefix: String::new(),
                file_extension: extension.map(str::to_owned),
                compression: Compression::Gzip,
                append_delimiter: false,
            };
            assert_eq!(object_key("events", "1", &config, now, now, uuid), format!(
                "2026/10/12/09/events-1-2026-10-12-09-00-00-12345678-1234-4234-8234-123456789abc{suffix}"));
        }
    }

    #[test]
    fn raw_bytes_are_verbatim() {
        assert_eq!(
            encode_records(
                &[b"abc".to_vec(), b"def\n".to_vec(), vec![0, 255]],
                &config(Compression::Uncompressed, false)
            )
            .unwrap()
            .as_ref(),
            b"abcdef\n\x00\xff"
        );
        assert!(
            encode_records(&[vec![], vec![]], &config(Compression::Uncompressed, false))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn append_delimiter_terminates_every_record() {
        let records = [b"a".to_vec(), b"b\n".to_vec(), vec![0, 255]];
        let expected = b"a\nb\n\n\x00\xff\n";
        let plain = config(Compression::Uncompressed, true);
        assert_eq!(encode_records(&records, &plain).unwrap().as_ref(), expected);
        assert_eq!(encode_records(&[vec![]], &plain).unwrap().as_ref(), b"\n");
        assert!(encode_records::<Vec<u8>>(&[], &plain).unwrap().is_empty());
        // The delimiter is part of the payload that GZIP compresses.
        let gzip = config(Compression::Gzip, true);
        assert_eq!(gunzip(&encode_records(&records, &gzip).unwrap()), expected);
        // Snappy chunks delimiters with record bytes, so a delimiter can close
        // one block and blocks stay exactly 32768 decoded bytes.
        let snappy = config(Compression::Snappy, true);
        assert_eq!(
            crate::test_xerial_decoder::decode(&encode_records(&records, &snappy).unwrap())
                .unwrap()
                .concat(),
            expected
        );
        let blocks = crate::test_xerial_decoder::decode(
            &encode_records(&[vec![b'x'; 32767], vec![b'y'; 5]], &snappy).unwrap(),
        )
        .unwrap();
        assert_eq!(blocks.iter().map(Vec::len).collect::<Vec<_>>(), [32768, 6]);
        assert_eq!(blocks[0][32767], b'\n');
        assert_eq!(blocks[1], b"yyyyy\n");
    }

    #[test]
    fn append_delimiter_requires_enabled_processor() {
        use serde_json::json;

        for (processing, expected) in [
            (
                json!({"Enabled": true, "Processors": [{"Type": "AppendDelimiterToRecord"}]}),
                true,
            ),
            (
                json!({"Enabled": true, "Processors": [{"Type": "Lambda"}, {"Type": "AppendDelimiterToRecord", "Parameters": [{"ParameterName": "Delimiter", "ParameterValue": "\\n"}]}]}),
                true,
            ),
            (
                json!({"Enabled": false, "Processors": [{"Type": "AppendDelimiterToRecord"}]}),
                false,
            ),
            (
                json!({"Processors": [{"Type": "AppendDelimiterToRecord"}]}),
                false,
            ),
            (
                json!({"Enabled": "true", "Processors": [{"Type": "AppendDelimiterToRecord"}]}),
                false,
            ),
            (
                json!({"Enabled": true, "Processors": [{"Type": "Lambda"}]}),
                false,
            ),
            (
                json!({"Enabled": true, "Processors": {"Type": "AppendDelimiterToRecord"}}),
                false,
            ),
            (json!({"Enabled": true}), false),
        ] {
            assert_eq!(
                append_delimiter_enabled(Some(&processing)),
                expected,
                "{processing}"
            );
        }
        assert!(!append_delimiter_enabled(None));
    }

    #[test]
    fn static_prefix_and_suffix_use_distinct_utc_times() {
        let config = OutputConfig {
            prefix: "events/".to_owned(),
            file_extension: None,
            compression: Compression::Uncompressed,
            append_delimiter: false,
        };
        let key = object_key(
            "events",
            "2",
            &config,
            time("2026-10-12T09:59:59Z"),
            time("2026-10-12T10:00:02Z"),
            Uuid::parse_str("12345678-1234-4234-8234-123456789abc").unwrap(),
        );
        assert_eq!(key,
            "events/2026/10/12/09/events-2-2026-10-12-10-00-02-12345678-1234-4234-8234-123456789abc");
    }

    #[test]
    fn prefixes_and_extensions_are_appended_verbatim() {
        for (prefix, extension, expected) in [
            ("", None, "2026/10/12/09/events-1-2026-10-12-09-00-00-12345678-1234-4234-8234-123456789abc"),
            ("events", Some(""), "events2026/10/12/09/events-1-2026-10-12-09-00-00-12345678-1234-4234-8234-123456789abc"),
            ("events/", Some(".custom.log"), "events/2026/10/12/09/events-1-2026-10-12-09-00-00-12345678-1234-4234-8234-123456789abc.custom.log"),
        ] {
            let config = OutputConfig { prefix: prefix.to_owned(), file_extension: extension.map(str::to_owned), compression: Compression::Uncompressed, append_delimiter: false };
            let now = time("2026-10-12T09:00:00Z");
            assert_eq!(object_key("events", "1", &config, now, now,
                Uuid::parse_str("12345678-1234-4234-8234-123456789abc").unwrap()), expected);
        }
    }
}
