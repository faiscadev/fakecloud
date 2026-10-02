//! Decompression of object bodies for services that read S3 data as text
//! (S3 Select, Athena, imports).
//!
//! Real AWS readers decode compressed objects transparently: S3 Select by the
//! request's `InputSerialization.CompressionType`, Athena by the object's file
//! extension (`.gz`, `.bz2`, `.zst`, `.deflate`). Reading the raw compressed
//! bytes as UTF-8 silently yields zero rows, so every reader goes through here.

use std::io::{self, Read};

/// Compression codecs AWS readers recognize on S3 text data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    None,
    Gzip,
    Bzip2,
    Zstd,
    Deflate,
}

impl Codec {
    /// Parse a request-level compression name (`NONE`, `GZIP`, `BZIP2`,
    /// `ZSTD`), case-insensitively. Unknown names return `None` so callers
    /// can reject them with the service's own error.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "NONE" => Some(Self::None),
            "GZIP" => Some(Self::Gzip),
            "BZIP2" => Some(Self::Bzip2),
            "ZSTD" => Some(Self::Zstd),
            _ => None,
        }
    }

    /// Infer the codec the way Athena does for text-format tables: from the
    /// object key's extension, falling back to the object's
    /// `Content-Encoding` when the key has no compression extension.
    pub fn from_key(key: &str, content_encoding: Option<&str>) -> Self {
        let lower = key.to_ascii_lowercase();
        if lower.ends_with(".gz") || lower.ends_with(".gzip") {
            return Self::Gzip;
        }
        if lower.ends_with(".bz2") {
            return Self::Bzip2;
        }
        if lower.ends_with(".zst") || lower.ends_with(".zstd") {
            return Self::Zstd;
        }
        if lower.ends_with(".deflate") {
            return Self::Deflate;
        }
        match content_encoding
            .map(|e| e.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("gzip") | Some("x-gzip") => Self::Gzip,
            Some("zstd") => Self::Zstd,
            Some("deflate") => Self::Deflate,
            Some("bzip2") => Self::Bzip2,
            _ => Self::None,
        }
    }
}

/// Decode `data` with `codec`. Multi-member gzip streams (concatenated
/// `.gz` files, which CloudWatch Logs and Firehose both produce) are read to
/// the end.
pub fn decompress(codec: Codec, data: &[u8]) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    match codec {
        Codec::None => out.extend_from_slice(data),
        Codec::Gzip => {
            flate2::read::MultiGzDecoder::new(data).read_to_end(&mut out)?;
        }
        Codec::Bzip2 => {
            bzip2::read::MultiBzDecoder::new(data).read_to_end(&mut out)?;
        }
        Codec::Zstd => {
            zstd::stream::read::Decoder::new(data)?.read_to_end(&mut out)?;
        }
        Codec::Deflate => {
            // Hadoop's `.deflate` codec writes zlib-wrapped streams.
            flate2::read::ZlibDecoder::new(data).read_to_end(&mut out)?;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn infers_codec_from_extension_and_encoding() {
        assert_eq!(Codec::from_key("a/b.csv.gz", None), Codec::Gzip);
        assert_eq!(Codec::from_key("a/b.CSV.BZ2", None), Codec::Bzip2);
        assert_eq!(Codec::from_key("a/b.csv.zst", None), Codec::Zstd);
        assert_eq!(Codec::from_key("a/b.deflate", None), Codec::Deflate);
        assert_eq!(Codec::from_key("a/b.csv", Some("gzip")), Codec::Gzip);
        assert_eq!(Codec::from_key("a/b.csv", None), Codec::None);
        assert_eq!(Codec::from_name("gzip"), Some(Codec::Gzip));
        assert_eq!(Codec::from_name("LZO"), None);
    }

    #[test]
    fn round_trips_every_codec() {
        let text = b"a,b\n1,2\n";
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(text).unwrap();
        let gz = gz.finish().unwrap();
        assert_eq!(decompress(Codec::Gzip, &gz).unwrap(), text);

        // Two concatenated gzip members decode as one stream.
        let mut two = gz.clone();
        two.extend_from_slice(&gz);
        assert_eq!(
            decompress(Codec::Gzip, &two).unwrap(),
            b"a,b\n1,2\na,b\n1,2\n"
        );

        let mut bz = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
        bz.write_all(text).unwrap();
        assert_eq!(
            decompress(Codec::Bzip2, &bz.finish().unwrap()).unwrap(),
            text
        );

        let zs = zstd::stream::encode_all(&text[..], 0).unwrap();
        assert_eq!(decompress(Codec::Zstd, &zs).unwrap(), text);

        let mut zl = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        zl.write_all(text).unwrap();
        assert_eq!(
            decompress(Codec::Deflate, &zl.finish().unwrap()).unwrap(),
            text
        );

        assert!(decompress(Codec::Gzip, b"not gzip").is_err());
    }
}
