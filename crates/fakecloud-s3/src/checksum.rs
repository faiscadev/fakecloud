//! S3 flexible (additional) checksums.
//!
//! One place for every algorithm S3 accepts in `x-amz-checksum-*` /
//! `x-amz-sdk-checksum-algorithm` / `x-amz-trailer`, the digests behind them,
//! and the request-side validation every upload path shares.
//!
//! Wire format: every value is the base64 encoding of the digest in network
//! (big-endian) byte order, as the S3 user guide specifies for trailing
//! checksums ("a base64 encoding of the big-endian checksum value") and as the
//! AWS SDKs encode headers. For the CRCs that is the big-endian integer; for
//! SHA-1/SHA-256/SHA-512/MD5 the digest bytes as defined; for the xxHash family
//! the canonical (big-endian) representation of the 64- or 128-bit result.
//!
//! xxHash parameters follow the AWS Common Runtime, which the SDKs use to
//! compute these checksums: `aws-c-s3` (`source/s3_checksums.c`) creates
//! `XXHASH64` / `XXHASH3` (XXH3-64) / `XXHASH128` (XXH3-128) hashers with seed
//! `0` and the default secret, and `aws-checksums` (`aws/checksums/xxhash.h`)
//! writes the finalized hash "in big-endian, i.e. network order".

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use fakecloud_core::service::AwsServiceError;
use http::{HeaderMap, StatusCode};

/// The S3 `ChecksumAlgorithm` enum (every member of the Smithy shape).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Crc64Nvme,
    Sha1,
    Sha256,
    Sha512,
    Md5,
    XxHash64,
    XxHash3,
    XxHash128,
}

impl ChecksumAlgorithm {
    pub const ALL: [ChecksumAlgorithm; 10] = [
        ChecksumAlgorithm::Crc32,
        ChecksumAlgorithm::Crc32c,
        ChecksumAlgorithm::Crc64Nvme,
        ChecksumAlgorithm::Sha1,
        ChecksumAlgorithm::Sha256,
        ChecksumAlgorithm::Sha512,
        ChecksumAlgorithm::Md5,
        ChecksumAlgorithm::XxHash64,
        ChecksumAlgorithm::XxHash3,
        ChecksumAlgorithm::XxHash128,
    ];

    /// The enum value on the wire (`CRC32`, `XXHASH128`, ...).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Crc64Nvme => "CRC64NVME",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Sha512 => "SHA512",
            Self::Md5 => "MD5",
            Self::XxHash64 => "XXHASH64",
            Self::XxHash3 => "XXHASH3",
            Self::XxHash128 => "XXHASH128",
        }
    }

    /// Parse an algorithm name, case-insensitively. State persisted by older
    /// builds can hold the lowercase form a client sent, so every read path
    /// goes through here.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        Self::ALL
            .into_iter()
            .find(|a| a.as_str().eq_ignore_ascii_case(s))
    }

    /// The value header (`x-amz-checksum-crc32`), also the trailer name.
    pub fn header_name(self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Crc64Nvme => "x-amz-checksum-crc64nvme",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
            Self::Sha512 => "x-amz-checksum-sha512",
            Self::Md5 => "x-amz-checksum-md5",
            Self::XxHash64 => "x-amz-checksum-xxhash64",
            Self::XxHash3 => "x-amz-checksum-xxhash3",
            Self::XxHash128 => "x-amz-checksum-xxhash128",
        }
    }

    /// The lowercase name S3 uses in its error messages (`crc32`).
    pub fn lowercase_name(self) -> &'static str {
        &self.header_name()["x-amz-checksum-".len()..]
    }

    /// The XML element carrying a value of this algorithm (`ChecksumCRC32`).
    pub fn xml_element(self) -> &'static str {
        match self {
            Self::Crc32 => "ChecksumCRC32",
            Self::Crc32c => "ChecksumCRC32C",
            Self::Crc64Nvme => "ChecksumCRC64NVME",
            Self::Sha1 => "ChecksumSHA1",
            Self::Sha256 => "ChecksumSHA256",
            Self::Sha512 => "ChecksumSHA512",
            Self::Md5 => "ChecksumMD5",
            Self::XxHash64 => "ChecksumXXHASH64",
            Self::XxHash3 => "ChecksumXXHASH3",
            Self::XxHash128 => "ChecksumXXHASH128",
        }
    }

    /// Digest length in bytes.
    pub fn digest_len(self) -> usize {
        match self {
            Self::Crc32 | Self::Crc32c => 4,
            Self::Crc64Nvme | Self::XxHash64 | Self::XxHash3 => 8,
            Self::Sha1 => 20,
            Self::Sha256 => 32,
            Self::Sha512 => 64,
            Self::Md5 | Self::XxHash128 => 16,
        }
    }

    /// Multipart checksum types the algorithm supports (S3 user guide,
    /// "Checking object integrity for data uploads", multipart table): only the
    /// CRCs linearize into a FULL_OBJECT checksum, and CRC64NVME is the one
    /// algorithm without a COMPOSITE form.
    pub fn supports(self, checksum_type: ChecksumType) -> bool {
        match checksum_type {
            ChecksumType::FullObject => {
                matches!(self, Self::Crc32 | Self::Crc32c | Self::Crc64Nvme)
            }
            ChecksumType::Composite => self != Self::Crc64Nvme,
        }
    }

    /// The multipart checksum type used when CreateMultipartUpload names an
    /// algorithm but no type: FULL_OBJECT for CRC64NVME (its only type),
    /// COMPOSITE for every other algorithm.
    pub fn default_multipart_type(self) -> ChecksumType {
        if self == Self::Crc64Nvme {
            ChecksumType::FullObject
        } else {
            ChecksumType::Composite
        }
    }
}

impl std::fmt::Display for ChecksumAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The algorithm S3 applies when an upload names none: since December 2024
/// every new object gets a CRC-64/NVME full-object checksum (S3 user guide:
/// "If you don't specify a checksum algorithm and the SDK also doesn't
/// calculate a checksum for you, then S3 automatically chooses the CRC-64/NVME
/// (`CRC64NVME`) checksum algorithm").
pub const DEFAULT_ALGORITHM: ChecksumAlgorithm = ChecksumAlgorithm::Crc64Nvme;

/// The S3 `ChecksumType` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChecksumType {
    Composite,
    FullObject,
}

impl ChecksumType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Composite => "COMPOSITE",
            Self::FullObject => "FULL_OBJECT",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("COMPOSITE") {
            Some(Self::Composite)
        } else if s.eq_ignore_ascii_case("FULL_OBJECT") {
            Some(Self::FullObject)
        } else {
            None
        }
    }
}

impl std::fmt::Display for ChecksumType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An incremental hasher for one algorithm.
pub struct Hasher(HasherState);

enum HasherState {
    Crc32(crc32fast::Hasher),
    Crc32c(u32),
    Crc64Nvme(crc64fast_nvme::Digest),
    Sha1(sha1::Sha1),
    Sha256(sha2::Sha256),
    Sha512(sha2::Sha512),
    Md5(md5::Md5),
    XxHash64(xxhash_rust::xxh64::Xxh64),
    // The XXH3 state carries a 256-byte buffer plus accumulators; boxed so the
    // enum stays small.
    XxHash3(Box<xxhash_rust::xxh3::Xxh3Default>),
    XxHash128(Box<xxhash_rust::xxh3::Xxh3Default>),
}

impl Hasher {
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        use sha1::Digest as _;
        Self(match algorithm {
            ChecksumAlgorithm::Crc32 => HasherState::Crc32(crc32fast::Hasher::new()),
            ChecksumAlgorithm::Crc32c => HasherState::Crc32c(0),
            ChecksumAlgorithm::Crc64Nvme => HasherState::Crc64Nvme(crc64fast_nvme::Digest::new()),
            ChecksumAlgorithm::Sha1 => HasherState::Sha1(sha1::Sha1::new()),
            ChecksumAlgorithm::Sha256 => HasherState::Sha256(sha2::Sha256::new()),
            ChecksumAlgorithm::Sha512 => HasherState::Sha512(sha2::Sha512::new()),
            ChecksumAlgorithm::Md5 => HasherState::Md5(md5::Md5::new()),
            ChecksumAlgorithm::XxHash64 => HasherState::XxHash64(xxhash_rust::xxh64::Xxh64::new(0)),
            ChecksumAlgorithm::XxHash3 => {
                HasherState::XxHash3(Box::new(xxhash_rust::xxh3::Xxh3Default::new()))
            }
            ChecksumAlgorithm::XxHash128 => {
                HasherState::XxHash128(Box::new(xxhash_rust::xxh3::Xxh3Default::new()))
            }
        })
    }

    pub fn update(&mut self, data: &[u8]) {
        use sha1::Digest as _;
        match &mut self.0 {
            HasherState::Crc32(h) => h.update(data),
            HasherState::Crc32c(crc) => *crc = crc32c::crc32c_append(*crc, data),
            HasherState::Crc64Nvme(h) => h.write(data),
            HasherState::Sha1(h) => h.update(data),
            HasherState::Sha256(h) => h.update(data),
            HasherState::Sha512(h) => h.update(data),
            HasherState::Md5(h) => h.update(data),
            HasherState::XxHash64(h) => h.update(data),
            HasherState::XxHash3(h) | HasherState::XxHash128(h) => h.update(data),
        }
    }

    /// The digest bytes in network (big-endian) order.
    pub fn finalize(self) -> Vec<u8> {
        use sha1::Digest as _;
        match self.0 {
            HasherState::Crc32(h) => h.finalize().to_be_bytes().to_vec(),
            HasherState::Crc32c(crc) => crc.to_be_bytes().to_vec(),
            HasherState::Crc64Nvme(h) => h.sum64().to_be_bytes().to_vec(),
            HasherState::Sha1(h) => h.finalize().to_vec(),
            HasherState::Sha256(h) => h.finalize().to_vec(),
            HasherState::Sha512(h) => h.finalize().to_vec(),
            HasherState::Md5(h) => h.finalize().to_vec(),
            HasherState::XxHash64(h) => h.digest().to_be_bytes().to_vec(),
            HasherState::XxHash3(h) => h.digest().to_be_bytes().to_vec(),
            HasherState::XxHash128(h) => h.digest128().to_be_bytes().to_vec(),
        }
    }
}

/// The raw digest of `data`.
pub fn digest(algorithm: ChecksumAlgorithm, data: &[u8]) -> Vec<u8> {
    let mut h = Hasher::new(algorithm);
    h.update(data);
    h.finalize()
}

/// Base64-encode a raw digest, the form every checksum takes on the wire.
pub fn encode(raw: &[u8]) -> String {
    BASE64.encode(raw)
}

/// The wire (base64) checksum of `data`.
pub fn checksum(algorithm: ChecksumAlgorithm, data: &[u8]) -> String {
    encode(&digest(algorithm, data))
}

/// Split a checksum value into its base64 digest and the optional
/// `-<part count>` suffix of a multipart COMPOSITE checksum.
pub fn split_composite(value: &str) -> (&str, Option<&str>) {
    let value = value.trim();
    match value.rsplit_once('-') {
        Some((digest, count)) if !count.is_empty() && count.bytes().all(|b| b.is_ascii_digit()) => {
            (digest, Some(count))
        }
        _ => (value, None),
    }
}

/// Decode a supplied base64 checksum, requiring the algorithm's digest length.
pub fn decode(algorithm: ChecksumAlgorithm, value: &str) -> Option<Vec<u8>> {
    BASE64
        .decode(value.trim())
        .ok()
        .filter(|raw| raw.len() == algorithm.digest_len())
}

/// The multipart COMPOSITE checksum: the algorithm applied to the
/// concatenation of each part's raw digest, base64'd, with `-<part count>`
/// appended. `None` for an empty part list.
pub fn composite(algorithm: ChecksumAlgorithm, part_digests: &[Vec<u8>]) -> Option<String> {
    if part_digests.is_empty() {
        return None;
    }
    let mut h = Hasher::new(algorithm);
    for d in part_digests {
        h.update(d);
    }
    Some(format!("{}-{}", encode(&h.finalize()), part_digests.len()))
}

/// The CRC of `A || B` from `crc(A)`, `crc(B)` and `len(B)`, for the CRCs S3
/// accepts as FULL_OBJECT multipart checksums (`None` for other algorithms).
/// The same zlib-style construction `aws-checksums` uses for its
/// `aws_checksums_crc*_combine`: shift `crc(A)` by `len(B)` zero bytes
/// (multiplication by x^(8*len(B)) modulo the reflected polynomial), then xor
/// in `crc(B)`. The all-ones init/xorout conditioning cancels out.
pub fn crc_combine(
    algorithm: ChecksumAlgorithm,
    crc_a: u64,
    crc_b: u64,
    len_b: u64,
) -> Option<u64> {
    let (poly, width) = match algorithm {
        ChecksumAlgorithm::Crc32 => (0xEDB8_8320u64, 32),
        ChecksumAlgorithm::Crc32c => (0x82F6_3B78u64, 32),
        ChecksumAlgorithm::Crc64Nvme => (0x9A6C_9329_AC4B_C9B5u64, 64),
        _ => return None,
    };
    let top: u64 = 1 << (width - 1);
    // a * b modulo the polynomial, both in reflected bit order (x^0 at the top
    // bit). `a` is never zero here.
    let mul = |a: u64, mut b: u64| -> u64 {
        let mut m = top;
        let mut p = 0u64;
        loop {
            if a & m != 0 {
                p ^= b;
                if a & (m - 1) == 0 {
                    break;
                }
            }
            m >>= 1;
            b = if b & 1 != 0 { (b >> 1) ^ poly } else { b >> 1 };
        }
        p
    };
    // x^(2^k) for k = 0.., starting from x^1.
    let mut x2n = [0u64; 64];
    let mut p = top >> 1;
    for slot in x2n.iter_mut() {
        *slot = p;
        p = mul(p, p);
    }
    // x^(8 * len_b): bit k of the exponent selects x^(2^k); the 8 is k += 3.
    let mut shift = top; // x^0
    let mut n = len_b;
    let mut k = 3usize;
    while n != 0 {
        if n & 1 != 0 {
            shift = mul(x2n[k % 64], shift);
        }
        n >>= 1;
        k += 1;
    }
    Some(mul(shift, crc_a) ^ crc_b)
}

/// Combine per-part CRC digests (raw big-endian bytes, with each part's
/// length) into the CRC of the whole object. `None` when the algorithm has no
/// combine or a digest has the wrong width.
pub fn combine_crc_digests(
    algorithm: ChecksumAlgorithm,
    parts: &[(&[u8], u64)],
) -> Option<Vec<u8>> {
    let width = algorithm.digest_len();
    let read = |raw: &[u8]| -> Option<u64> {
        (raw.len() == width).then(|| raw.iter().fold(0u64, |acc, b| (acc << 8) | u64::from(*b)))
    };
    let mut acc: Option<u64> = None;
    for (raw, len) in parts {
        let crc = read(raw)?;
        acc = Some(match acc {
            None => crc,
            Some(a) => crc_combine(algorithm, a, crc, *len)?,
        });
    }
    let value = match acc {
        Some(v) => v,
        // The CRC of nothing.
        None => return Some(digest(algorithm, b"")),
    };
    Some(value.to_be_bytes()[8 - width..].to_vec())
}

/// Hashes one payload with several algorithms in a single pass.
pub struct MultiHasher {
    hashers: Vec<(ChecksumAlgorithm, Hasher)>,
}

impl MultiHasher {
    pub fn new(algorithms: impl IntoIterator<Item = ChecksumAlgorithm>) -> Self {
        let mut hashers: Vec<(ChecksumAlgorithm, Hasher)> = Vec::new();
        for a in algorithms {
            if !hashers.iter().any(|(b, _)| *b == a) {
                hashers.push((a, Hasher::new(a)));
            }
        }
        Self { hashers }
    }

    pub fn update(&mut self, data: &[u8]) {
        for (_, h) in &mut self.hashers {
            h.update(data);
        }
    }

    pub fn finish(self) -> Digests {
        Digests(
            self.hashers
                .into_iter()
                .map(|(a, h)| (a, h.finalize()))
                .collect(),
        )
    }
}

/// Raw digests computed over one payload, keyed by algorithm.
#[derive(Debug, Default, Clone)]
pub struct Digests(Vec<(ChecksumAlgorithm, Vec<u8>)>);

impl Digests {
    /// Record a digest computed elsewhere (e.g. the MD5 / SHA-256 the spool
    /// already produced), unless one is already present.
    pub fn insert(&mut self, algorithm: ChecksumAlgorithm, raw: Vec<u8>) {
        if self.get(algorithm).is_none() {
            self.0.push((algorithm, raw));
        }
    }

    pub fn get(&self, algorithm: ChecksumAlgorithm) -> Option<&[u8]> {
        self.0
            .iter()
            .find(|(a, _)| *a == algorithm)
            .map(|(_, d)| d.as_slice())
    }

    /// The wire value for `algorithm`, if it was computed.
    pub fn encoded(&self, algorithm: ChecksumAlgorithm) -> Option<String> {
        self.get(algorithm).map(encode)
    }
}

/// A checksum value the client asserted, from a header or a trailer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuppliedChecksum {
    pub algorithm: ChecksumAlgorithm,
    pub value: String,
}

pub(crate) fn invalid_request(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequest", message)
}

/// The error for an `x-amz-sdk-checksum-algorithm` / `x-amz-checksum-algorithm`
/// value outside the `ChecksumAlgorithm` enum.
pub fn unsupported_algorithm() -> AwsServiceError {
    let valid: Vec<&str> = ChecksumAlgorithm::ALL.iter().map(|a| a.as_str()).collect();
    invalid_request(format!(
        "Checksum algorithm provided is unsupported. Please try again with any of the valid \
         types: [{}]",
        valid.join(", ")
    ))
}

/// Parse an algorithm header value, rejecting values outside the enum.
pub fn parse_algorithm_header(
    headers: &HeaderMap,
    name: &str,
) -> Result<Option<ChecksumAlgorithm>, AwsServiceError> {
    match non_empty_header(headers, name) {
        None => Ok(None),
        Some(v) => ChecksumAlgorithm::parse(v)
            .map(Some)
            .ok_or_else(unsupported_algorithm),
    }
}

/// `BadDigest` for a checksum that does not match the received payload.
pub fn bad_digest(algorithm: ChecksumAlgorithm) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "BadDigest",
        format!(
            "The {} you specified did not match the calculated checksum.",
            algorithm.as_str()
        ),
    )
}

fn multiple_checksums() -> AwsServiceError {
    invalid_request(
        "Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed.",
    )
}

fn malformed_trailer() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MalformedTrailerError",
        "The request contained trailing data that was not well-formed or did not conform to \
         our published schema.",
    )
}

/// The integrity assertions an upload request carries: the single
/// `x-amz-checksum-<algo>` value header (or the trailer named by
/// `x-amz-trailer`), the SDK's declared `x-amz-sdk-checksum-algorithm`, and
/// `Content-MD5`. Parsed (and syntactically validated) before the body is
/// read; [`ChecksumHeaders::verify`] checks them against the digests once it
/// has been.
#[derive(Debug, Clone, Default)]
pub struct ChecksumHeaders {
    pub sdk_algorithm: Option<ChecksumAlgorithm>,
    pub value: Option<SuppliedChecksum>,
    pub trailer: Option<ChecksumAlgorithm>,
    pub content_md5: Option<Vec<u8>>,
}

/// A header's trimmed value, `None` when absent or empty. An empty integrity
/// header is treated as not sent.
fn non_empty_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .map(|v| v.to_str().unwrap_or("").trim())
        .filter(|v| !v.is_empty())
}

impl ChecksumHeaders {
    /// Parse the integrity headers of a payload-carrying request: each
    /// `x-amz-checksum-<algorithm>` value must be a digest of that algorithm.
    pub fn parse(headers: &HeaderMap) -> Result<Self, AwsServiceError> {
        Self::parse_with(headers, false)
    }

    /// Parse CompleteMultipartUpload's headers, whose whole-object value may
    /// be a COMPOSITE checksum (`<digest>-<part count>`).
    pub fn parse_completion(headers: &HeaderMap) -> Result<Self, AwsServiceError> {
        Self::parse_with(headers, true)
    }

    fn parse_with(headers: &HeaderMap, composite_allowed: bool) -> Result<Self, AwsServiceError> {
        // A POST Object form names the algorithm in its `x-amz-checksum-algorithm`
        // field, which reaches the upload path as that header.
        let sdk_algorithm = match parse_algorithm_header(headers, "x-amz-sdk-checksum-algorithm")? {
            Some(a) => Some(a),
            None => parse_algorithm_header(headers, "x-amz-checksum-algorithm")?,
        };

        let mut value = None;
        for algorithm in ChecksumAlgorithm::ALL {
            let Some(text) = non_empty_header(headers, algorithm.header_name()) else {
                continue;
            };
            if value.is_some() {
                return Err(multiple_checksums());
            }
            let text = text.to_string();
            let digest = if composite_allowed {
                split_composite(&text).0
            } else {
                text.as_str()
            };
            if decode(algorithm, digest).is_none() {
                return Err(invalid_request(format!(
                    "Value for {} header is invalid.",
                    algorithm.header_name()
                )));
            }
            value = Some(SuppliedChecksum {
                algorithm,
                value: text,
            });
        }

        let mut trailer = None;
        for raw in headers.get_all("x-amz-trailer") {
            for name in raw.to_str().unwrap_or("").split(',') {
                let name = name.trim().to_ascii_lowercase();
                let Some(suffix) = name.strip_prefix("x-amz-checksum-") else {
                    continue;
                };
                let algorithm = ChecksumAlgorithm::parse(suffix).ok_or_else(|| {
                    invalid_request(
                        "The value specified in the x-amz-trailer header is not supported",
                    )
                })?;
                if trailer.is_some() || value.is_some() {
                    return Err(multiple_checksums());
                }
                trailer = Some(algorithm);
            }
        }

        let content_md5 = match non_empty_header(headers, "content-md5") {
            None => None,
            Some(v) => {
                let raw = BASE64
                    .decode(v)
                    .ok()
                    .filter(|d| d.len() == 16)
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidDigest",
                            "The Content-MD5 you specified was invalid.",
                        )
                    })?;
                Some(raw)
            }
        };

        Ok(Self {
            sdk_algorithm,
            value,
            trailer,
            content_md5,
        })
    }

    /// The algorithm the request's checksum is in: the value header's, else
    /// the trailer's, else the SDK-declared one.
    pub fn algorithm(&self) -> Option<ChecksumAlgorithm> {
        self.value
            .as_ref()
            .map(|v| v.algorithm)
            .or(self.trailer)
            .or(self.sdk_algorithm)
    }

    /// Whether the request carries any integrity assertion (Object Lock
    /// requires one on PutObject).
    pub fn has_integrity_check(&self) -> bool {
        self.content_md5.is_some()
            || self.value.is_some()
            || self.trailer.is_some()
            || self.sdk_algorithm.is_some()
    }

    /// Algorithms whose digest must be computed to verify this request and
    /// store its checksum (`store` is the algorithm the object will carry).
    pub fn algorithms_to_compute(
        &self,
        store: Option<ChecksumAlgorithm>,
    ) -> Vec<ChecksumAlgorithm> {
        let mut out: Vec<ChecksumAlgorithm> = Vec::new();
        for a in [self.algorithm(), store].into_iter().flatten() {
            if !out.contains(&a) {
                out.push(a);
            }
        }
        if self.content_md5.is_some() && !out.contains(&ChecksumAlgorithm::Md5) {
            out.push(ChecksumAlgorithm::Md5);
        }
        out
    }

    /// Verify every assertion against the digests of the received payload.
    /// `trailers` are the trailing headers read after an `aws-chunked` body.
    /// Returns the checksum the client asserted (header or trailer), if any.
    pub fn verify(
        &self,
        digests: &Digests,
        trailers: &[(String, String)],
    ) -> Result<Option<SuppliedChecksum>, AwsServiceError> {
        if let Some(expected) = &self.content_md5 {
            if digests.get(ChecksumAlgorithm::Md5) != Some(expected.as_slice()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadDigest",
                    "The Content-MD5 you specified did not match what we received.",
                ));
            }
        }

        let supplied = match (&self.value, self.trailer) {
            (Some(v), _) => Some(v.clone()),
            (None, Some(algorithm)) => {
                let (_, value) = trailers
                    .iter()
                    .find(|(name, _)| name == algorithm.header_name())
                    .ok_or_else(malformed_trailer)?;
                if decode(algorithm, value).is_none() {
                    return Err(invalid_request(format!(
                        "Value for {} trailing header is invalid.",
                        algorithm.header_name()
                    )));
                }
                Some(SuppliedChecksum {
                    algorithm,
                    value: value.clone(),
                })
            }
            (None, None) => None,
        };

        if let Some(s) = &supplied {
            let expected = decode(s.algorithm, &s.value);
            if expected.as_deref() != digests.get(s.algorithm) {
                return Err(bad_digest(s.algorithm));
            }
        }
        Ok(supplied)
    }
}

/// An upload body spooled to disk, every integrity assertion on it verified.
pub(crate) struct VerifiedSpool {
    pub spooled: fakecloud_core::service::SpooledBody,
    pub digests: Digests,
}

/// Spool a streaming upload body to disk (decoding `aws-chunked` framing),
/// computing every digest the request needs in that same single pass, then
/// verify `Content-MD5`, the `x-amz-checksum-*` value or trailing checksum,
/// and a hex `x-amz-content-sha256`. `store` is the algorithm whose checksum
/// the caller will keep; it is folded into the same pass. MD5 (the ETag) is
/// produced by the spool and not hashed twice. SHA-256 is computed only when
/// a hex `x-amz-content-sha256` or a SHA-256 checksum needs it: it is the most
/// expensive digest on the PutObject path, and `UNSIGNED-PAYLOAD` / streaming
/// uploads with a CRC checksum never read it. On any verification failure the
/// spool file is removed before returning the error.
pub(crate) async fn spool_verified(
    headers: &HeaderMap,
    stream: fakecloud_core::service::RequestBodyStream,
    dir: Option<&std::path::Path>,
    checks: &ChecksumHeaders,
    store: Option<ChecksumAlgorithm>,
) -> Result<VerifiedSpool, AwsServiceError> {
    let aws_chunked = fakecloud_core::service::is_aws_chunked(headers);
    let asserted_sha256 = asserted_content_sha256(headers, aws_chunked);
    let mut algorithms = checks.algorithms_to_compute(store);
    if asserted_sha256.is_some() && !algorithms.contains(&ChecksumAlgorithm::Sha256) {
        algorithms.push(ChecksumAlgorithm::Sha256);
    }
    algorithms.retain(|a| *a != ChecksumAlgorithm::Md5);
    let mut hasher = MultiHasher::new(algorithms);
    let spooled = fakecloud_core::service::spool_request_stream_observed(
        stream,
        dir,
        aws_chunked,
        &mut |chunk| hasher.update(chunk),
    )
    .await?;
    let mut digests = hasher.finish();
    digests.insert(ChecksumAlgorithm::Md5, spooled.md5.to_vec());
    let verdict = check_content_sha256(asserted_sha256.as_ref(), &digests)
        .and_then(|()| checks.verify(&digests, &spooled.trailers));
    if let Err(e) = verdict {
        let _ = tokio::fs::remove_file(&spooled.path).await;
        return Err(e);
    }
    Ok(VerifiedSpool { spooled, digests })
}

/// The payload SHA-256 a request's `x-amz-content-sha256` header asserts. A
/// SigV4-signed, non-chunked upload carries the payload's real SHA-256
/// (lowercase hex) there, and S3 rejects a divergence with
/// `XAmzContentSHA256Mismatch`. The markers that are not a body hash
/// (`UNSIGNED-PAYLOAD`, `STREAMING-…`) assert nothing, nor does the header on
/// an aws-chunked upload, where it covers the chunk framing rather than the
/// decoded payload.
pub(crate) fn asserted_content_sha256(headers: &HeaderMap, aws_chunked: bool) -> Option<[u8; 32]> {
    if aws_chunked {
        return None;
    }
    let supplied = headers.get("x-amz-content-sha256")?.to_str().ok()?.trim();
    // Only a 64-character hex value is an assertion about the payload.
    if supplied.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(supplied.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// Compare an asserted payload SHA-256 (see [`asserted_content_sha256`])
/// against the computed digest, which the caller computes whenever there is
/// an assertion.
fn check_content_sha256(
    asserted: Option<&[u8; 32]>,
    digests: &Digests,
) -> Result<(), AwsServiceError> {
    let Some(asserted) = asserted else {
        return Ok(());
    };
    if digests.get(ChecksumAlgorithm::Sha256) != Some(asserted.as_slice()) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "XAmzContentSHA256Mismatch",
            "The provided 'x-amz-content-sha256' header does not match what was computed.",
        ));
    }
    Ok(())
}

/// The buffered twin of [`spool_verified`], for a body that reaches the
/// handler already in memory: a small XML body, or a streaming-payload
/// operation's such as `PutObjectAnnotation`. The body is already unframed
/// ([`deframe_buffered`]); a trailing checksum is a header by now. Returns
/// the body's digests, `store`'s among them.
pub(crate) fn verify_buffered(
    headers: &HeaderMap,
    body: &[u8],
    checks: &ChecksumHeaders,
    store: Option<ChecksumAlgorithm>,
) -> Result<Digests, AwsServiceError> {
    let asserted_sha256 = asserted_content_sha256(headers, false);
    let mut algorithms = checks.algorithms_to_compute(store);
    if asserted_sha256.is_some() && !algorithms.contains(&ChecksumAlgorithm::Sha256) {
        algorithms.push(ChecksumAlgorithm::Sha256);
    }
    let mut hasher = MultiHasher::new(algorithms);
    hasher.update(body);
    let digests = hasher.finish();
    check_content_sha256(asserted_sha256.as_ref(), &digests)?;
    checks.verify(&digests, &[])?;
    Ok(digests)
}

/// Remove the `aws-chunked` framing of a buffered request body, so the
/// payload is what every reader of the request sees: pre-authorization
/// validation and IAM derivation (the tag set of PutObjectTagging) as much as
/// the handler. Done once, by [`crate::S3Service`]'s `prepare_request`; a
/// request no longer framed is left alone, so running it again is a no-op.
///
/// The request becomes the unframed request it is equivalent to:
/// `aws-chunked` leaves `Content-Encoding`, and the trailing checksum the
/// `x-amz-trailer` header announces becomes that checksum's header, checked
/// later exactly as a header value is. Headers that do not parse are left as
/// they are, for the handler to refuse as before, and so is an announced
/// trailer that never came (the handler answers it as a missing trailer).
///
/// All or nothing: a body whose framing is broken, or whose trailing value
/// is no digest of its algorithm (refused with the trailing-header message),
/// leaves the request exactly as it was, still framed. Its error is S3's to
/// return only once the bucket's existence and the caller's authorization
/// have been judged (recorded: NoSuchBucket and AccessDenied come first), so
/// the handler unframes again at that point and gets the same error.
pub(crate) fn deframe_buffered(
    headers: &mut HeaderMap,
    body: &mut bytes::Bytes,
) -> Result<(), AwsServiceError> {
    if !fakecloud_core::service::is_aws_chunked(headers) {
        return Ok(());
    }
    let decoded = fakecloud_core::service::decode_aws_chunked(body).map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidChunkSizeError",
            "Malformed aws-chunked request body",
        )
    })?;
    let mut unframed = headers.clone();
    let encoding = fakecloud_core::service::strip_aws_chunked_encoding(
        unframed
            .get("content-encoding")
            .and_then(|v| v.to_str().ok()),
    );
    match encoding.and_then(|e| http::HeaderValue::from_str(&e).ok()) {
        Some(value) => unframed.insert("content-encoding", value),
        None => unframed.remove("content-encoding"),
    };
    // A `STREAMING-...` payload hash names the framing, whose chunk
    // signatures were the payload's assertion; the unframed body asserts no
    // hash of its own.
    unframed.insert(
        "x-amz-content-sha256",
        http::HeaderValue::from_static("UNSIGNED-PAYLOAD"),
    );
    let trailer = match ChecksumHeaders::parse(&unframed) {
        Ok(ChecksumHeaders {
            trailer: Some(algorithm),
            ..
        }) => decoded
            .trailers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(algorithm.header_name()))
            .map(|(_, value)| (algorithm, value)),
        _ => None,
    };
    if let Some((algorithm, value)) = trailer {
        let invalid = || {
            invalid_request(format!(
                "Value for {} trailing header is invalid.",
                algorithm.header_name()
            ))
        };
        if decode(algorithm, value).is_none() {
            return Err(invalid());
        }
        let value = http::HeaderValue::from_str(value).map_err(|_| invalid())?;
        unframed.remove("x-amz-trailer");
        unframed.insert(algorithm.header_name(), value);
    }
    *headers = unframed;
    *body = decoded.payload.into();
    Ok(())
}

/// Whether a request's body is still `aws-chunked` framed, buffered: one
/// [`deframe_buffered`] could not unframe. Nothing may read its body before
/// the handler refuses it.
pub(crate) fn still_framed(headers: &HeaderMap) -> bool {
    fakecloud_core::service::is_aws_chunked(headers)
}

/// Verify the integrity headers of a small, fully buffered request body (the
/// XML of PutBucketCors, DeleteObjects, PutObjectTagging, ...): a
/// `Content-MD5` and/or `x-amz-checksum-<algorithm>` value, or a trailing
/// checksum, must match the body (already unframed, [`deframe_buffered`]).
/// A bare `x-amz-sdk-checksum-algorithm` asserts nothing to check.
pub(crate) fn verify_request_body(headers: &HeaderMap, body: &[u8]) -> Result<(), AwsServiceError> {
    let checks = ChecksumHeaders::parse(headers)?;
    if checks.value.is_none() && checks.trailer.is_none() && checks.content_md5.is_none() {
        return Ok(());
    }
    verify_buffered(headers, body, &checks, None).map(drop)
}

/// Parse the `x-amz-checksum-type` header.
pub fn parse_type_header(headers: &HeaderMap) -> Result<Option<ChecksumType>, AwsServiceError> {
    match headers.get("x-amz-checksum-type") {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .ok()
            .and_then(ChecksumType::parse)
            .map(Some)
            .ok_or_else(|| invalid_request("Value for x-amz-checksum-type header is invalid.")),
    }
}

/// Insert the checksum headers S3 returns for a stored checksum
/// (`x-amz-checksum-<algo>` and, when known, `x-amz-checksum-type`).
pub fn insert_response_headers(
    headers: &mut HeaderMap,
    algorithm: ChecksumAlgorithm,
    value: &str,
    checksum_type: Option<ChecksumType>,
) {
    if let Ok(v) = value.parse() {
        headers.insert(algorithm.header_name(), v);
    }
    if let Some(t) = checksum_type {
        headers.insert(
            "x-amz-checksum-type",
            http::HeaderValue::from_static(t.as_str()),
        );
    }
}

/// `<ChecksumCRC32>value</ChecksumCRC32>`.
pub fn xml_element(algorithm: ChecksumAlgorithm, value: &str) -> String {
    format!(
        "<{tag}>{}</{tag}>",
        fakecloud_aws::xml::xml_escape(value),
        tag = algorithm.xml_element()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // Published check values: the CRC catalogue "check" field (digest of the
    // ASCII string "123456789") for CRC-32/ISO-HDLC, CRC-32/ISCSI and
    // CRC-64/NVME; FIPS 180-4 / RFC 1321 digests of the empty string and of
    // "123456789"; and the xxHash reference digests (Cyan4973/xxHash, seed 0)
    // cross-checked with the python-xxhash binding of the reference C library.
    const CHECK: &[u8] = b"123456789";

    fn hex(raw: &[u8]) -> String {
        raw.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn crc_check_values() {
        assert_eq!(hex(&digest(ChecksumAlgorithm::Crc32, CHECK)), "cbf43926");
        assert_eq!(hex(&digest(ChecksumAlgorithm::Crc32c, CHECK)), "e3069283");
        assert_eq!(
            hex(&digest(ChecksumAlgorithm::Crc64Nvme, CHECK)),
            "ae8b14860a799888"
        );
        assert_eq!(checksum(ChecksumAlgorithm::Crc32, CHECK), "y/Q5Jg==");
    }

    #[test]
    fn sha_and_md5_known_answers() {
        assert_eq!(
            checksum(ChecksumAlgorithm::Sha1, b""),
            "2jmj7l5rSw0yVb/vlWAYkK/YBwk="
        );
        assert_eq!(
            checksum(ChecksumAlgorithm::Sha256, b""),
            "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        );
        assert_eq!(
            hex(&digest(ChecksumAlgorithm::Sha512, b"")),
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce\
             47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        );
        assert_eq!(
            checksum(ChecksumAlgorithm::Sha512, CHECK),
            "2eZ2LdHI6vbWGzxhkvxAjU1tXxF20MKRabwk5xw/J0rSf81YEbMT1oH35V7ALXPUmclUVba1u1A6z1dPuo/+hQ=="
        );
        assert_eq!(
            hex(&digest(ChecksumAlgorithm::Md5, b"")),
            "d41d8cd98f00b204e9800998ecf8427e"
        );
        assert_eq!(
            checksum(ChecksumAlgorithm::Md5, CHECK),
            "JfnnlDI7RTiF9RgfG2JNCw=="
        );
    }

    #[test]
    fn xxhash_reference_digests_big_endian() {
        // Empty-input values from the xxHash reference implementation.
        assert_eq!(
            hex(&digest(ChecksumAlgorithm::XxHash64, b"")),
            "ef46db3751d8e999"
        );
        assert_eq!(
            hex(&digest(ChecksumAlgorithm::XxHash3, b"")),
            "2d06800538d394c2"
        );
        assert_eq!(
            hex(&digest(ChecksumAlgorithm::XxHash128, b"")),
            "99aa06d3014798d86001c324468d497f"
        );
        // Non-empty inputs, as base64 on the wire.
        assert_eq!(checksum(ChecksumAlgorithm::XxHash64, CHECK), "jLhB20DmroM=");
        assert_eq!(checksum(ChecksumAlgorithm::XxHash3, CHECK), "ctyxi2ehff8=");
        assert_eq!(
            checksum(ChecksumAlgorithm::XxHash128, CHECK),
            "MxGUd+3l3NXpcWQnaB1YYA=="
        );
        assert_eq!(
            checksum(ChecksumAlgorithm::XxHash128, b"hello world"),
            "340J6T+HSQCpm4d1zBW2xw=="
        );
    }

    #[test]
    fn incremental_matches_one_shot_for_every_algorithm() {
        // Cross the XXH3 internal buffer (256 B) and stripe boundaries.
        let data: Vec<u8> = (0..5000u32).map(|i| (i * 31 % 251) as u8).collect();
        for a in ChecksumAlgorithm::ALL {
            let mut h = Hasher::new(a);
            for chunk in data.chunks(97) {
                h.update(chunk);
            }
            let raw = h.finalize();
            assert_eq!(raw, digest(a, &data), "{a}");
            assert_eq!(raw.len(), a.digest_len(), "{a}");
        }
    }

    #[test]
    fn parse_is_case_insensitive_and_round_trips() {
        for a in ChecksumAlgorithm::ALL {
            assert_eq!(ChecksumAlgorithm::parse(a.as_str()), Some(a));
            assert_eq!(
                ChecksumAlgorithm::parse(&a.as_str().to_ascii_lowercase()),
                Some(a)
            );
            assert_eq!(ChecksumAlgorithm::parse(a.lowercase_name()), Some(a));
        }
        assert_eq!(ChecksumAlgorithm::parse("crc16"), None);
        assert_eq!(
            ChecksumType::parse("full_object"),
            Some(ChecksumType::FullObject)
        );
    }

    #[test]
    fn composite_hashes_concatenated_part_digests() {
        let p1 = digest(ChecksumAlgorithm::Sha256, b"part one");
        let p2 = digest(ChecksumAlgorithm::Sha256, b"part two");
        let mut concat = p1.clone();
        concat.extend_from_slice(&p2);
        let expected = format!("{}-2", checksum(ChecksumAlgorithm::Sha256, &concat));
        assert_eq!(
            composite(ChecksumAlgorithm::Sha256, &[p1, p2]),
            Some(expected)
        );
        assert_eq!(composite(ChecksumAlgorithm::Sha256, &[]), None);
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        h
    }

    fn code(err: AwsServiceError) -> String {
        err.code().to_string()
    }

    fn digests_of(data: &[u8], algos: &[ChecksumAlgorithm]) -> Digests {
        let mut m = MultiHasher::new(algos.iter().copied());
        m.update(data);
        m.finish()
    }

    #[test]
    fn verify_accepts_matching_and_rejects_mismatched_values() {
        let body = b"hello world";
        for a in ChecksumAlgorithm::ALL {
            let good = checksum(a, body);
            let h = ChecksumHeaders::parse(&headers(&[(a.header_name(), &good)])).unwrap();
            assert_eq!(h.algorithm(), Some(a));
            let d = digests_of(body, &h.algorithms_to_compute(None));
            let supplied = h.verify(&d, &[]).unwrap().unwrap();
            assert_eq!(supplied.value, good);

            let wrong = checksum(a, b"other");
            let h = ChecksumHeaders::parse(&headers(&[(a.header_name(), &wrong)])).unwrap();
            let d = digests_of(body, &h.algorithms_to_compute(None));
            assert_eq!(code(h.verify(&d, &[]).unwrap_err()), "BadDigest", "{a}");
        }
    }

    #[test]
    fn content_md5_is_validated_and_verified() {
        let body = b"hello world";
        let good = checksum(ChecksumAlgorithm::Md5, body);
        let h = ChecksumHeaders::parse(&headers(&[("content-md5", &good)])).unwrap();
        let d = digests_of(body, &h.algorithms_to_compute(None));
        h.verify(&d, &[]).unwrap();

        let wrong = checksum(ChecksumAlgorithm::Md5, b"x");
        let h = ChecksumHeaders::parse(&headers(&[("content-md5", &wrong)])).unwrap();
        let d = digests_of(body, &h.algorithms_to_compute(None));
        assert_eq!(code(h.verify(&d, &[]).unwrap_err()), "BadDigest");

        for bad in ["not base64!", "AAAA"] {
            let err = ChecksumHeaders::parse(&headers(&[("content-md5", bad)])).unwrap_err();
            assert_eq!(code(err), "InvalidDigest");
        }
    }

    #[test]
    fn malformed_and_multiple_values_are_invalid_requests() {
        let err =
            ChecksumHeaders::parse(&headers(&[("x-amz-checksum-crc32", "nope")])).unwrap_err();
        assert_eq!(code(err), "InvalidRequest");
        // Right base64, wrong digest length for the algorithm.
        let err =
            ChecksumHeaders::parse(&headers(&[("x-amz-checksum-sha256", "AAAAAA==")])).unwrap_err();
        assert_eq!(code(err), "InvalidRequest");
        let err = ChecksumHeaders::parse(&headers(&[
            ("x-amz-checksum-crc32", "AAAAAA=="),
            ("x-amz-checksum-crc32c", "AAAAAA=="),
        ]))
        .unwrap_err();
        assert_eq!(code(err), "InvalidRequest");
        let err = ChecksumHeaders::parse(&headers(&[("x-amz-sdk-checksum-algorithm", "CRC16")]))
            .unwrap_err();
        assert_eq!(code(err), "InvalidRequest");
        let err = ChecksumHeaders::parse(&headers(&[("x-amz-trailer", "x-amz-checksum-crc16")]))
            .unwrap_err();
        assert_eq!(code(err), "InvalidRequest");
        // Lowercase SDK algorithm names are accepted and normalized.
        let h = ChecksumHeaders::parse(&headers(&[("x-amz-sdk-checksum-algorithm", "sha512")]))
            .unwrap();
        assert_eq!(h.algorithm(), Some(ChecksumAlgorithm::Sha512));
    }

    #[test]
    fn trailing_checksum_is_verified() {
        let body = b"trailing payload";
        let h = ChecksumHeaders::parse(&headers(&[
            ("x-amz-trailer", "x-amz-checksum-xxhash3"),
            ("x-amz-sdk-checksum-algorithm", "XXHASH3"),
        ]))
        .unwrap();
        assert_eq!(h.algorithm(), Some(ChecksumAlgorithm::XxHash3));
        let d = digests_of(body, &h.algorithms_to_compute(None));
        let good = checksum(ChecksumAlgorithm::XxHash3, body);
        let trailers = vec![("x-amz-checksum-xxhash3".to_string(), good.clone())];
        assert_eq!(h.verify(&d, &trailers).unwrap().unwrap().value, good);

        let wrong = vec![(
            "x-amz-checksum-xxhash3".to_string(),
            checksum(ChecksumAlgorithm::XxHash3, b"x"),
        )];
        assert_eq!(code(h.verify(&d, &wrong).unwrap_err()), "BadDigest");
        // Declared but missing / named differently.
        assert_eq!(
            code(h.verify(&d, &[]).unwrap_err()),
            "MalformedTrailerError"
        );
        let other = vec![("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string())];
        assert_eq!(
            code(h.verify(&d, &other).unwrap_err()),
            "MalformedTrailerError"
        );
        let garbage = vec![("x-amz-checksum-xxhash3".to_string(), "zz".to_string())];
        assert_eq!(code(h.verify(&d, &garbage).unwrap_err()), "InvalidRequest");
    }

    #[test]
    fn buffered_aws_chunked_payload_is_decoded_and_trailer_verified() {
        let payload = b"annotation body";
        let good = checksum(ChecksumAlgorithm::Sha512, payload);
        let framed = format!(
            "{:x}\r\n{}\r\n0\r\nx-amz-checksum-sha512:{good}\r\n\r\n",
            payload.len(),
            std::str::from_utf8(payload).unwrap()
        );
        let framed_headers = headers(&[
            ("content-encoding", "aws-chunked, gzip"),
            ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            ("x-amz-trailer", "x-amz-checksum-sha512"),
        ]);
        let deframe = |text: &str| {
            let mut h = framed_headers.clone();
            let mut body = bytes::Bytes::from(text.to_string());
            deframe_buffered(&mut h, &mut body).map(|()| (h, body))
        };
        let (h, body) = deframe(&framed).unwrap();
        assert_eq!(&body[..], payload);
        assert_eq!(h["content-encoding"], "gzip");
        assert!(h.get("x-amz-trailer").is_none());
        assert_eq!(h["x-amz-checksum-sha512"], good.as_str());
        let checks = ChecksumHeaders::parse(&h).unwrap();
        let digests = verify_buffered(&h, &body, &checks, checks.algorithm()).unwrap();
        assert_eq!(digests.encoded(ChecksumAlgorithm::Sha512), Some(good));
        // Idempotent: an unframed request is left as it is.
        let (mut again, mut same) = (h.clone(), body.clone());
        deframe_buffered(&mut again, &mut same).unwrap();
        assert_eq!((again, same), (h, body));

        let bad = framed.replace("x-amz-checksum-sha512:", "x-amz-checksum-sha512:A");
        let err = deframe(&bad).unwrap_err();
        assert_eq!(code(err), "InvalidRequest");
        let other = format!(
            "{:x}\r\n{}\r\n0\r\nx-amz-checksum-sha512:{}\r\n\r\n",
            payload.len(),
            std::str::from_utf8(payload).unwrap(),
            checksum(ChecksumAlgorithm::Sha512, b"tampered")
        );
        let (h, body) = deframe(&other).unwrap();
        let checks = ChecksumHeaders::parse(&h).unwrap();
        let err = verify_buffered(&h, &body, &checks, None).unwrap_err();
        assert_eq!(code(err), "BadDigest");
        // An announced trailer that never came stays announced, and is
        // answered as a missing trailer.
        let missing = format!(
            "{:x}\r\n{}\r\n0\r\n\r\n",
            payload.len(),
            std::str::from_utf8(payload).unwrap()
        );
        let (h, body) = deframe(&missing).unwrap();
        assert_eq!(h["x-amz-trailer"], "x-amz-checksum-sha512");
        let checks = ChecksumHeaders::parse(&h).unwrap();
        let err = verify_buffered(&h, &body, &checks, None).unwrap_err();
        assert_eq!(code(err), "MalformedTrailerError");
        assert_eq!(
            code(deframe("zz\r\n").unwrap_err()),
            "InvalidChunkSizeError"
        );
    }

    #[test]
    fn buffered_content_sha256_mismatch_is_rejected() {
        let h = headers(&[("x-amz-content-sha256", &"0".repeat(64))]);
        let checks = ChecksumHeaders::parse(&h).unwrap();
        let err = verify_buffered(&h, b"x", &checks, None).unwrap_err();
        assert_eq!(code(err), "XAmzContentSHA256Mismatch");
        let h = headers(&[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")]);
        verify_buffered(&h, b"x", &checks, None).unwrap();
    }

    #[tokio::test]
    async fn spool_verified_hashes_once_and_cleans_up_on_mismatch() {
        let body = b"streamed object body".to_vec();
        let good = checksum(ChecksumAlgorithm::XxHash64, &body);
        let h = headers(&[("x-amz-checksum-xxhash64", &good)]);
        let checks = ChecksumHeaders::parse(&h).unwrap();
        let v = spool_verified(
            &h,
            axum::body::Body::from(body.clone()),
            None,
            &checks,
            Some(ChecksumAlgorithm::Crc64Nvme),
        )
        .await
        .unwrap();
        assert_eq!(v.digests.encoded(ChecksumAlgorithm::XxHash64), Some(good));
        assert_eq!(
            v.digests.encoded(ChecksumAlgorithm::Crc64Nvme),
            Some(checksum(ChecksumAlgorithm::Crc64Nvme, &body))
        );
        // MD5 comes from the spool itself.
        assert_eq!(
            v.digests.encoded(ChecksumAlgorithm::Md5),
            Some(checksum(ChecksumAlgorithm::Md5, &body))
        );
        let _ = std::fs::remove_file(&v.spooled.path);

        let dir = tempfile::tempdir().unwrap();
        let h = headers(&[(
            "x-amz-checksum-xxhash64",
            &checksum(ChecksumAlgorithm::XxHash64, b"other"),
        )]);
        let checks = ChecksumHeaders::parse(&h).unwrap();
        let err = spool_verified(
            &h,
            axum::body::Body::from(body),
            Some(dir.path()),
            &checks,
            None,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(code(err), "BadDigest");
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "a rejected upload must not leave its spool file behind"
        );
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        use sha2::Digest;
        sha2::Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    async fn spool(
        h: &HeaderMap,
        body: &[u8],
        store: Option<ChecksumAlgorithm>,
    ) -> Result<VerifiedSpool, AwsServiceError> {
        let checks = ChecksumHeaders::parse(h).unwrap();
        let v = spool_verified(
            h,
            axum::body::Body::from(body.to_vec()),
            None,
            &checks,
            store,
        )
        .await;
        if let Ok(v) = &v {
            let _ = std::fs::remove_file(&v.spooled.path);
        }
        v
    }

    #[tokio::test]
    async fn spool_verified_hashes_sha256_only_when_something_reads_it() {
        let body = b"streamed object body";
        // Nothing asserts or stores a SHA-256: it is never computed.
        for h in [
            headers(&[]),
            headers(&[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")]),
            headers(&[(
                "x-amz-content-sha256",
                "not-hex-but-64-chars-long-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            )]),
        ] {
            let v = spool(&h, body, Some(ChecksumAlgorithm::Crc64Nvme))
                .await
                .unwrap();
            assert_eq!(v.digests.get(ChecksumAlgorithm::Sha256), None);
        }
        // A hex payload hash is verified against the received bytes.
        let h = headers(&[("x-amz-content-sha256", &sha256_hex(body))]);
        let v = spool(&h, body, None).await.unwrap();
        assert_eq!(
            v.digests.encoded(ChecksumAlgorithm::Sha256),
            Some(checksum(ChecksumAlgorithm::Sha256, body))
        );
        let h = headers(&[("x-amz-content-sha256", &sha256_hex(b"other"))]);
        let err = spool(&h, body, None).await.err().unwrap();
        assert_eq!(code(err), "XAmzContentSHA256Mismatch");
        // Uppercase hex is still a hash assertion.
        let h = headers(&[("x-amz-content-sha256", &sha256_hex(body).to_uppercase())]);
        spool(&h, body, None).await.unwrap();
        // A stored SHA-256 checksum is computed without any header assertion.
        let v = spool(&headers(&[]), body, Some(ChecksumAlgorithm::Sha256))
            .await
            .unwrap();
        assert_eq!(
            v.digests.encoded(ChecksumAlgorithm::Sha256),
            Some(checksum(ChecksumAlgorithm::Sha256, body))
        );
    }

    #[test]
    fn content_sha256_assertion_parsing() {
        let hex = sha256_hex(b"x");
        assert!(
            asserted_content_sha256(&headers(&[("x-amz-content-sha256", &hex)]), false).is_some()
        );
        // On aws-chunked uploads the header covers the framing, not the payload.
        assert!(
            asserted_content_sha256(&headers(&[("x-amz-content-sha256", &hex)]), true).is_none()
        );
        for marker in [
            "UNSIGNED-PAYLOAD",
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        ] {
            assert!(
                asserted_content_sha256(&headers(&[("x-amz-content-sha256", marker)]), false)
                    .is_none()
            );
        }
        // 64 bytes that are not all hex, including multi-byte UTF-8, assert nothing.
        let odd = format!("{}é", "a".repeat(62));
        assert_eq!(odd.len(), 64);
        assert!(
            asserted_content_sha256(&headers(&[("x-amz-content-sha256", &odd)]), false).is_none()
        );
    }

    #[test]
    fn crc_combine_matches_hashing_the_concatenation() {
        let data: Vec<u8> = (0..70_000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
            .collect();
        for algo in [
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32c,
            ChecksumAlgorithm::Crc64Nvme,
        ] {
            for cuts in [
                vec![0usize],
                vec![1],
                vec![5, 6],
                vec![8192, 30_000, 69_999],
                vec![70_000],
            ] {
                let mut bounds = vec![0usize];
                bounds.extend(cuts.iter().copied());
                bounds.push(data.len());
                let parts: Vec<(Vec<u8>, u64)> = bounds
                    .windows(2)
                    .map(|w| (digest(algo, &data[w[0]..w[1]]), (w[1] - w[0]) as u64))
                    .collect();
                let refs: Vec<(&[u8], u64)> =
                    parts.iter().map(|(d, l)| (d.as_slice(), *l)).collect();
                assert_eq!(
                    combine_crc_digests(algo, &refs),
                    Some(digest(algo, &data)),
                    "{algo} cuts={cuts:?}"
                );
            }
        }
        assert_eq!(crc_combine(ChecksumAlgorithm::Sha256, 1, 2, 3), None);
    }

    #[test]
    fn multipart_type_support_matches_the_user_guide_table() {
        use ChecksumAlgorithm::*;
        for a in ChecksumAlgorithm::ALL {
            let full = matches!(a, Crc32 | Crc32c | Crc64Nvme);
            assert_eq!(a.supports(ChecksumType::FullObject), full, "{a}");
            assert_eq!(a.supports(ChecksumType::Composite), a != Crc64Nvme, "{a}");
        }
        assert_eq!(Crc64Nvme.default_multipart_type(), ChecksumType::FullObject);
        assert_eq!(Sha512.default_multipart_type(), ChecksumType::Composite);
    }
}
