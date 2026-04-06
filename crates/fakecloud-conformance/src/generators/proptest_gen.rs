//! Strategy 4: Property-based random value generation.
//!
//! Uses a deterministic xorshift64 PRNG seeded from the operation name to generate
//! N random but constraint-valid inputs per operation. This provides fuzzing-like
//! coverage without the heavy `proptest` dependency.

use serde_json::Value;
use std::collections::HashMap;

use super::{Expectation, Strategy, TestVariant};
use crate::smithy::{self, ServiceModel, ShapeType};

/// Simple xorshift64 PRNG for deterministic pseudo-random generation.
struct Xorshift64 {
    state: u64,
}

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        // Avoid zero state which would produce only zeros.
        Self {
            state: if seed == 0 {
                0x5EED_DEAD_BEEF_CAFE
            } else {
                seed
            },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Random u64 in [lo, hi] inclusive.
    fn range_u64(&mut self, lo: u64, hi: u64) -> u64 {
        if lo >= hi {
            return lo;
        }
        lo + self.next_u64() % (hi - lo + 1)
    }

    /// Random i64 in [lo, hi] inclusive.
    fn range_i64(&mut self, lo: i64, hi: i64) -> i64 {
        if lo >= hi {
            return lo;
        }
        let span = (hi as i128 - lo as i128 + 1) as u128;
        let offset = (self.next_u64() as u128) % span;
        lo.wrapping_add(offset as i64)
    }

    /// Random f64 in [lo, hi].
    fn range_f64(&mut self, lo: f64, hi: f64) -> f64 {
        if lo >= hi {
            return lo;
        }
        let t = (self.next_u64() as f64) / (u64::MAX as f64);
        lo + t * (hi - lo)
    }

    /// Random bool.
    fn next_bool(&mut self) -> bool {
        self.next_u64() & 1 == 0
    }

    /// Random usize in [0, n) (exclusive upper bound).
    fn next_usize(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        (self.next_u64() % n as u64) as usize
    }
}

/// Derive a deterministic seed from an operation name and variant index.
fn seed_for(operation_name: &str, variant_index: usize) -> u64 {
    // Simple FNV-1a-ish hash of the name + index.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in operation_name.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h ^= variant_index as u64;
    h = h.wrapping_mul(0x0100_0000_01b3);
    h
}

const ALPHANUMERIC: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const BASE64_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn generate(
    model: &ServiceModel,
    input_shape_id: &str,
    overrides: &HashMap<String, Value>,
    num_variants: usize,
) -> Vec<TestVariant> {
    let mut variants = Vec::new();

    for i in 0..num_variants {
        let seed = seed_for(input_shape_id, i);
        let mut rng = Xorshift64::new(seed);

        let input = random_value_for_shape(model, input_shape_id, overrides, &mut rng, 0);

        variants.push(TestVariant {
            name: format!("proptest_{}", i),
            strategy: Strategy::Proptest,
            input,
            expectation: Expectation::Success,
        });
    }

    variants
}

fn random_value_for_shape(
    model: &ServiceModel,
    shape_id: &str,
    overrides: &HashMap<String, Value>,
    rng: &mut Xorshift64,
    depth: usize,
) -> Value {
    if depth > 8 {
        return Value::Null;
    }

    if smithy::is_prelude_shape(shape_id) {
        return random_prelude_value(shape_id, rng);
    }

    let shape = match model.shapes.get(shape_id) {
        Some(s) => s,
        None => return random_prelude_value(shape_id, rng),
    };

    random_value_for_shape_def(model, shape, overrides, rng, depth)
}

fn random_value_for_shape_def(
    model: &ServiceModel,
    shape: &smithy::Shape,
    overrides: &HashMap<String, Value>,
    rng: &mut Xorshift64,
    depth: usize,
) -> Value {
    match &shape.shape_type {
        ShapeType::Structure { members } => {
            let mut obj = serde_json::Map::new();
            for member in members {
                // Always apply overrides.
                if let Some(ov) = overrides.get(&member.name) {
                    obj.insert(member.name.clone(), ov.clone());
                    continue;
                }

                // Always include required fields; randomly include optional fields.
                if member.required || rng.next_bool() {
                    let val =
                        random_value_for_shape(model, &member.target, overrides, rng, depth + 1);
                    obj.insert(member.name.clone(), val);
                }
            }
            Value::Object(obj)
        }
        ShapeType::List { member_target } => {
            let len = rng.next_usize(6); // 0..5
            let items: Vec<Value> = (0..len)
                .map(|_| random_value_for_shape(model, member_target, overrides, rng, depth + 1))
                .collect();
            Value::Array(items)
        }
        ShapeType::Map {
            key_target,
            value_target,
        } => {
            let len = rng.next_usize(4); // 0..3
            let mut obj = serde_json::Map::new();
            for _ in 0..len {
                let key = match random_value_for_shape(model, key_target, overrides, rng, depth + 1)
                {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                let val = random_value_for_shape(model, value_target, overrides, rng, depth + 1);
                obj.insert(key, val);
            }
            Value::Object(obj)
        }
        ShapeType::Union { members } => {
            if members.is_empty() {
                return Value::Object(serde_json::Map::new());
            }
            let idx = rng.next_usize(members.len());
            let chosen = &members[idx];
            let mut obj = serde_json::Map::new();
            let val = random_value_for_shape(model, &chosen.target, overrides, rng, depth + 1);
            obj.insert(chosen.name.clone(), val);
            Value::Object(obj)
        }
        ShapeType::String { enum_values } => {
            if let Some(values) = enum_values {
                if !values.is_empty() {
                    let idx = rng.next_usize(values.len());
                    return Value::String(values[idx].value.clone());
                }
            }
            random_string(&shape.traits, rng)
        }
        ShapeType::Enum { values } => {
            if values.is_empty() {
                return Value::String("test".to_string());
            }
            let idx = rng.next_usize(values.len());
            Value::String(values[idx].value.clone())
        }
        ShapeType::IntEnum { values } => {
            if values.is_empty() {
                return Value::Number(0.into());
            }
            let idx = rng.next_usize(values.len());
            Value::Number(values[idx].1.into())
        }
        ShapeType::Integer | ShapeType::Long => {
            let lo = shape.traits.range_min.map(|v| v as i64).unwrap_or(0);
            let hi = shape
                .traits
                .range_max
                .map(|v| v as i64)
                .unwrap_or(lo + 1000);
            let val = rng.range_i64(lo, hi);
            Value::Number(val.into())
        }
        ShapeType::Float | ShapeType::Double => {
            let lo = shape.traits.range_min.unwrap_or(0.0);
            let hi = shape.traits.range_max.unwrap_or(lo + 1000.0);
            let val = rng.range_f64(lo, hi);
            match serde_json::Number::from_f64(val) {
                Some(n) => Value::Number(n),
                None => Value::Number(serde_json::Number::from_f64(1.0).unwrap()),
            }
        }
        ShapeType::Boolean => Value::Bool(rng.next_bool()),
        ShapeType::Blob => {
            // Random base64 string (4-24 chars, multiple of 4).
            let groups = rng.range_u64(1, 6) as usize;
            let len = groups * 4;
            let s: String = (0..len)
                .map(|_| {
                    let idx = rng.next_usize(BASE64_CHARS.len());
                    BASE64_CHARS[idx] as char
                })
                .collect();
            Value::String(s)
        }
        ShapeType::Timestamp => {
            // Random ISO8601 timestamp between 2020 and 2030.
            let year = rng.range_u64(2020, 2030);
            let month = rng.range_u64(1, 12);
            let day = rng.range_u64(1, 28); // safe for all months
            let hour = rng.range_u64(0, 23);
            let minute = rng.range_u64(0, 59);
            let second = rng.range_u64(0, 59);
            Value::String(format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                year, month, day, hour, minute, second
            ))
        }
        _ => Value::Null,
    }
}

/// Generate a random string respecting length constraints from traits.
fn random_string(traits: &smithy::ShapeTraits, rng: &mut Xorshift64) -> Value {
    let min_len = traits.length_min.unwrap_or(1) as usize;
    let max_len = traits
        .length_max
        .map(|m| (m as usize).min(200))
        .unwrap_or(min_len + 20);
    let max_len = max_len.max(min_len);

    let len = rng.range_u64(min_len as u64, max_len as u64) as usize;

    let s: String = (0..len)
        .map(|_| {
            let idx = rng.next_usize(ALPHANUMERIC.len());
            ALPHANUMERIC[idx] as char
        })
        .collect();

    Value::String(s)
}

/// Generate a random value for a Smithy prelude type.
fn random_prelude_value(shape_id: &str, rng: &mut Xorshift64) -> Value {
    match shape_id {
        "smithy.api#String" | "smithy.api#Document" => {
            let len = rng.range_u64(1, 20) as usize;
            let s: String = (0..len)
                .map(|_| {
                    let idx = rng.next_usize(ALPHANUMERIC.len());
                    ALPHANUMERIC[idx] as char
                })
                .collect();
            Value::String(s)
        }
        "smithy.api#Integer"
        | "smithy.api#Short"
        | "smithy.api#Byte"
        | "smithy.api#PrimitiveInteger"
        | "smithy.api#PrimitiveShort"
        | "smithy.api#PrimitiveByte" => {
            let val = rng.range_i64(0, 1000);
            Value::Number(val.into())
        }
        "smithy.api#Long" | "smithy.api#BigInteger" | "smithy.api#PrimitiveLong" => {
            let val = rng.range_i64(0, 100_000);
            Value::Number(val.into())
        }
        "smithy.api#Float"
        | "smithy.api#Double"
        | "smithy.api#BigDecimal"
        | "smithy.api#PrimitiveFloat"
        | "smithy.api#PrimitiveDouble" => {
            let val = rng.range_f64(0.0, 1000.0);
            Value::Number(serde_json::Number::from_f64(val).unwrap_or_else(|| 1.into()))
        }
        "smithy.api#Boolean" | "smithy.api#PrimitiveBoolean" => Value::Bool(rng.next_bool()),
        "smithy.api#Blob" => {
            let groups = rng.range_u64(1, 6) as usize;
            let len = groups * 4;
            let s: String = (0..len)
                .map(|_| {
                    let idx = rng.next_usize(BASE64_CHARS.len());
                    BASE64_CHARS[idx] as char
                })
                .collect();
            Value::String(s)
        }
        "smithy.api#Timestamp" => {
            let year = rng.range_u64(2020, 2030);
            let month = rng.range_u64(1, 12);
            let day = rng.range_u64(1, 28);
            let hour = rng.range_u64(0, 23);
            let minute = rng.range_u64(0, 59);
            let second = rng.range_u64(0, 59);
            Value::String(format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                year, month, day, hour, minute, second
            ))
        }
        "smithy.api#Unit" => Value::Object(serde_json::Map::new()),
        _ => {
            let len = rng.range_u64(1, 10) as usize;
            let s: String = (0..len)
                .map(|_| {
                    let idx = rng.next_usize(ALPHANUMERIC.len());
                    ALPHANUMERIC[idx] as char
                })
                .collect();
            Value::String(s)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xorshift64_is_deterministic() {
        let mut a = Xorshift64::new(42);
        let mut b = Xorshift64::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn xorshift64_range_stays_in_bounds() {
        let mut rng = Xorshift64::new(123);
        for _ in 0..1000 {
            let v = rng.range_u64(5, 10);
            assert!((5..=10).contains(&v));
        }
        for _ in 0..1000 {
            let v = rng.range_i64(-10, 10);
            assert!((-10..=10).contains(&v));
        }
        for _ in 0..1000 {
            let v = rng.range_f64(0.0, 1.0);
            assert!((0.0..=1.0).contains(&v));
        }
    }

    #[test]
    fn range_i64_full_span_does_not_overflow() {
        let mut rng = Xorshift64::new(999);
        // This used to panic due to overflow in (max - min + 1).
        for _ in 0..100 {
            let _ = rng.range_i64(i64::MIN, i64::MAX);
        }
    }

    #[test]
    fn seed_for_is_deterministic() {
        let a = seed_for("CreateQueue", 0);
        let b = seed_for("CreateQueue", 0);
        assert_eq!(a, b);

        let c = seed_for("CreateQueue", 1);
        assert_ne!(a, c);

        let d = seed_for("SendMessage", 0);
        assert_ne!(a, d);
    }

    #[test]
    fn generate_produces_requested_count() {
        let model = ServiceModel {
            service_name: "test".to_string(),
            operations: vec![],
            shapes: {
                let mut m = HashMap::new();
                m.insert(
                    "test#Input".to_string(),
                    smithy::Shape {
                        shape_id: "test#Input".to_string(),
                        shape_type: ShapeType::Structure {
                            members: vec![smithy::Member {
                                name: "Name".to_string(),
                                target: "smithy.api#String".to_string(),
                                required: true,
                                traits: smithy::ShapeTraits::default(),
                            }],
                        },
                        traits: smithy::ShapeTraits::default(),
                    },
                );
                m
            },
        };

        let overrides = HashMap::new();
        let variants = generate(&model, "test#Input", &overrides, 10);
        assert_eq!(variants.len(), 10);

        // All should have the Proptest strategy.
        for v in &variants {
            assert_eq!(v.strategy, Strategy::Proptest);
        }

        // All should include the required "Name" field.
        for v in &variants {
            assert!(v.input.get("Name").is_some());
        }
    }

    #[test]
    fn generate_respects_overrides() {
        let model = ServiceModel {
            service_name: "test".to_string(),
            operations: vec![],
            shapes: {
                let mut m = HashMap::new();
                m.insert(
                    "test#Input".to_string(),
                    smithy::Shape {
                        shape_id: "test#Input".to_string(),
                        shape_type: ShapeType::Structure {
                            members: vec![
                                smithy::Member {
                                    name: "Name".to_string(),
                                    target: "smithy.api#String".to_string(),
                                    required: true,
                                    traits: smithy::ShapeTraits::default(),
                                },
                                smithy::Member {
                                    name: "Count".to_string(),
                                    target: "smithy.api#Integer".to_string(),
                                    required: true,
                                    traits: smithy::ShapeTraits::default(),
                                },
                            ],
                        },
                        traits: smithy::ShapeTraits::default(),
                    },
                );
                m
            },
        };

        let mut overrides = HashMap::new();
        overrides.insert("Name".to_string(), Value::String("fixed".to_string()));

        let variants = generate(&model, "test#Input", &overrides, 5);
        for v in &variants {
            assert_eq!(v.input.get("Name").unwrap(), "fixed");
        }
    }

    #[test]
    fn generate_is_deterministic() {
        let model = ServiceModel {
            service_name: "test".to_string(),
            operations: vec![],
            shapes: {
                let mut m = HashMap::new();
                m.insert(
                    "test#Input".to_string(),
                    smithy::Shape {
                        shape_id: "test#Input".to_string(),
                        shape_type: ShapeType::Structure {
                            members: vec![smithy::Member {
                                name: "Name".to_string(),
                                target: "smithy.api#String".to_string(),
                                required: true,
                                traits: smithy::ShapeTraits::default(),
                            }],
                        },
                        traits: smithy::ShapeTraits::default(),
                    },
                );
                m
            },
        };

        let overrides = HashMap::new();
        let a = generate(&model, "test#Input", &overrides, 5);
        let b = generate(&model, "test#Input", &overrides, 5);

        for (va, vb) in a.iter().zip(b.iter()) {
            assert_eq!(va.input, vb.input);
        }
    }
}
