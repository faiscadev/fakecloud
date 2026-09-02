//! Vector similarity search (`SearchVectors`).
//!
//! A vector index names one list-valued attribute on the table. `SearchVectors`
//! reads that attribute off every item, scores it against the query vector with
//! the index's distance function, and returns the top K. The scoring is real —
//! cosine, dot-product and Euclidean over the stored numbers — so a search
//! actually ranks the items the caller wrote.

use super::*;

/// Read a list of DynamoDB `N` values into a numeric vector.
fn numbers_from(list: &[AttributeValue]) -> Option<Vec<f64>> {
    list.iter()
        .map(|e| e.get("N")?.as_str()?.parse::<f64>().ok())
        .collect()
}

/// Read an item's vector attribute, which is a DynamoDB `L` of `N` values.
/// Returns `None` when the attribute is absent or is not a list of numbers,
/// which is how an item without a usable vector is skipped rather than scored
/// as zeroes.
fn item_vector(value: Option<&AttributeValue>) -> Option<Vec<f64>> {
    numbers_from(value?.get("L")?.as_array()?)
}

/// Read the request's `SearchVector`. It is modeled as `SearchVectorList` —
/// a bare list of `AttributeValue` — not an `L`-wrapped attribute, so it is
/// parsed differently from the item attribute it is compared against.
fn search_vector(value: Option<&Value>) -> Option<Vec<f64>> {
    numbers_from(value?.as_array()?)
}

fn dot(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Score `candidate` against `query` under `function`. Cosine and dot-product
/// are similarities (higher is closer); Euclidean is a distance (lower is
/// closer), which the caller accounts for when ordering.
fn score(function: &str, query: &[f64], candidate: &[f64]) -> f64 {
    match function {
        "DOT_PRODUCT" => dot(query, candidate),
        "EUCLIDEAN" => query
            .iter()
            .zip(candidate)
            .map(|(x, y)| (x - y) * (x - y))
            .sum::<f64>()
            .sqrt(),
        // COSINE, and the default.
        _ => {
            let denom = dot(query, query).sqrt() * dot(candidate, candidate).sqrt();
            if denom == 0.0 {
                0.0
            } else {
                dot(query, candidate) / denom
            }
        }
    }
}

impl DynamoDbService {
    pub(super) fn search_vectors(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let index_name = require_str(&body, "IndexName")?;

        let query_vector = search_vector(body.get("SearchVector")).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "SearchVector must be a list of numbers",
            )
        })?;
        if query_vector.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "SearchVector must not be empty",
            ));
        }
        let top_k = body["TopK"].as_i64().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TopK is required",
            )
        })?;
        if top_k < 1 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TopK must be at least 1",
            ));
        }

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        let table = get_table(&state.tables, table_name)?;
        let index = table
            .vector_indexes
            .iter()
            .find(|i| i.index_name == index_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Vector index not found: {index_name}"),
                )
            })?;
        // A query vector of the wrong width cannot be compared with the
        // indexed vectors at all.
        if query_vector.len() as i64 != index.dimensions {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "SearchVector has {} dimensions but index {index_name} expects {}",
                    query_vector.len(),
                    index.dimensions
                ),
            ));
        }

        let mut scored: Vec<(f64, &HashMap<String, AttributeValue>)> = table
            .items
            .iter()
            .filter_map(|item| {
                let candidate = item_vector(item.get(&index.vector_attribute))?;
                // Items whose vector does not match the index width are not
                // indexed, so they cannot be returned.
                if candidate.len() as i64 != index.dimensions {
                    return None;
                }
                Some((
                    score(&index.distance_function, &query_vector, &candidate),
                    item,
                ))
            })
            .collect();

        // Euclidean ranks ascending (nearest first); the similarity functions
        // rank descending.
        if index.distance_function == "EUCLIDEAN" {
            scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        } else {
            scored.sort_by(|a, b| b.0.total_cmp(&a.0));
        }

        let results: Vec<Value> = scored
            .into_iter()
            .take(top_k as usize)
            .map(|(s, item)| json!({ "Item": project_item(item, &body), "Score": s }))
            .collect();

        let mut out = json!({ "SearchResults": results });
        if return_consumed_mode(&body) != "NONE" {
            // The read cost is expressed in bytes for vector operations.
            let bytes = query_vector.len() as f64 * 8.0;
            out["ConsumedCapacity"] = json!({
                "VectorSearchRequestBytes": bytes,
                "VectorWriteRequestBytes": 0.0,
            });
        }
        Self::ok_json(out)
    }
}
