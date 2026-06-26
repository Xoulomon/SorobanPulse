//! Content-based notification filtering (Issue #477).
//!
//! The notification system delivers all events matching the configured
//! `contract_id` filter. There is no way to filter on event *data* content
//! (e.g. "only notify when `amount` > 1000000"), so operators who only care
//! about high-value events receive notifications for everything, causing alert
//! fatigue.
//!
//! A [`ContentFilter`] describes a single predicate over an event's JSON data:
//! a JSONPath-style `path`, a comparison `op`, and a `value` to compare against.
//! Filters are evaluated before a notification is delivered; only events whose
//! data satisfies the predicate are delivered.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Supported comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FilterOp {
    /// Equal (numeric when both sides parse as numbers, else string).
    Eq,
    /// Not equal.
    Ne,
    /// Greater than.
    Gt,
    /// Less than.
    Lt,
    /// Greater than or equal.
    Gte,
    /// Less than or equal.
    Lte,
    /// Field (string or array) contains the value.
    Contains,
    /// Field matches the value interpreted as a regular expression.
    Matches,
}

/// A single content filter predicate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentFilter {
    /// JSONPath-style selector, e.g. `$.amount` or `$.transfer.to`.
    pub path: String,
    /// Comparison operator.
    pub op: FilterOp,
    /// Right-hand comparison value (always provided as a string).
    pub value: String,
}

impl ContentFilter {
    /// Validate the filter without evaluating it. Returns a human-readable error
    /// for invalid path syntax or an uncompilable regex (`matches`).
    pub fn validate(&self) -> Result<(), String> {
        if !self.path.starts_with('$') {
            return Err(format!(
                "content_filter path must start with '$' (got '{}')",
                self.path
            ));
        }
        // Reject an empty selector like "$" or "$." — there is nothing to test.
        if parse_path(&self.path).map_or(true, |segs| segs.is_empty()) {
            return Err(format!(
                "content_filter path '{}' does not select a field",
                self.path
            ));
        }
        if self.op == FilterOp::Matches {
            regex::Regex::new(&self.value)
                .map_err(|e| format!("content_filter value is not a valid regex: {e}"))?;
        }
        Ok(())
    }

    /// Evaluate the filter against an event's JSON data. Returns `true` when the
    /// notification should be delivered. A path that does not resolve, or a type
    /// mismatch, evaluates to `false` (the event does not match the predicate).
    pub fn evaluate(&self, data: &Value) -> bool {
        let Some(segments) = parse_path(&self.path) else {
            return false;
        };
        let Some(target) = resolve(data, &segments) else {
            // Absent field: only "ne" is satisfied (the value is not equal to it).
            return self.op == FilterOp::Ne;
        };
        compare(target, self.op, &self.value)
    }
}

/// Parse a JSONPath-style selector into segments. Supports `$.a.b`, `$.a[0]`,
/// and bracketed keys `$['a']`. Returns `None` for malformed input.
fn parse_path(path: &str) -> Option<Vec<Segment>> {
    let rest = path.strip_prefix('$')?;
    let mut segments = Vec::new();
    let mut chars = rest.chars().peekable();

    while let Some(&c) = chars.peek() {
        match c {
            '.' => {
                chars.next();
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == '[' {
                        break;
                    }
                    key.push(c);
                    chars.next();
                }
                if key.is_empty() {
                    return None;
                }
                segments.push(Segment::Key(key));
            }
            '[' => {
                chars.next();
                let mut inner = String::new();
                while let Some(&c) = chars.peek() {
                    if c == ']' {
                        break;
                    }
                    inner.push(c);
                    chars.next();
                }
                // Consume the closing ']'.
                if chars.next() != Some(']') {
                    return None;
                }
                let inner = inner.trim();
                if let Ok(idx) = inner.parse::<usize>() {
                    segments.push(Segment::Index(idx));
                } else {
                    let key = inner.trim_matches(|c| c == '\'' || c == '"');
                    if key.is_empty() {
                        return None;
                    }
                    segments.push(Segment::Key(key.to_string()));
                }
            }
            _ => return None,
        }
    }

    Some(segments)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Key(String),
    Index(usize),
}

/// Walk `data` following `segments`, returning the targeted value if present.
fn resolve<'a>(data: &'a Value, segments: &[Segment]) -> Option<&'a Value> {
    let mut current = data;
    for seg in segments {
        current = match seg {
            Segment::Key(k) => current.get(k)?,
            Segment::Index(i) => current.get(i)?,
        };
    }
    Some(current)
}

/// Compare a resolved JSON value against the filter's string value.
fn compare(target: &Value, op: FilterOp, expected: &str) -> bool {
    match op {
        FilterOp::Eq => values_equal(target, expected),
        FilterOp::Ne => !values_equal(target, expected),
        FilterOp::Gt | FilterOp::Lt | FilterOp::Gte | FilterOp::Lte => {
            ordered_compare(target, op, expected)
        }
        FilterOp::Contains => contains(target, expected),
        FilterOp::Matches => regex::Regex::new(expected)
            .map(|re| re.is_match(&scalar_to_string(target)))
            .unwrap_or(false),
    }
}

/// Extract a comparable `f64` from a JSON value or a string.
fn as_number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn values_equal(target: &Value, expected: &str) -> bool {
    // Prefer numeric equality when both sides are numbers.
    if let (Some(a), Some(b)) = (as_number(target), expected.trim().parse::<f64>().ok()) {
        return a == b;
    }
    if let Value::Bool(b) = target {
        if let Ok(eb) = expected.parse::<bool>() {
            return *b == eb;
        }
    }
    scalar_to_string(target) == expected
}

fn ordered_compare(target: &Value, op: FilterOp, expected: &str) -> bool {
    let ordering = match (as_number(target), expected.trim().parse::<f64>().ok()) {
        (Some(a), Some(b)) => a.partial_cmp(&b),
        // Fall back to lexicographic comparison for non-numeric operands.
        _ => Some(scalar_to_string(target).as_str().cmp(expected)),
    };
    let Some(ord) = ordering else { return false };
    match op {
        FilterOp::Gt => ord.is_gt(),
        FilterOp::Lt => ord.is_lt(),
        FilterOp::Gte => ord.is_ge(),
        FilterOp::Lte => ord.is_le(),
        _ => unreachable!("ordered_compare only handles gt/lt/gte/lte"),
    }
}

fn contains(target: &Value, expected: &str) -> bool {
    match target {
        Value::Array(items) => items.iter().any(|item| values_equal(item, expected)),
        Value::String(s) => s.contains(expected),
        other => scalar_to_string(other).contains(expected),
    }
}

/// Render a scalar JSON value as a string for string-wise comparisons. Objects
/// and arrays render as their compact JSON form.
fn scalar_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn filter(path: &str, op: FilterOp, value: &str) -> ContentFilter {
        ContentFilter {
            path: path.to_string(),
            op,
            value: value.to_string(),
        }
    }

    #[test]
    fn gt_delivers_only_high_value_events() {
        let f = filter("$.amount", FilterOp::Gt, "1000000");
        assert!(f.evaluate(&json!({ "amount": "2000000" })));
        assert!(f.evaluate(&json!({ "amount": 1500000 })));
        assert!(!f.evaluate(&json!({ "amount": "500000" })));
        assert!(!f.evaluate(&json!({ "amount": 1000000 }))); // not strictly greater
    }

    #[test]
    fn comparison_operators() {
        assert!(filter("$.n", FilterOp::Eq, "5").evaluate(&json!({"n": 5})));
        assert!(filter("$.n", FilterOp::Ne, "5").evaluate(&json!({"n": 6})));
        assert!(filter("$.n", FilterOp::Lt, "10").evaluate(&json!({"n": 9})));
        assert!(filter("$.n", FilterOp::Gte, "10").evaluate(&json!({"n": 10})));
        assert!(filter("$.n", FilterOp::Lte, "10").evaluate(&json!({"n": 10})));
        assert!(!filter("$.n", FilterOp::Gte, "10").evaluate(&json!({"n": 9})));
    }

    #[test]
    fn string_equality_and_inequality() {
        assert!(filter("$.status", FilterOp::Eq, "ok").evaluate(&json!({"status": "ok"})));
        assert!(filter("$.status", FilterOp::Ne, "ok").evaluate(&json!({"status": "fail"})));
    }

    #[test]
    fn contains_on_string_and_array() {
        assert!(filter("$.memo", FilterOp::Contains, "swap").evaluate(&json!({"memo": "token swap"})));
        assert!(filter("$.tags", FilterOp::Contains, "defi").evaluate(&json!({"tags": ["defi", "amm"]})));
        assert!(!filter("$.tags", FilterOp::Contains, "nft").evaluate(&json!({"tags": ["defi"]})));
    }

    #[test]
    fn matches_uses_regex() {
        let f = filter("$.account", FilterOp::Matches, "^G[A-Z0-9]+$");
        assert!(f.evaluate(&json!({"account": "GABC123"})));
        assert!(!f.evaluate(&json!({"account": "invalid"})));
    }

    #[test]
    fn nested_and_indexed_paths() {
        let data = json!({ "transfer": { "to": "GDEST" }, "amounts": [10, 250] });
        assert!(filter("$.transfer.to", FilterOp::Eq, "GDEST").evaluate(&data));
        assert!(filter("$.amounts[1]", FilterOp::Gt, "100").evaluate(&data));
    }

    #[test]
    fn absent_field_does_not_match_except_ne() {
        let data = json!({ "other": 1 });
        assert!(!filter("$.amount", FilterOp::Gt, "100").evaluate(&data));
        assert!(!filter("$.amount", FilterOp::Eq, "100").evaluate(&data));
        // "ne" against an absent field is vacuously true.
        assert!(filter("$.amount", FilterOp::Ne, "100").evaluate(&data));
    }

    #[test]
    fn validate_rejects_bad_path_and_regex() {
        assert!(filter("amount", FilterOp::Eq, "1").validate().is_err());
        assert!(filter("$", FilterOp::Eq, "1").validate().is_err());
        assert!(filter("$.account", FilterOp::Matches, "[unclosed").validate().is_err());
    }

    #[test]
    fn validate_accepts_well_formed_filters() {
        assert!(filter("$.amount", FilterOp::Gt, "1000000").validate().is_ok());
        assert!(filter("$.account", FilterOp::Matches, "^G.*$").validate().is_ok());
        assert!(filter("$.a.b[0]", FilterOp::Eq, "x").validate().is_ok());
    }

    #[test]
    fn deserializes_from_issue_example() {
        let f: ContentFilter =
            serde_json::from_value(json!({"path": "$.amount", "op": "gt", "value": "1000000"}))
                .expect("valid filter");
        assert_eq!(f.op, FilterOp::Gt);
        assert!(f.evaluate(&json!({"amount": "2000000"})));
    }
}
