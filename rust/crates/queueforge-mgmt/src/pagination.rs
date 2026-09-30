//! Cursor pagination helpers for management list endpoints.

use serde::{Deserialize, Serialize};

/// Maximum allowed `page_size` (design).
pub const MAX_PAGE_SIZE: u32 = 500;
/// Default `page_size` when omitted.
pub const DEFAULT_PAGE_SIZE: u32 = 100;

/// Common list query parameters.
#[derive(Debug, Clone, Deserialize)]
pub struct ListQuery {
    /// Page size (capped at [`MAX_PAGE_SIZE`]).
    pub page_size: Option<u32>,
    /// Opaque cursor (last seen name from previous page).
    pub cursor: Option<String>,
    /// Optional name prefix filter.
    pub name_prefix: Option<String>,
}

impl ListQuery {
    /// Resolved, capped page size.
    pub fn page_size(&self) -> usize {
        self.page_size
            .unwrap_or(DEFAULT_PAGE_SIZE)
            .clamp(1, MAX_PAGE_SIZE) as usize
    }

    /// Whether `name` matches the optional prefix filter.
    pub fn matches_prefix(&self, name: &str) -> bool {
        match &self.name_prefix {
            Some(p) if !p.is_empty() => name.starts_with(p.as_str()),
            _ => true,
        }
    }
}

/// Standard paginated list response.
#[derive(Debug, Clone, Serialize)]
pub struct Page<T: Serialize> {
    /// Page items.
    pub items: Vec<T>,
    /// Cursor for the next page (`None` if this is the last page).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Total matching items (before pagination), when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_count: Option<u64>,
}

/// Paginate a sorted-by-name slice.
///
/// `name_fn` extracts the sort/cursor key from each item. Items must already
/// be sorted ascending by that key. Cursor is exclusive (items *after* cursor).
pub fn paginate_by_name<T, F>(mut items: Vec<T>, query: &ListQuery, name_fn: F) -> Page<T>
where
    T: Serialize,
    F: Fn(&T) -> &str,
{
    items.retain(|item| query.matches_prefix(name_fn(item)));
    let total_count = items.len() as u64;

    if let Some(cursor) = query.cursor.as_deref() {
        items.retain(|item| name_fn(item) > cursor);
    }

    let page_size = query.page_size();
    let mut next_cursor = None;
    if items.len() > page_size {
        items.truncate(page_size);
        if let Some(last) = items.last() {
            next_cursor = Some(name_fn(last).to_string());
        }
    }

    Page {
        items,
        next_cursor,
        total_count: Some(total_count),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paginate_with_cursor_and_prefix() {
        let items: Vec<String> = ["a", "ab", "b", "c", "d"]
            .into_iter()
            .map(String::from)
            .collect();
        let q = ListQuery {
            page_size: Some(2),
            cursor: None,
            name_prefix: Some("".into()),
        };
        let page = paginate_by_name(items.clone(), &q, |s| s.as_str());
        assert_eq!(page.items, vec!["a", "ab"]);
        assert_eq!(page.next_cursor.as_deref(), Some("ab"));
        assert_eq!(page.total_count, Some(5));

        let q2 = ListQuery {
            page_size: Some(2),
            cursor: Some("ab".into()),
            name_prefix: None,
        };
        let page2 = paginate_by_name(items, &q2, |s| s.as_str());
        assert_eq!(page2.items, vec!["b", "c"]);
        assert_eq!(page2.next_cursor.as_deref(), Some("c"));
    }
}
