//! Pure rendering helpers for Redis values. I/O lives in the connection; the
//! byte→display decisions here are unit tested.

use base64::Engine as _;

use crate::broker::{PayloadEncoding, StreamEntry, ValueView};

/// Render a (possibly truncated) string value.
///
/// `total_bytes` is the full `STRLEN`; `bytes` may be a prefix when the value
/// exceeds the preview limit. UTF-8 that parses as JSON is pretty-printed;
/// other UTF-8 is shown verbatim; non-UTF-8 is base64-encoded.
pub fn render_string(bytes: Vec<u8>, total_bytes: usize) -> ValueView {
    let shown_bytes = bytes.len();
    match std::str::from_utf8(&bytes) {
        Ok(text) => {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
                let pretty =
                    serde_json::to_string_pretty(&json).unwrap_or_else(|_| text.to_string());
                ValueView::Str {
                    total_bytes,
                    shown_bytes,
                    text: pretty,
                    encoding: PayloadEncoding::Json,
                }
            } else {
                ValueView::Str {
                    total_bytes,
                    shown_bytes,
                    text: text.to_string(),
                    encoding: PayloadEncoding::Utf8,
                }
            }
        }
        // A byte-prefix preview can slice a multi-byte codepoint at its tail.
        // When the value was truncated, salvage the valid UTF-8 portion rather
        // than mislabeling a genuinely-textual value as binary/base64.
        Err(e) if shown_bytes < total_bytes && e.valid_up_to() > 0 => {
            let valid = e.valid_up_to();
            let text = std::str::from_utf8(&bytes[..valid])
                .expect("valid_up_to() bytes are valid UTF-8")
                .to_string();
            ValueView::Str {
                total_bytes,
                shown_bytes: valid,
                text,
                encoding: PayloadEncoding::Utf8,
            }
        }
        Err(_) => ValueView::Str {
            total_bytes,
            shown_bytes,
            text: base64::engine::general_purpose::STANDARD.encode(&bytes),
            encoding: PayloadEncoding::Base64,
        },
    }
}

/// Wrap a window of list elements (already fetched via `LRANGE`) as a value.
/// `offset` is the index of `items[0]` within the full list of `len` elements.
pub fn render_list(len: usize, offset: usize, items: Vec<String>) -> ValueView {
    ValueView::List { len, offset, items }
}

/// Wrap a sample of set members (already fetched via `SSCAN`) as a value.
pub fn render_set(len: usize, members: Vec<String>) -> ValueView {
    ValueView::Set { len, members }
}

/// Pair a flat `[field, value, field, value, …]` reply (from `HSCAN`) into a
/// hash value. A trailing unpaired element — which only occurs in a truncated
/// reply — is dropped.
pub fn render_hash(len: usize, flat: Vec<String>) -> ValueView {
    ValueView::Hash {
        len,
        fields: pair_up(flat),
    }
}

/// Pair a flat `[member, score, member, score, …]` reply (from
/// `ZRANGE … WITHSCORES`) into a sorted-set value. A member whose score doesn't
/// parse as a float is dropped rather than failing the whole inspection.
pub fn render_zset(len: usize, flat: Vec<String>) -> ValueView {
    let items = flat
        .chunks_exact(2)
        .filter_map(|c| c[1].parse::<f64>().ok().map(|score| (c[0].clone(), score)))
        .collect();
    ValueView::ZSet { len, items }
}

/// Build a stream value from raw `XRANGE` entries (`[(id, [field, value, …]), …]`),
/// pairing each entry's flat field list and recording the last id seen (empty
/// when there are no entries).
pub fn render_stream(len: usize, raw: Vec<(String, Vec<String>)>) -> ValueView {
    let entries: Vec<StreamEntry> = raw
        .into_iter()
        .map(|(id, flat)| StreamEntry {
            id,
            fields: pair_up(flat),
        })
        .collect();
    let last_id = entries.last().map(|e| e.id.clone()).unwrap_or_default();
    ValueView::Stream {
        len,
        last_id,
        entries,
    }
}

/// Pair a flat `[a, b, a, b, …]` list into `(a, b)` tuples, dropping a trailing
/// unpaired element (only present in a truncated reply).
fn pair_up(flat: Vec<String>) -> Vec<(String, String)> {
    flat.chunks_exact(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_utf8() {
        let v = render_string(b"hello".to_vec(), 5);
        match v {
            ValueView::Str {
                text,
                encoding,
                total_bytes,
                shown_bytes,
            } => {
                assert_eq!(text, "hello");
                assert_eq!(encoding, PayloadEncoding::Utf8);
                assert_eq!(total_bytes, 5);
                assert_eq!(shown_bytes, 5);
            }
            _ => panic!("expected Str"),
        }
    }

    #[test]
    fn json_is_pretty_printed() {
        let v = render_string(br#"{"a":1,"b":[2,3]}"#.to_vec(), 17);
        match v {
            ValueView::Str { text, encoding, .. } => {
                assert_eq!(encoding, PayloadEncoding::Json);
                assert!(text.contains('\n'), "pretty JSON should be multiline");
                assert!(text.contains("\"a\": 1"));
            }
            _ => panic!("expected Str"),
        }
    }

    #[test]
    fn truncated_prefix_salvages_valid_utf8_boundary() {
        // "héllo" = [h, 0xC3, 0xA9, l, l, o]; a 2-byte preview slices the 'é'.
        // The valid prefix ("h") must render as UTF-8, not the whole as base64.
        let v = render_string(vec![b'h', 0xC3], 6);
        match v {
            ValueView::Str {
                text,
                encoding,
                shown_bytes,
                total_bytes,
            } => {
                assert_eq!(encoding, PayloadEncoding::Utf8);
                assert_eq!(text, "h");
                assert_eq!(shown_bytes, 1, "only the valid-up-to prefix is shown");
                assert_eq!(total_bytes, 6);
            }
            _ => panic!("expected Str"),
        }
    }

    #[test]
    fn binary_is_base64() {
        let v = render_string(vec![0x00, 0x01, 0xff, 0xfe], 4);
        match v {
            ValueView::Str { text, encoding, .. } => {
                assert_eq!(encoding, PayloadEncoding::Base64);
                assert_eq!(text, "AAH//g==");
            }
            _ => panic!("expected Str"),
        }
    }

    fn strs(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn list_carries_len_offset_and_items() {
        match render_list(100, 10, strs(&["a", "b"])) {
            ValueView::List { len, offset, items } => {
                assert_eq!(len, 100);
                assert_eq!(offset, 10);
                assert_eq!(items, strs(&["a", "b"]));
            }
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn set_carries_len_and_members() {
        match render_set(3, strs(&["x", "y"])) {
            ValueView::Set { len, members } => {
                assert_eq!(len, 3);
                assert_eq!(members, strs(&["x", "y"]));
            }
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn hash_pairs_flat_reply_and_drops_trailing_unpaired() {
        // A truncated HSCAN can leave a dangling field with no value; it must be
        // dropped, not paired with a phantom value.
        match render_hash(2, strs(&["f1", "v1", "f2", "v2", "dangling"])) {
            ValueView::Hash { len, fields } => {
                assert_eq!(len, 2);
                assert_eq!(
                    fields,
                    vec![
                        ("f1".to_string(), "v1".to_string()),
                        ("f2".to_string(), "v2".to_string()),
                    ]
                );
            }
            _ => panic!("expected Hash"),
        }
    }

    #[test]
    fn zset_parses_scores_and_drops_unparseable() {
        match render_zset(3, strs(&["a", "1.5", "b", "not-a-number", "c", "-2"])) {
            ValueView::ZSet { len, items } => {
                assert_eq!(len, 3);
                // "b" is dropped: its score doesn't parse, but that must not fail
                // the whole inspection.
                assert_eq!(items, vec![("a".to_string(), 1.5), ("c".to_string(), -2.0)]);
            }
            _ => panic!("expected ZSet"),
        }
    }

    #[test]
    fn stream_builds_entries_and_tracks_last_id() {
        let raw = vec![
            ("1-0".to_string(), strs(&["k", "v"])),
            ("2-0".to_string(), strs(&["a", "b", "c", "d"])),
        ];
        match render_stream(2, raw) {
            ValueView::Stream {
                len,
                last_id,
                entries,
            } => {
                assert_eq!(len, 2);
                assert_eq!(last_id, "2-0");
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[0].id, "1-0");
                assert_eq!(entries[0].fields, vec![("k".to_string(), "v".to_string())]);
                assert_eq!(entries[1].fields.len(), 2);
            }
            _ => panic!("expected Stream"),
        }
    }

    #[test]
    fn empty_stream_has_blank_last_id() {
        match render_stream(0, vec![]) {
            ValueView::Stream {
                last_id, entries, ..
            } => {
                assert!(last_id.is_empty());
                assert!(entries.is_empty());
            }
            _ => panic!("expected Stream"),
        }
    }
}
