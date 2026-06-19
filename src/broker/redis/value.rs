//! Pure rendering helpers for Redis values. I/O lives in the connection; the
//! byte→display decisions here are unit tested.

use base64::Engine as _;

use crate::broker::{PayloadEncoding, ValueView};

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
        Err(_) => ValueView::Str {
            total_bytes,
            shown_bytes,
            text: base64::engine::general_purpose::STANDARD.encode(&bytes),
            encoding: PayloadEncoding::Base64,
        },
    }
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
}
