use super::Encoding;
use super::negotiate;

fn accepted(header: &str) -> Option<&'static str> {
  negotiate(Some(header)).map(Encoding::name)
}

#[test]
fn prefers_brotli_when_accepted() {
  assert_eq!(accepted("gzip, deflate, br"), Some("br"));
}

#[test]
fn honours_quality_values() {
  assert_eq!(accepted("br;q=0.5, gzip"), Some("gzip"));
}

#[test]
fn falls_back_to_gzip() {
  assert_eq!(accepted("gzip"), Some("gzip"));
}

#[test]
fn returns_none_when_nothing_supported() {
  assert_eq!(accepted("deflate"), None);
  assert_eq!(accepted("br;q=0"), None);
  assert_eq!(accepted("identity"), None);
  assert_eq!(negotiate(None), None);
}

#[test]
fn honours_wildcard() {
  assert_eq!(accepted("*"), Some("br"));
  assert_eq!(accepted("br;q=0, *"), Some("gzip"));
}
