/// The content codings the server can produce, in preference order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
  Brotli,
  Gzip,
}

impl Encoding {
  pub fn name(self) -> &'static str {
    match self {
      Encoding::Brotli => "br",
      Encoding::Gzip => "gzip",
    }
  }
}

/// Pick the most preferred supported coding from an `Accept-Encoding` header.
pub fn negotiate(accept_encoding: Option<&str>) -> Option<Encoding> {
  let header = accept_encoding?;

  let mut best: Option<(Encoding, f32)> = None;

  for encoding in [Encoding::Brotli, Encoding::Gzip] {
    let Some(quality) = quality(header, encoding.name()) else {
      continue;
    };

    if quality <= 0.0 {
      continue;
    }

    match best {
      Some((_, best_quality)) if best_quality >= quality => {}
      _ => best = Some((encoding, quality)),
    }
  }

  best.map(|(encoding, _)| encoding)
}

fn quality(
  header: &str,
  name: &str,
) -> Option<f32> {
  let mut wildcard = None;

  for part in header.split(',') {
    let mut params = part.split(';');
    let token = params.next().unwrap_or_default().trim();

    let quality = params
      .find_map(|param| param.trim().strip_prefix("q=")?.trim().parse::<f32>().ok())
      .unwrap_or(1.0);

    if token.eq_ignore_ascii_case(name) {
      return Some(quality);
    }

    if token == "*" {
      wildcard = Some(quality);
    }
  }

  wildcard
}
