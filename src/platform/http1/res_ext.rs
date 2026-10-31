use futures::TryStreamExt;
use http::Response as HttpResponse;
use http::Result as HttpResult;
use http_body_util::StreamBody;
use http_body_util::combinators::BoxBody;
use hyper::body::Bytes as HyperBytes;
use hyper::http::response::Builder as ResponseBuilder;
use tokio::io::DuplexStream;

use super::Bytes;

pub trait ResponseBuilderExt {
  fn body_stream(
    self,
    stream_buffer_size: usize,
  ) -> HttpResult<(
    HttpResponse<BoxBody<HyperBytes, std::io::Error>>,
    DuplexStream,
  )>;
  fn body_from(
    self,
    bytes: impl Into<Bytes>,
  ) -> HttpResult<HttpResponse<BoxBody<HyperBytes, std::io::Error>>>;
}

impl ResponseBuilderExt for ResponseBuilder {
  fn body_stream(
    self,
    stream_buffer_size: usize,
  ) -> HttpResult<(
    HttpResponse<BoxBody<HyperBytes, std::io::Error>>,
    DuplexStream,
  )> {
    let (writer, reader) = tokio::io::duplex(stream_buffer_size);

    let reader_stream = tokio_util::io::ReaderStream::new(reader)
      .map_ok(hyper::body::Frame::data)
      .map_err(std::io::Error::other);

    let stream_body = StreamBody::new(reader_stream);
    let boxed_body: BoxBody<HyperBytes, std::io::Error> =
      BoxBody::<HyperBytes, std::io::Error>::new(stream_body);

    let res: http::Response<BoxBody<HyperBytes, std::io::Error>> = self.body(boxed_body)?;

    Ok((res, writer))
  }

  fn body_from(
    self,
    bytes: impl Into<Bytes>,
  ) -> HttpResult<HttpResponse<BoxBody<HyperBytes, std::io::Error>>> {
    self.body(bytes.into().into())
  }
}
