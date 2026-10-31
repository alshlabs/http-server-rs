use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use http::Response as HttpResponse;
use http::Result as HttpResult;
use http_body_util::StreamBody;
use http_body_util::combinators::BoxBody;
use hyper::body::Bytes as HyperBytes;
use hyper::body::Frame;
use hyper::http::response::Builder as ResponseBuilder;
use tokio::io::DuplexStream;

use super::Bytes;
use super::ShutdownSignal;

pub trait ResponseBuilderExt {
  fn body_stream(
    self,
    stream_buffer_size: usize,
  ) -> HttpResult<(
    HttpResponse<BoxBody<HyperBytes, std::io::Error>>,
    DuplexStream,
  )>;
  /// Stream body that ends as soon as shutdown is triggered, so a held-open
  /// stream (e.g. an event stream) can't stall graceful shutdown.
  fn body_stream_until_shutdown(
    self,
    stream_buffer_size: usize,
    shutdown_signal: ShutdownSignal,
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

  fn body_stream_until_shutdown(
    self,
    stream_buffer_size: usize,
    shutdown_signal: ShutdownSignal,
  ) -> HttpResult<(
    HttpResponse<BoxBody<HyperBytes, std::io::Error>>,
    DuplexStream,
  )> {
    let (writer, reader) = tokio::io::duplex(stream_buffer_size);
    let (_tx_kill, rx_kill) = tokio::sync::oneshot::channel::<()>();

    let reader_stream = tokio_util::io::ReaderStream::new(reader)
      .map_ok(Frame::data)
      .map_err(std::io::Error::other)
      // Detatched from the writer so it ends on shutdown even while the writer
      // side is still held open by a request task
      .take_until(async move {
        let _ = rx_kill.await;
        shutdown_signal.wait_owned().await;
      })
      // hyper treats an empty DATA frame as the end of stream
      .chain(stream::once(async { Ok(Frame::data(HyperBytes::new())) }));

    let bodystream = StreamBody::new(reader_stream);
    let boxed_body: BoxBody<HyperBytes, std::io::Error> =
      BoxBody::<HyperBytes, std::io::Error>::new(bodystream);

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
