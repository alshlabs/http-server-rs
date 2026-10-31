use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::BodyExt;
use http_body_util::Full;
use http_body_util::combinators::BoxBody;
use hyper::Request;
use hyper::Response;
use hyper::body::Bytes as HyperBytes;
use hyper::body::Incoming;
use hyper::http::response::Builder as ResponseBuilder;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::net::ToSocketAddrs;
use tokio::task::JoinSet;

/// How long connections get after shutdown to finish their response before they
/// are dropped, which closes their socket.
const SHUTDOWN_GRACE: Duration = Duration::from_millis(250);

/// Simple wrapper around hyper to make it a little nicer to use.
///
/// The returned future resolves once the listener stops and every in-flight
/// connection has drained or been closed.
pub async fn http1_server<F, Fut, A>(
  addr: A,
  shutdown: impl Future<Output = ()>,
  handle_func: F,
) -> anyhow::Result<()>
where
  A: ToSocketAddrs,
  F: 'static + Send + Sync + Fn(Request<Incoming>, ResponseBuilder) -> Fut,
  Fut: Send + Future<Output = anyhow::Result<Response<BoxBody<HyperBytes, std::io::Error>>>>,
{
  let listener = TcpListener::bind(&addr).await?;
  let handler_func_ref = Arc::new(handle_func);
  let mut connections = JoinSet::new();
  tokio::pin!(shutdown);

  loop {
    tokio::select! {
      _ = &mut shutdown => break,
      accepted = listener.accept() => {
        handle_accepted(accepted, &handler_func_ref, &mut connections)
      },
    }
  }

  // hyper-util's GracefulShutdown can't finish this on its own: for an in-flight
  // (mid-body) connection it only calls `disable_keep_alive()`, and hyper then
  // waits on a request that never arrives, so an open event stream hung the
  // process on Ctrl+C. Wait briefly for in-flight responses to finish, then drop
  // whatever is left, which closes its socket.
  let _ = tokio::time::timeout(SHUTDOWN_GRACE, async {
    while connections.join_next().await.is_some() {}
  })
  .await;

  connections.shutdown().await;

  Ok(())
}

fn handle_accepted<F, Fut>(
  accepted: std::io::Result<(TcpStream, SocketAddr)>,
  handler_func_ref: &Arc<F>,
  connections: &mut JoinSet<()>,
) where
  F: 'static + Send + Sync + Fn(Request<Incoming>, ResponseBuilder) -> Fut,
  Fut: Send + Future<Output = anyhow::Result<Response<BoxBody<HyperBytes, std::io::Error>>>>,
{
  let Ok((stream, _)) = accepted else {
    return;
  };
  let io = TokioIo::new(stream);
  let handler_func_ref = handler_func_ref.clone();

  let service_handler = service_fn(move |req| {
    let fut = handler_func_ref(req, Response::builder());

    async move {
      let handler_response = match fut.await {
        Ok(handler_response) => handler_response,
        Err(handler_error) => handle_error(handler_error),
      };

      Ok::<Response<BoxBody<HyperBytes, std::io::Error>>, anyhow::Error>(handler_response)
    }
  });

  let connection = http1::Builder::new().serve_connection(io, service_handler);

  connections.spawn(async move {
    let _ = connection.await;
  });
}

fn handle_error(error: anyhow::Error) -> Response<BoxBody<HyperBytes, std::io::Error>> {
  eprintln!("[500] {error:#}");
  let content = HyperBytes::from("Internal Server Error");
  let body = Full::new(content).map_err(std::io::Error::other).boxed();
  match Response::builder().status(500).body(body) {
    Ok(response) => response,
    Err(_) => Response::new(
      Full::new(HyperBytes::from_static(b"Internal Server Error"))
        .map_err(std::io::Error::other)
        .boxed(),
    ),
  }
}
