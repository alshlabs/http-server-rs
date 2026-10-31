mod config;
mod constants;
mod handler;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use colored::Colorize;
use grass::OutputStyle;

use self::config::Config;
use self::handler::ServeState;
use crate::platform::http1::http1_server;
use crate::platform::ip_address;
use crate::platform::ip_address::DomainOptions;
use crate::platform::logger::Logger;
use crate::platform::proxy::build_client;
use crate::platform::self_upgrade;
use crate::platform::self_upgrade::TARGET_REPO;
use crate::platform::self_upgrade::UpgradeOptions;
use crate::platform::shutdown::shutdown_signal;
use crate::platform::transpile::scss::ScssTransformer;
use crate::platform::transpile::typescript::TypeScriptTransformer;
use crate::platform::watcher::Watcher;
use crate::platform::watcher::WatcherOptions;

#[derive(Debug, Parser)]
pub struct ServeCommand {
  /// Target directory to serve
  #[arg(default_value = ".")]
  pub serve_dir: PathBuf,

  #[arg(short = 'a', long = "address", default_value = "0.0.0.0")]
  pub address: String,

  #[arg(short = 'p', long = "port", default_value = "8080")]
  pub port: usize,

  /// Redirect requests to /index.html for Single Page Applications
  #[arg(long = "spa")]
  pub spa: bool,

  /// Cache control time
  #[arg(short = 'c', long = "cache-time", default_value = "0")]
  pub cache_time: usize,

  /// Compress responses (JIT)
  #[arg(short = 'Z', long = "compress")]
  pub compress: bool,

  /// Custom headers (Format "key:value")
  #[arg(short = 'H', long = "header")]
  pub headers: Vec<String>,

  /// Put server behind basic auth (Format "username:password")
  #[arg(long = "auth")]
  pub basic_auth: Vec<String>,

  /// Enable CORS header
  #[arg(long = "cors")]
  pub cors: bool,

  /// Enable headers for SharedArrayBuffer
  #[arg(short = 'S', long = "shared-array-buffer")]
  pub sab: bool,

  /// Don't print any logs to terminal
  #[arg(short = 'Q', long = "quiet")]
  pub quiet: bool,

  /// Transpile served files
  #[arg(short = 't', long = "transpile")]
  pub transpile: bool,

  /// Don't generate source maps for transpiled files
  #[arg(long = "no-source-maps")]
  pub no_source_maps: bool,

  /// Number of worker threads used for transpilation [default: available parallelism]
  #[arg(long = "transpile-worker-threads")]
  pub transpile_worker_threads: Option<usize>,

  /// Watch folder for changes and trigger a browser reload
  #[arg(short = 'w', long = "watch")]
  pub watch: bool,

  /// Watch for changes [default: SERVE_DIR]
  #[arg(long = "watch-dir")]
  pub watch_dir: Option<PathBuf>,

  /// Debounce delay in milliseconds before watched changes trigger a reload
  #[arg(long = "watch-debounce-delay", default_value = "200")]
  pub watch_debounce_delay: u64,

  /// Don't automatically inject watch listener into html
  #[arg(long = "no-watch-inject")]
  pub no_watch_inject: bool,

  /// Skip checking for a newer release on startup
  #[arg(long = "skip-upgrade-check", default_value_t = false)]
  pub skip_upgrade_check: bool,

  /// Configure the buffer size when streaming files
  #[arg(long = "stream-buffer-size", default_value = "65536")]
  pub stream_buffer_size: usize,

  /// Reverse proxy routes (URL-encoded, e.g.
  /// "path=/api&target=http://localhost:3000&secure=false&change_origin=false&path_rewrite[key]=^/api&path_rewrite[value]=")
  #[arg(long = "proxy")]
  pub proxy: Vec<String>,
}

pub async fn main(args: ServeCommand) -> anyhow::Result<()> {
  let config = Arc::new(Config::from_args(args)?);

  let logger: Arc<Logger> = match config.quiet {
    true => Arc::new(Logger::Quiet),
    false => Arc::new(Logger::Default),
  };

  if !config.quiet && !config.skip_upgrade_check {
    tokio::spawn({
      let version = config.version.clone();
      async move {
        let available = self_upgrade::check_for_update(&UpgradeOptions {
          target_repo: TARGET_REPO,
          current_version: &version,
        })
        .await;

        if let Ok(Some(latest)) = available {
          println!(
            "⚡ Update available: {} -> {}  (run `http-server-rs update`)",
            version, latest
          );
        }
      }
    });
  }

  logger.println(
    format!("🚀 HTTP Server 🌏 v{}", config.version)
      .green()
      .bold()
      .to_string(),
  );
  logger.br();

  logger.print_folder(&config.serve_dir_fmt);
  logger.print_config("Directory Listings", &true);
  logger.print_config("Compress (JIT)", &config.compress);
  logger.print_config("CORS", &config.cors);
  logger.print_config("SharedArrayBuffer", &config.sab);
  logger.print_config("SPA", &config.spa);
  logger.print_config("Watch", &config.watch);
  logger.print_config("Transpile", &config.transpile);
  logger.print_config("Source Maps", &config.source_maps);
  logger.br();

  if !config.proxy.is_empty() {
    for (path, route) in config.proxy.iter() {
      logger.println(format!(
        "🔀 {:<19} {} -> {}",
        format!("{}:", path).bold(),
        path,
        route.target
      ));
    }
    logger.br();
  }

  logger.print_headers(&config.headers);
  logger.br();

  logger.println(format!("🔗 http://{}", config.domain));
  if config.domain != config.domain_pretty {
    logger.println(format!("🔗 http://{}", config.domain_pretty));
  }

  if let Some(intranet_domain) = ip_address::get_domain(&DomainOptions {
    domain: &config.domain,
    domain_pretty: &config.domain_pretty,
    port: config.port,
  }) {
    logger.println(format!("🔗 http://{}", intranet_domain));
  }

  logger.br();

  logger.println("📜 LOGS 📜".bold().blue().to_string());

  let watcher = match config.watch {
    true => Some(Watcher::new(WatcherOptions {
      target_dir: config.watch_dir.clone(),
      logger: logger.clone(),
      debounce_delay: config.watch_debounce_delay,
    })?),
    false => None,
  };

  let proxy_client = match config.proxy.is_empty() {
    true => None,
    false => Some(Arc::new(build_client(&config.proxy)?)),
  };

  let worker_threads = config
    .transpile_worker_threads
    .unwrap_or_else(|| {
      std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
    })
    .max(1);

  // Resolve symlinks once so the handler's boundary check can't be escaped by one
  let serve_dir_canonical = tokio::fs::canonicalize(&config.serve_dir_abs).await?;

  let state = Arc::new(ServeState {
    config: config.clone(),
    serve_dir_canonical,
    logger: logger.clone(),
    watcher,
    proxy_client,
    scss_transformer: Arc::new(ScssTransformer::new(
      &config.serve_dir_abs,
      OutputStyle::Expanded,
      worker_threads,
    )),
    typescript_transformer: Arc::new(TypeScriptTransformer::new(
      &config.serve_dir_abs,
      worker_threads,
    )),
  });

  http1_server(&config.domain, shutdown_signal(), {
    let state = state.clone();

    move |req, res| {
      let state = state.clone();
      handler::handle(req, res, state)
    }
  })
  .await?;
  Ok(())
}
