use axum::{Router, response::Redirect, routing::get};
use color_eyre::Result;
use std::{net::IpAddr, sync::Arc};

use crate::deviantart::DeviantartState;
use tracing::level_filters::LevelFilter;
use clap::Parser as _;

mod deviantart;
mod handlers;
mod utils;

#[derive(Clone, Debug)]
struct AppState {
    deviantart_state: DeviantartState,
    config: Arc<AppConfig>,
}

#[derive(clap::Parser, Debug, Clone)]
#[command(author, version, about)]
pub struct AppConfig {
    /// Address to bind
    #[arg(long = "addr", env = "RSS_PROXY_ADDR")]
    bind_address: IpAddr,

    /// Port to bind
    #[arg(env = "RSS_PROXY_PORT")]
    bind_port: u16,

    /// DeviantArt: Time to wait between two requests (in seconds)
    #[arg(env = "RSS_PROXY_DEVIANTART_WAITING_TIME", default_value = "10")]
    deviantart_waiting_time: u64,

    /// DeviantArt: Time for a single (succesful) request to live in the cache (in minutes)
    #[arg(env = "RSS_PROXY_DEVIANTART_CACHE_TTL", default_value = "30")]
    deviantart_cache_ttl: u64,

    /// DeviantArt: Maximum amount of entries to keep at one time
    #[arg(env = "RSS_PROXY_DEVIANTART_MAX_ENTRIES", default_value = "300")]
    deviantart_max_entries: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .with_env_var("RSS_LOG")
                .from_env_lossy(),
        )
        .init();
    color_eyre::install()?;

    let config = AppConfig::parse();

    let state = AppState {
        config: Arc::new(config.clone()),
        deviantart_state: config.clone().into(),
    };

    deviantart::spawn_refresh_tasks(&state.deviantart_state, &config);

    let app = Router::new()
        .route("/", get(|| async { Redirect::permanent("/browse/") }))
        .route("/deviantart", get(handlers::deviantart_rss_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind((config.bind_address, config.bind_port))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
