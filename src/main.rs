use axum::{Router, routing::get};
use clap::Parser as _;
use color_eyre::Result;
use std::{net::IpAddr, sync::Arc};
use tracing::level_filters::LevelFilter;

use crate::deviantart::{DeviantartState, DeviantartConfig};

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
    #[arg(long, value_name = "RSS_PROXY_ADDR", env = "RSS_PROXY_ADDR")]
    bind_address: IpAddr,

    /// Port to bind
    #[arg(long, value_name = "RSS_PROXY_PORT", env = "RSS_PROXY_PORT")]
    bind_port: u16,

    /// Deviantart configuration
    #[command(flatten)]
    deviantart_config: DeviantartConfig,
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
        .route("/version", get(handlers::version_handler))
        .route("/stats", get(handlers::stats_handler))
        .route("/deviantart", get(handlers::deviantart_rss_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind((config.bind_address, config.bind_port))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
