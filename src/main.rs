use axum::{Router, response::Redirect, routing::get};
use color_eyre::Result;
use std::{net::IpAddr, sync::Arc};

use crate::deviantart::DeviantartState;
use tracing::level_filters::LevelFilter;

mod deviantart;
mod handlers;
mod utils;

#[derive(Clone, Debug)]
struct AppState {
    deviantart_state: DeviantartState,
    config: Arc<AppConfig>,
}

#[derive(Debug, Clone)]
struct AppConfig {
    bind_address: IpAddr,
    bind_port: u16,
    deviantart_waiting_time: u16,
}

impl AppConfig {
    fn parse() -> Self {
        let name = "RSS_PROXY_ADDR";
        let addr = std::env::var(name)
            .unwrap_or_else(|_| panic!("{name} must be a valid env variable"))
            .parse()
            .expect("valid ip address");

        let name = "RSS_PROXY_PORT";
        let port = std::env::var(name)
            .unwrap_or_else(|_| panic!("{name} must be a valid env variable"))
            .parse()
            .expect("port must be a number");

        let name = "RSS_DEVIANTART_WAITING_TIME";
        let waiting_time = match std::env::var(name) {
            Ok(v) => v,
            Err(std::env::VarError::NotPresent) => "10".to_string(),
            Err(_) => panic!("expected valid environment variable for {name}"),
        }
        .parse()
        .expect("waiting time must be a number");

        Self {
            deviantart_waiting_time: waiting_time,
            bind_port: port,
            bind_address: addr,
        }
    }
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
