use axum::{
    Router,
    extract::{Query, State},
    http::status::StatusCode,
    response::Redirect,
    routing::get,
};
use color_eyre::Result;
use moka::future::{Cache, CacheBuilder};
use serde::Deserialize;
use std::{
    collections::HashSet,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Mutex as TokioMutex;
use tracing::{Instrument as _, instrument, level_filters::LevelFilter};

#[derive(Deserialize, Debug)]
struct DeviantartQuery {
    id: String,
}

fn compress_zstd(b: &[u8]) -> Vec<u8> {
    let mut out = vec![];
    zstd::stream::copy_encode(b, &mut out, 3).expect("it's in memory");
    out
}

fn decompress_zstd(b: &[u8]) -> Vec<u8> {
    let mut out = vec![];
    zstd::stream::copy_decode(b, &mut out).expect("it's in memory");
    out
}

#[instrument]
async fn fetch_deviantart_rss(id: &str) -> Result<String, FetchError> {
    let url = format!("https://backend.deviantart.com/rss.xml?q=gallery:{}", id);

    let response = match reqwest::get(&url).await {
        Ok(response) => response,
        Err(e) => {
            tracing::error!(error = ?e, url, "Failed sending request to url");
            return Err(FetchError::NetworkError);
        }
    };

    match response.error_for_status() {
        Ok(response) => {
            let bytes = match response.bytes().await {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::error!(error = ?e, url, "Failed downloading bytes from url");
                    return Err(FetchError::NetworkError);
                }
            };

            let s = match String::from_utf8(bytes.into()) {
                Ok(s) => {
                    // If someone puts this as their feed description then they deserve to not be
                    // followed
                    if s.contains("<description>Error generating RSS.</description>") {
                        tracing::error!(url, "Deviantart failed generating the feed");
                        return Err(FetchError::ServerFailed);
                    } else {
                        s
                    }
                }
                Err(e) => {
                    tracing::error!(error = ?e, url, "Failed converting bytes from url to UTF-8");
                    return Err(FetchError::NotUTF8);
                }
            };

            Ok(s)
        }
        Err(e) => match e.status().expect("this is from a response") {
            StatusCode::FORBIDDEN => {
                tracing::error!(url, "URL was blocked");
                Err(FetchError::NotAllowed)
            }
            code => {
                tracing::error!(?code, url, "Unknown error response");
                Err(FetchError::UnknownResponse)
            }
        },
    }
}

async fn fetch_deviantart_rss_with_timeout(
    id: &str,
    lock: Arc<TokioMutex<()>>,
    timeout: u16,
) -> Result<String, FetchError> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let id = id.to_string();

    let span = tracing::span!(
        tracing::Level::INFO,
        "fetch_deviantart_rss_with_timeout",
        id
    );

    tokio::spawn(
        async move {
            let guard = match lock.try_lock() {
                Ok(g) => g,
                Err(_) => {
                    async {
                        tracing::info!("Waiting for turn...");
                        let l = lock.lock().await;
                        tracing::info!("I got the turn");
                        l
                    }
                    .await
                }
            };

            tx.send(fetch_deviantart_rss(&id).await)
                .expect("the receiver shouldn't drop");

            tracing::info!("Waiting for {timeout} secs until ceding turn");
            tokio::time::sleep(Duration::from_secs(timeout.into())).await;
            tracing::info!("Ceding turn");
            drop(guard);
        }
        .instrument(span),
    );

    rx.await.expect("the sender shouldn't drop")
}

async fn deviantart_rss_handler(
    State(state): State<AppState>,
    Query(query): Query<DeviantartQuery>,
) -> Result<String, (StatusCode, String)> {
    let cache = state.deviantart_state.cache.clone();
    let global_lock = state.deviantart_state.global_lock.clone();
    let id = query.id.clone();

    let span = tracing::span!(tracing::Level::INFO, "deviantart_rss_handler", id);
    let _guard = span.enter();
    span.in_scope(|| tracing::info!("Getting rss"));

    // This task is spawned so that if this request is cancelled then
    // the task continues and the id is fetched anyways
    let coro_span = span.clone();
    let handle = tokio::spawn(async move {
        cache
            .get_with_by_ref(
                &id,
                async {
                    tracing::info!("Result not cached, need to hit the network");
                    let result = fetch_deviantart_rss_with_timeout(
                        &id,
                        global_lock,
                        state.config.deviantart_waiting_time,
                    )
                    .await;
                    tracing::info!("Finished fetching from network");

                    match result {
                        Ok(_) => tracing::info!("Got result from server"),
                        Err(FetchError::NotAllowed) => tracing::info!("Got blocked by server"),
                        Err(e) => tracing::info!(?e, "An error ocurred"),
                    };

                    match result {
                        Ok(_) | Err(FetchError::NotAllowed) => {
                            state
                                .deviantart_state
                                .fetch_ids
                                .lock()
                                .expect("this shouldn't be poisoned")
                                .insert(id.clone());
                        }
                        _ => {}
                    }

                    Arc::new(result.map(|s| compress_zstd(&s.into_bytes())))
                }
                .instrument(coro_span),
            )
            .await
    });

    let cached = handle.await.expect("can join thread");
    tracing::debug!(id = query.id, "Got result from cache for rss");

    match *cached {
        Ok(ref cached) => {
            let bytes = decompress_zstd(cached);
            let s = String::from_utf8(bytes).expect("we checked it was UTF-8 before putting it in");
            Ok(s)
        }
        Err(FetchError::NotAllowed) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Try again later".to_string(),
        )),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            format!("Request failed for some reason: {:?}", e),
        )),
    }
}

#[derive(Copy, Clone, Debug)]
enum FetchError {
    NotAllowed,
    NetworkError,
    NotUTF8,
    ServerFailed,
    UnknownResponse,
}

#[derive(Clone, Debug)]
struct DeviantartState {
    cache: Cache<String, Arc<Result<Vec<u8>, FetchError>>>,
    fetch_ids: Arc<Mutex<HashSet<String>>>,
    global_lock: Arc<TokioMutex<()>>,
}

#[derive(Clone, Debug)]
struct AppState {
    deviantart_state: DeviantartState,
    config: Arc<AppConfig>,
}

fn spawn_refresh(state: AppState) {
    // Every `n` minutes, we refetch keys that were successfully fetched before
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_mins(10)).await;

            let ids = state
                .deviantart_state
                .fetch_ids
                .lock()
                .expect("shouldn't be poisoned")
                .clone();
            if ids.is_empty() {
                continue;
            }

            let span = tracing::span!(tracing::Level::INFO, "automatic-refresh");
            span.in_scope(|| tracing::info!("Starting automatic refresh of {} ids", ids.len()));
            for id in ids.into_iter() {
                let id_span = tracing::span!(parent: &span, tracing::Level::INFO, "automatic-refresh-for-id", id);
                state
                    .deviantart_state
                    .cache
                    .get_with_by_ref(
                        &id,
                        async {
                            tracing::info!("Re-fetching rss");

                            let result = fetch_deviantart_rss_with_timeout(
                                &id,
                                state.deviantart_state.global_lock.clone(),
                                state.config.deviantart_waiting_time,
                            )
                            .instrument(id_span.clone())
                            .await;

                            match result {
                                Ok(_) => tracing::info!("Got result from server"),
                                Err(FetchError::NotAllowed) => {
                                    tracing::warn!("Got blocked by server")
                                }
                                Err(e) => tracing::error!(?e, "An error ocurred"),
                            };

                            Arc::new(result.map(|s| compress_zstd(&s.into_bytes())))
                        }
                        .instrument(id_span.clone()),
                    )
                    .await;
            }
            tracing::info!("Finished automatic refresh of ids");
        }
    });
}

fn spawn_refresh_blocked(state: AppState) {
    // Every `n` minutes, we retry on keys that were blocked before
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_mins(5)).await;

            let ids = state
                .deviantart_state
                .fetch_ids
                .lock()
                .expect("shouldn't be poisoned")
                .clone();
            if ids.is_empty() {
                continue;
            }

            let mut did_fetch = false;
            for id in ids.into_iter() {
                let should_fetch = match state
                    .deviantart_state
                    .cache
                    .get(&id)
                    .await
                    .as_ref()
                    .map(|res| res.as_ref())
                {
                    Some(Ok(_)) => false,
                    Some(Err(FetchError::NotAllowed)) => true,
                    Some(Err(_)) => true,
                    None => false,
                };

                if !should_fetch {
                    continue;
                }

                // We need to invalidate this because we need to run `get_with_by_ref`, and that
                // function won't evaluate the future if the key is already there. We need to run
                // `get_with_by_ref` because we want this fetch to coalesce with the
                // `get_with_by_ref` calls elsewhere to avoid hammering the deviantart server
                state.deviantart_state.cache.invalidate(&id).await;

                let span = tracing::span!(tracing::Level::INFO, "blocked-id-refresh", id);
                span.in_scope(|| tracing::info!("Starting automatic refresh of blocked id"));
                did_fetch = true;
                state
                    .deviantart_state
                    .cache
                    .get_with_by_ref(&id, async {
                        tracing::info!(id, "Re-fetching blocked rss");
                        let result = fetch_deviantart_rss_with_timeout(
                            &id,
                            state.deviantart_state.global_lock.clone(),
                            state.config.deviantart_waiting_time,
                        )
                        .instrument(span.clone())
                        .await;

                        span.in_scope(|| match result {
                            Ok(_) => tracing::info!("Got result from server"),
                            Err(FetchError::NotAllowed) => tracing::info!("Got blocked by server"),
                            Err(e) => tracing::info!(?e, "An error ocurred"),
                        });

                        Arc::new(result.map(|s| compress_zstd(&s.into_bytes())))
                    })
                    .await;
            }
            if did_fetch {
                tracing::info!("Finished automatic refresh of blocked ids");
            }
        }
    });
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
        deviantart_state: DeviantartState {
            cache: CacheBuilder::new(300)
                .time_to_live(Duration::from_mins(30))
                .build(),
            global_lock: Default::default(),
            fetch_ids: Default::default(),
        },
    };

    spawn_refresh(state.clone());
    spawn_refresh_blocked(state.clone());

    let app = Router::new()
        .route("/", get(|| async { Redirect::permanent("/browse/") }))
        .route("/deviantart", get(deviantart_rss_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind((config.bind_address, config.bind_port))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
