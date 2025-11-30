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
use std::sync::Mutex;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::Mutex as TokioMutex;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

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

    tokio::spawn(async move {
        let guard = lock.lock().await;

        tx.send(fetch_deviantart_rss(&id).await)
            .expect("the receiver shouldn't drop");

        tokio::time::sleep(Duration::from_secs(timeout.into())).await;
        drop(guard);
    });

    rx.await.expect("the sender shouldn't drop")
}

#[axum::debug_handler]
async fn deviantart_rss_handler(
    State(state): State<AppState>,
    Query(query): Query<DeviantartQuery>,
) -> Result<String, (StatusCode, String)> {
    let cache = state.deviantart_state.cache.clone();
    let global_lock = state.deviantart_state.global_lock.clone();
    let id = query.id.clone();

    tracing::info!(id, "Getting rss for id");

    // This task is spawned so that if this request is cancelled then
    // the task continues and the id is fetched anyways
    let handle = tokio::spawn(async move {
        cache
            .get_with_by_ref(&id, async {
                tracing::info!(id, "Need to hit the network for rss");
                let result = fetch_deviantart_rss_with_timeout(&id, global_lock, 10).await;
                tracing::info!(id, "Got result for rss");

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
            })
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

            tracing::info!("Starting automatic refresh of {} ids", ids.len());
            for id in ids.into_iter() {
                state
                    .deviantart_state
                    .cache
                    .get_with_by_ref(&id, async {
                        tracing::info!(id, "Re-fetching rss");
                        let result = fetch_deviantart_rss_with_timeout(
                            &id,
                            state.deviantart_state.global_lock.clone(),
                            10,
                        )
                        .await;
                        tracing::info!(id, "Got result for rss");
                        Arc::new(result.map(|s| compress_zstd(&s.into_bytes())))
                    })
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

                tracing::info!(id, "Starting automatic refresh of blocked id");
                did_fetch = true;
                state
                    .deviantart_state
                    .cache
                    .get_with_by_ref(&id, async {
                        tracing::info!(id, "Re-fetching rss");
                        let result = fetch_deviantart_rss_with_timeout(
                            &id,
                            state.deviantart_state.global_lock.clone(),
                            10,
                        )
                        .await;
                        tracing::info!(id, "Got result for rss");
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "deviantart_rss=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    color_eyre::install()?;

    let state = AppState {
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

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
