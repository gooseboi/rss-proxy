use moka::future::{Cache, CacheBuilder};
use reqwest::StatusCode;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Mutex as TokioMutex;
use tracing::{Instrument as _, instrument};

use crate::AppConfig;
use crate::utils;

#[derive(Clone, Debug)]
pub struct DeviantartState {
    pub cache: Cache<String, Arc<Result<Vec<u8>, FetchError>>>,
    pub fetch_ids: Arc<Mutex<HashSet<String>>>,
    pub global_lock: Arc<TokioMutex<()>>,
}

impl From<AppConfig> for DeviantartState {
    fn from(config: AppConfig) -> Self {
        DeviantartState {
            cache: CacheBuilder::new(config.deviantart_max_entries)
                .time_to_live(Duration::from_mins(config.deviantart_cache_ttl))
                .build(),
            global_lock: Default::default(),
            fetch_ids: Default::default(),
        }
    }
}

pub async fn fetch_deviantart_rss_with_timeout(
    id: &str,
    lock: Arc<TokioMutex<()>>,
    timeout: u64,
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
            tokio::time::sleep(Duration::from_secs(timeout)).await;
            tracing::info!("Ceding turn");
            drop(guard);
        }
        .instrument(span),
    );

    rx.await.expect("the sender shouldn't drop")
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

#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub enum FetchError {
    NotAllowed,
    NetworkError,
    NotUTF8,
    ServerFailed,
    UnknownResponse,
}

pub fn spawn_refresh_tasks(state: &DeviantartState, config: &AppConfig) {
    spawn_refresh(state, config);
    spawn_refresh_blocked(state, config);
}

fn spawn_refresh(state: &DeviantartState, config: &AppConfig) {
    let state = state.clone();
    let config = config.clone();

    // Every `n` minutes, we refetch keys that were successfully fetched before
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_mins(10)).await;

            let ids = state
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
                    .cache
                    .get_with_by_ref(
                        &id,
                        async {
                            tracing::info!("Re-fetching rss");

                            let result = fetch_deviantart_rss_with_timeout(
                                &id,
                                state.global_lock.clone(),
                                config.deviantart_waiting_time,
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

                            Arc::new(result.map(|s| utils::compress_zstd(&s.into_bytes())))
                        }
                        .instrument(id_span.clone()),
                    )
                    .await;
            }
            tracing::info!("Finished automatic refresh of ids");
        }
    });
}

fn spawn_refresh_blocked(state: &DeviantartState, config: &AppConfig) {
    let state = state.clone();
    let config = config.clone();

    // Every `n` minutes, we retry on keys that were blocked before
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_mins(5)).await;

            let ids = state
                .fetch_ids
                .lock()
                .expect("shouldn't be poisoned")
                .clone();
            if ids.is_empty() {
                continue;
            }

            let mut did_fetch = false;
            for id in ids.into_iter() {
                let should_fetch = match state.cache.get(&id).await.as_ref().map(|res| res.as_ref())
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
                state.cache.invalidate(&id).await;

                let span = tracing::span!(tracing::Level::INFO, "blocked-id-refresh", id);
                span.in_scope(|| tracing::info!("Starting automatic refresh of blocked id"));
                did_fetch = true;
                state
                    .cache
                    .get_with_by_ref(&id, async {
                        tracing::info!(id, "Re-fetching blocked rss");
                        let result = fetch_deviantart_rss_with_timeout(
                            &id,
                            state.global_lock.clone(),
                            config.deviantart_waiting_time,
                        )
                        .instrument(span.clone())
                        .await;

                        span.in_scope(|| match result {
                            Ok(_) => tracing::info!("Got result from server"),
                            Err(FetchError::NotAllowed) => {
                                tracing::info!("Got blocked by server")
                            }
                            Err(e) => tracing::info!(?e, "An error ocurred"),
                        });

                        Arc::new(result.map(|s| utils::compress_zstd(&s.into_bytes())))
                    })
                    .await;
            }
            if did_fetch {
                tracing::info!("Finished automatic refresh of blocked ids");
            }
        }
    });
}

pub fn get_stats(state: DeviantartState) -> String {
    let mut out = String::new();
    out.push_str("<div>");
    out.push_str("<h2>Fetch ids</h2>");
    out.push_str("<ul>");
    let lock = state.fetch_ids.lock().expect("lock shouldn't be poisoned");
    for id in lock.iter() {
        out.push_str(&format!("<li>{id}</li>"));
    }
    drop(lock);
    out.push_str("</ul>");

    let results = state.cache.iter().collect::<Vec<_>>();

    out.push_str("<h2>Values</h2>");
    out.push_str("<h3>Success</h3>");
    out.push_str("<ul>");
    for (k, _) in results.iter().filter(|(_, v)| v.is_ok()) {
        out.push_str(&format!("<li>{k}</li>"));
    }
    out.push_str("</ul>");

    out.push_str("<h3>Blocked</h3>");
    out.push_str("<ul>");
    for (k, _) in results.iter().filter(|(_, v)| {
        v.as_ref()
            .as_ref()
            .is_err_and(|e| *e == FetchError::NotAllowed)
    }) {
        out.push_str(&format!("<li>{k}</li>"));
    }
    out.push_str("</ul>");

    out.push_str("<h3>Error</h3>");
    out.push_str("<table>");

    out.push_str("<thead>");
    out.push_str("<tr>");
    out.push_str("<th>ID</th>");
    out.push_str("<th>Error</th>");
    out.push_str("</tr>");
    out.push_str("</thead>");

    out.push_str("<tbody>");
    for (k, v) in results.iter().filter(|(_, v)| {
        v.as_ref()
            .as_ref()
            .is_err_and(|e| *e != FetchError::NotAllowed)
    }) {
        let Err(v) = v.as_ref() else { unreachable!() };
        out.push_str("<tr>");
        out.push_str(&format!("<td>{k}</td>"));
        out.push_str(&format!("<td>{v:?}</td>"));
        out.push_str("<td>Error</td>");
        out.push_str("</tr>");
    }
    out.push_str("</tbody>");

    out.push_str("</table>");

    out.push_str("</div>");

    out
}
