use moka::future::{Cache, CacheBuilder};
use reqwest::StatusCode;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{Mutex, RwLock};
use tracing::{Instrument as _, instrument};

use crate::AppConfig;
use crate::utils;
use std::time::Instant;

#[derive(clap::Parser, Debug, Clone)]
pub struct DeviantartConfig {
    /// DeviantArt: Time to wait between two requests (in seconds)
    #[arg(long, env = "RSS_PROXY_DEVIANTART_WAITING_TIME", default_value = "10")]
    pub deviantart_waiting_time: u64,

    /// DeviantArt: Time for a single (succesful) request to live in the cache (in minutes)
    #[arg(long, env = "RSS_PROXY_DEVIANTART_CACHE_TTL", default_value = "30")]
    pub deviantart_cache_ttl: u64,

    /// DeviantArt: Maximum amount of entries to keep at one time
    #[arg(long, env = "RSS_PROXY_DEVIANTART_MAX_ENTRIES", default_value = "300")]
    pub deviantart_max_entries: u64,
}

#[derive(Clone, Debug)]
pub struct CacheEntry {
    pub response: Result<Vec<u8>, FetchError>,
    pub response_at: Instant,
}

#[derive(Clone, Debug)]
pub struct DeviantartState {
    pub cache_time: Duration,
    pub cache: Cache<String, Arc<CacheEntry>>,
    pub fetch_ids: Arc<RwLock<HashSet<Arc<str>>>>,
    pub global_lock: Arc<Mutex<()>>,
}

impl From<AppConfig> for DeviantartState {
    fn from(config: AppConfig) -> Self {
        let cache_time = Duration::from_mins(config.deviantart_config.deviantart_cache_ttl);
        DeviantartState {
            cache_time,
            cache: CacheBuilder::new(config.deviantart_config.deviantart_max_entries).build(),
            global_lock: Default::default(),
            fetch_ids: Default::default(),
        }
    }
}

pub async fn fetch_deviantart_rss_with_timeout(
    id: &str,
    lock: Arc<Mutex<()>>,
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

            let ids = state.fetch_ids.read().await;
            if ids.is_empty() {
                continue;
            }

            let span = tracing::span!(tracing::Level::INFO, "automatic-refresh");
            span.in_scope(|| tracing::info!("Starting automatic refresh of {} ids", ids.len()));
            for id in ids.iter() {
                let id_span = tracing::span!(parent: &span, tracing::Level::INFO, "automatic-refresh-for-id", id = id.as_ref());
                let val = state.cache.get(id.as_ref()).await;

                if let Some(val) = val
                    && val.response_at.elapsed() <= state.cache_time
                {
                    continue;
                }

                // Remove and then run get_with_by_ref to coalesce with
                // a possible fetch at the same time
                state.cache.remove(id.as_ref()).await;
                state
                    .cache
                    .get_with_by_ref(
                        id.as_ref(),
                        async {
                            tracing::info!("Re-fetching rss");

                            let result = fetch_deviantart_rss_with_timeout(
                                id.as_ref(),
                                state.global_lock.clone(),
                                config.deviantart_config.deviantart_waiting_time,
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

                            let now = Instant::now();
                            Arc::new(CacheEntry {
                                response: result.map(|s| utils::compress_zstd(&s.into_bytes())),
                                response_at: now,
                            })
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

            let ids = state.fetch_ids.read().await;
            if ids.is_empty() {
                continue;
            }

            let mut did_fetch = false;
            for id in ids.iter() {
                let id = id.as_ref();
                let should_fetch = match state
                    .cache
                    .get(id)
                    .await
                    .as_ref()
                    .map(|res| &res.as_ref().response)
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
                state.cache.invalidate(id).await;

                let span = tracing::span!(tracing::Level::INFO, "blocked-id-refresh", id);
                span.in_scope(|| tracing::info!("Starting automatic refresh of blocked id"));
                did_fetch = true;
                state
                    .cache
                    .get_with_by_ref(id, async {
                        tracing::info!(id, "Re-fetching blocked rss");
                        let result = fetch_deviantart_rss_with_timeout(
                            id,
                            state.global_lock.clone(),
                            config.deviantart_config.deviantart_waiting_time,
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

                        let now = Instant::now();
                        Arc::new(CacheEntry {
                            response: result.map(|s| utils::compress_zstd(&s.into_bytes())),
                            response_at: now,
                        })
                    })
                    .await;
            }
            if did_fetch {
                tracing::info!("Finished automatic refresh of blocked ids");
            }
        }
    });
}

async fn cache_value_with_timeout(
    cache: Cache<String, Arc<CacheEntry>>,
    id: &str,
) -> Option<Option<Arc<CacheEntry>>> {
    tokio::select! {
        val = cache.get(id) => {
            Some(val)
        }
        _ = tokio::time::sleep(Duration::from_millis(50)) => {
            None
        }
    }
}

pub async fn get_stats(state: DeviantartState) -> String {
    let mut out = String::new();
    out.push_str("<div>");

    let lock = state.fetch_ids.read().await;

    out.push_str("<h2>Automatically fetched</h2>");
    if !lock.is_empty() {
        out.push_str("<table>");
        out.push_str("<thead>");
        out.push_str("<tr>");
        out.push_str("<th>ID</th>");
        out.push_str("<th>Status</th>");
        out.push_str("<th>Remaining time</th>");
        out.push_str("</tr>");
        out.push_str("</thead>");
        out.push_str("<tbody>");
        for id in lock.iter() {
            out.push_str("<tr>");
            out.push_str(&format!("<td>{id}</td>"));
            let value = cache_value_with_timeout(state.cache.clone(), id).await;
            let mut did_time_output = false;
            match value.as_ref().map(|v| v.as_ref().map(|res| res.as_ref())) {
                Some(Some(CacheEntry {
                    response: Ok(_),
                    response_at,
                })) => {
                    out.push_str("<td>Ok</td>");
                    let elapsed = response_at.elapsed();
                    let remaining = state.cache_time.abs_diff(elapsed);
                    let mins = remaining.as_secs() / 60;
                    let secs = remaining.as_secs() % 60;
                    if state.cache_time > elapsed {
                        out.push_str(&format!("<td>{mins:02}:{secs:02}</td>"));
                    } else {
                        out.push_str(&format!("<td>Expired {mins:02}:{secs:02} ago</td>"));
                    }
                    did_time_output = true;
                }
                Some(Some(CacheEntry {
                    response: Err(FetchError::NotAllowed),
                    ..
                })) => out.push_str("<td>Blocked</td>"),
                Some(Some(CacheEntry {
                    response: Err(e), ..
                })) => out.push_str(&format!("<td>{e:?}</td>")),

                Some(None) => out.push_str("<td>Empty</td>"),
                None => out.push_str("<td>Stats timed out</td>"),
            }
            if !did_time_output {
                out.push_str("<td>N/A</td>");
            }
            out.push_str("</tr>");
        }
        out.push_str("</tbody>");
        out.push_str("</table>");
    } else {
        out.push_str("None");
    }

    let non_fetch = state
        .cache
        .iter()
        .filter(|(id, _)| !lock.contains(id.as_ref().as_str()))
        .collect::<Vec<_>>();
    drop(lock);

    out.push_str("<h2>Not automatically fetched</h2>");

    let success = non_fetch.iter().filter(|(_, v)| v.response.is_ok());
    out.push_str("<h3>Success</h3>");
    if success.clone().count() != 0 {
        out.push_str("<ul>");
        for (k, _) in success {
            out.push_str(&format!("<li>{k}</li>"));
        }
        out.push_str("</ul>");
    } else {
        out.push_str("None");
    }

    let blocked = non_fetch.iter().filter(|(_, v)| {
        v.as_ref()
            .response
            .as_ref()
            .is_err_and(|e| *e == FetchError::NotAllowed)
    });
    out.push_str("<h3>Blocked</h3>");
    if blocked.clone().count() != 0 {
        out.push_str("<ul>");
        for (k, _) in blocked {
            out.push_str(&format!("<li>{k}</li>"));
        }
        out.push_str("</ul>");
    } else {
        out.push_str("None");
    }

    out.push_str("<h3>Error</h3>");
    let error = non_fetch.iter().filter(|(_, v)| {
        v.as_ref()
            .response
            .as_ref()
            .is_err_and(|e| *e != FetchError::NotAllowed)
    });
    if error.clone().count() != 0 {
        out.push_str("<table>");

        out.push_str("<thead>");
        out.push_str("<tr>");
        out.push_str("<th>ID</th>");
        out.push_str("<th>Error</th>");
        out.push_str("</tr>");
        out.push_str("</thead>");

        out.push_str("<tbody>");
        for (k, v) in error {
            let Err(v) = v.response.as_ref() else {
                unreachable!()
            };
            out.push_str("<tr>");
            out.push_str(&format!("<td>{k}</td>"));
            out.push_str(&format!("<td>{v:?}</td>"));
            out.push_str("<td>Error</td>");
            out.push_str("</tr>");
        }
        out.push_str("</tbody>");

        out.push_str("</table>");
    } else {
        out.push_str("None");
    }

    out.push_str("</div>");

    out
}
