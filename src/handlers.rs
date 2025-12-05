use axum::{
    extract::{Query, State},
    http::{Response, status::StatusCode},
};
use serde::Deserialize;

use crate::{
    AppState,
    deviantart::{self, FetchError, fetch_deviantart_rss_with_timeout},
    utils,
};
use std::sync::Arc;
use tracing::Instrument as _;

pub async fn stats_handler(
    State(state): State<AppState>,
) -> Result<Response<String>, (StatusCode, String)> {
    let deviantart_html = deviantart::get_stats(state.deviantart_state.clone());

    let mut out = String::new();
    out.push_str("<html>");

    out.push_str("<head>");
    out.push_str("</head>");

    out.push_str("<body>");
    out.push_str("<h1>Deviantart</h1>");
    out.push_str(&deviantart_html);
    out.push_str("</body>");

    Response::builder()
        .status(200)
        .header("Content-Type", "text/html")
        .body(out)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

#[derive(Deserialize, Debug)]
pub struct DeviantartQuery {
    id: String,
}

pub async fn deviantart_rss_handler(
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

                    Arc::new(result.map(|s| utils::compress_zstd(&s.into_bytes())))
                }
                .instrument(coro_span),
            )
            .await
    });

    let cached = handle.await.expect("can join thread");
    tracing::debug!(id = query.id, "Got result from cache for rss");

    match *cached {
        Ok(ref cached) => {
            let bytes = utils::decompress_zstd(cached);
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
