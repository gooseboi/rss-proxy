use axum::{
    extract::{Query, State},
    http::status::StatusCode,
};
use serde::Deserialize;

use crate::{
    AppState,
    deviantart::{FetchError, fetch_deviantart_rss_with_timeout},
    utils,
};
use std::sync::Arc;
use tracing::Instrument as _;

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
