use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{
    extract::Json,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use chrono::DateTime;
use deadpool_lapin::{Config, PoolConfig, Runtime};
use replay::{fetch_messages, replay_header, replay_time_frame};

pub mod replay;

#[derive(serde::Deserialize, Debug)]
#[serde(untagged)]
enum ReplayMode {
    TimeFrameReplay(TimeFrameReplay),
    HeaderReplay(HeaderReplay),
}

#[derive(serde::Deserialize, Debug)]
pub struct TimeFrameReplay {
    queue: String,
    from: DateTime<chrono::Utc>,
    to: DateTime<chrono::Utc>,
}

#[derive(serde::Deserialize, Debug)]
pub struct HeaderReplay {
    queue: String,
    header: AMQPHeader,
}

#[derive(serde::Deserialize, Debug)]
struct AMQPHeader {
    name: String,
    value: String,
}

struct AppState {
    pool: deadpool_lapin::Pool,
}

#[tokio::main]
async fn main() {
    // initialize tracing
    tracing_subscriber::fmt::init();

    let mut cfg = Config::default();
    cfg.url = Some(std::env::var("AMQP_URL").unwrap_or("amqp://guest:guest@localhost:5672".into()));

    // TODO: read from env
    cfg.pool = Some(PoolConfig::new(5));

    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

    let app = Router::new()
        .route("/", get(get_messages).post(replay))
        .with_state(Arc::new(AppState { pool }));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn get_messages(
    app_state: State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let pool = app_state.pool.clone();
    let queue = params.get("queue").cloned();
    let from = params.get("from").cloned();
    let to = params.get("to").cloned();

    let queue = match queue {
        Some(queue) => queue.to_string(),
        None => return (StatusCode::BAD_REQUEST, "Missing queue parameter").into_response(),
    };

    match fetch_messages(&pool, queue, from, to).await {
        Ok(messages) => (StatusCode::OK, Json(messages)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn replay(
    app_state: State<Arc<AppState>>,
    Json(replay_mode): Json<ReplayMode>,
) -> impl IntoResponse {
    let pool = app_state.pool.clone();
    match replay_mode {
        ReplayMode::TimeFrameReplay(timeframe) => match replay_time_frame(&pool, timeframe).await {
            Ok(_) => (StatusCode::OK, "Replay successful"),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, "Replay failed"),
        },
        ReplayMode::HeaderReplay(transaction) => {
            replay_header(&pool, transaction);
            (StatusCode::OK, "Replay successful")
        }
    }
}
