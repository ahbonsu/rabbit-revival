use std::{net::SocketAddr, sync::Arc};

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

#[derive(serde::Deserialize, Debug)]
pub struct MessageQuery {
    queue: String,
    from: Option<DateTime<chrono::Utc>>,
    to: Option<DateTime<chrono::Utc>>,
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
    Query(message_query): Query<MessageQuery>,
) -> Result<impl IntoResponse, AppError> {
    let messages = fetch_messages(&app_state.pool.clone(), message_query).await?;
    Ok((StatusCode::OK, Json(messages)))
}

async fn replay(
    app_state: State<Arc<AppState>>,
    Json(replay_mode): Json<ReplayMode>,
) -> Result<impl IntoResponse, AppError> {
    let pool = app_state.pool.clone();
    match replay_mode {
        ReplayMode::TimeFrameReplay(timeframe) => {
            replay_time_frame(&pool, timeframe).await?;
            Ok((StatusCode::OK, "Replay successful"))
        }
        ReplayMode::HeaderReplay(transaction) => {
            replay_header(&pool, transaction);
            Ok((StatusCode::OK, "Replay successful"))
        }
    }
}

//https://github.com/tokio-rs/axum/blob/main/examples/anyhow-error-response/src/main.rs
// Make our own error that wraps `anyhow::Error`.
struct AppError(anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
