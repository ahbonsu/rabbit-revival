use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{
    extract::Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use chrono::DateTime;
use deadpool_lapin::{Config, PoolConfig, Runtime};

pub mod replay;

#[derive(serde::Deserialize, Debug)]
#[serde(untagged)]
enum ReplayMode {
    Timeframe(Timeframe),
    Transaction(Transaction),
}

#[derive(serde::Deserialize, Debug)]
pub struct Timeframe {
    queue: String,
    from: DateTime<chrono::Utc>,
    to: DateTime<chrono::Utc>,
}

#[derive(serde::Deserialize, Debug)]
pub struct Transaction {
    queue: String,
    transaction_id: String,
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
) -> impl IntoResponse {
}

async fn replay(
    app_state: State<Arc<AppState>>,
    Json(replay_mode): Json<ReplayMode>,
) -> impl IntoResponse {
    let pool = app_state.pool.clone();
    //TODO: error handling
    let connection = pool.get().await.unwrap();
    let channel = connection.create_channel().await.unwrap();
    match replay_mode {
        ReplayMode::Timeframe(timeframe) => {
            match replay::replay_timeframe(channel, timeframe).await {
                Ok(_) => (StatusCode::OK, "Replay successful"),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, "Replay failed"),
            }
        }
        ReplayMode::Transaction(transaction) => {
            replay::replay_transaction_id(channel, transaction);
            (StatusCode::OK, "Replay successful")
        }
    };
}
