use std::net::SocketAddr;

use axum::{routing::get, Router};
use rabbit_revival::{get_messages, initialize_state, replay};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    // initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "rabbit_revival=debug,tower_http=trace,axum::rejection=trace".into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let app = Router::new()
        .route("/", get(get_messages).post(replay))
        .layer(TraceLayer::new_for_http())
        .with_state(initialize_state().await);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));

    tracing::info!("Listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
