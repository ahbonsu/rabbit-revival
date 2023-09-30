use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{
    extract::Json,
    extract::{Query, State},
    response::IntoResponse,
    routing::get,
    Router,
};
use chrono::DateTime;
use deadpool_lapin::{Config, PoolConfig, Runtime};
use futures_lite::StreamExt;
use lapin::types::AMQPValue::{self};
use lapin::{
    options::{BasicConsumeOptions, BasicQosOptions},
    types::{FieldTable, ShortString},
    Channel,
};

#[derive(serde::Deserialize, Debug)]
#[serde(untagged)]
enum ReplayMode {
    Timeframe(Timeframe),
    Transaction(Transaction),
}

#[derive(serde::Deserialize, Debug)]
struct Timeframe {
    queue: String,
    from: DateTime<chrono::Utc>,
    to: DateTime<chrono::Utc>,
}

#[derive(serde::Deserialize, Debug)]
struct Transaction {
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
            replay_timeframe(channel, timeframe).await;
        }
        ReplayMode::Transaction(transaction) => {
            replay_transaction_id(channel, transaction);
        }
    };
}

async fn replay_timeframe(channel: Channel, timeframe: Timeframe) -> Result<()> {
    channel
        .basic_qos(1000u16, BasicQosOptions { global: false })
        .await
        .unwrap();
    let mut consumer_args = FieldTable::default();
    consumer_args.insert(
        ShortString::from("x-stream-offset"),
        AMQPValue::LongString("first".into()),
        //Timestamp(timeframe.from.timestamp_millis() as u64),
    );

    let mut consumer = channel
        .basic_consume(
            &timeframe.queue,
            "replay",
            BasicConsumeOptions::default(),
            consumer_args,
        )
        .await?;
    //let mut messages = vec![];

    while let Some(delivery) = consumer.next().await {
        //while delibery timestamp < timeframe.to add to messages else break
        match delivery {
            Ok(delivery) => {
                println!("got message: {:?}", delivery);
                tracing::debug!("got message: {:?}", delivery);
            }
            Err(error) => {
                println!("Error caught in consumer: {}", error);
                break;
            }
        }
    }
    Ok(())
}

fn replay_transaction_id(channel: Channel, transaction: Transaction) {
    println!("{:?}", transaction);
}

#[cfg(test)]
mod tests {
    use lapin::{
        options::{BasicPublishOptions, QueueDeclareOptions, QueueDeleteOptions},
        protocol::basic::AMQPProperties,
        types::{AMQPValue, FieldTable, ShortString},
        Connection, ConnectionProperties,
    };

    async fn setup() {
        let connection = Connection::connect(
            "amqp://guest:guest@localhost:5672",
            ConnectionProperties::default(),
        )
        .await
        .unwrap();

        let channel = connection.create_channel().await.unwrap();

        let _ = channel
            .queue_delete("replay", QueueDeleteOptions::default())
            .await;

        let mut queue_args = FieldTable::default();
        queue_args.insert(
            ShortString::from("x-queue-type"),
            AMQPValue::LongString("stream".into()),
        );

        channel
            .queue_declare(
                "replay",
                QueueDeclareOptions {
                    durable: true,
                    auto_delete: false,
                    ..Default::default()
                },
                queue_args,
            )
            .await
            .unwrap();

        for i in 0..500 {
            let timestamp = chrono::Utc::now().timestamp_millis() as u64;
            let transaction_id = format!("transaction_{}", i);
            let mut headers = FieldTable::default();
            headers.insert(
                ShortString::from("transaction_id"),
                AMQPValue::LongString(transaction_id.clone().into()),
            );

            channel
                .basic_publish(
                    "",
                    "replay",
                    BasicPublishOptions::default(),
                    b"test",
                    AMQPProperties::default()
                        .with_headers(headers)
                        .with_timestamp(timestamp),
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn foo() {
        setup().await;
        assert_eq!(true, true);
    }
}
