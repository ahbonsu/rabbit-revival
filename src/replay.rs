use lapin::message::Delivery;
use lapin::options::BasicAckOptions;
use lapin::types::AMQPValue::{self};
use lapin::Connection;
use lapin::{
    options::{BasicConsumeOptions, BasicQosOptions},
    types::{FieldTable, ShortString},
};

use anyhow::{anyhow, Result};
use futures_lite::{stream, StreamExt};
use serde::Serialize;

use crate::{HeaderReplay, TimeFrameReplay};

#[derive(Serialize, Debug)]
pub struct ReplayedMessage {
    headers: FieldTable,
    data: Vec<u8>,
}

pub async fn replay_time_frame(
    pool: &deadpool_lapin::Pool,
    time_frame: TimeFrameReplay,
) -> Result<()> {
    let message_count = match get_queue_metadata(&time_frame.queue).await {
        Some(message_count) => message_count,
        None => return Err(anyhow!("Queue not found or empty")),
    };

    let connection = pool.get().await?;
    let channel = connection.create_channel().await?;

    channel
        .basic_qos(1000u16, BasicQosOptions { global: false })
        .await?;

    let mut consumer = channel
        .basic_consume(
            &time_frame.queue,
            "replay",
            BasicConsumeOptions::default(),
            //TODO: use from as offset
            stream_consume_args(AMQPValue::LongString("first".into())),
        )
        .await?;

    let mut messages = Vec::new();
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        let headers = delivery.properties.headers().as_ref().unwrap();
        delivery.ack(BasicAckOptions::default()).await.expect("ack");
        if let AMQPValue::LongLongInt(offset) = headers.inner().get("x-stream-offset").unwrap() {
            if *offset >= i64::try_from(message_count - 1)? {
                messages.push(delivery);
                break;
            }
        }
        if let Some(timestamp) = *delivery.properties.timestamp() {
            if time_frame.from.timestamp_millis() as u64 <= timestamp
                && time_frame.to.timestamp_millis() as u64 >= timestamp
            {
                messages.push(delivery);
            }
        }
    }

    println!("messages: {}", messages.len());

    Ok(())

    //publish_message(&connection, messages).await
}

pub async fn fetch_messages(
    pool: &deadpool_lapin::Pool,
    queue: String,
    from: Option<String>,
    to: Option<String>,
) -> Result<Vec<ReplayedMessage>> {
    let message_count = match get_queue_metadata(queue.as_str()).await {
        Some(message_count) => message_count,
        None => return Err(anyhow!("Queue not found or empty")),
    };

    let connection = pool.get().await?;
    let channel = connection.create_channel().await?;

    channel
        .basic_qos(1000u16, BasicQosOptions { global: false })
        .await?;

    let mut consumer = channel
        .basic_consume(
            &queue,
            "fetch_messages",
            BasicConsumeOptions::default(),
            stream_consume_args(AMQPValue::LongString("first".into())),
        )
        .await?;

    let mut messages = Vec::new();
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        let headers = delivery.properties.headers().as_ref().unwrap();
        let mut response_headers = FieldTable::default();
        delivery.ack(BasicAckOptions::default()).await.expect("ack");
        if let AMQPValue::LongLongInt(offset) = headers.inner().get("x-stream-offset").unwrap() {
            if *offset >= i64::try_from(message_count - 1)? {
                break;
            }
            response_headers.insert(
                ShortString::from("x-stream-offset"),
                AMQPValue::LongLongInt(*offset),
            );
            response_headers.insert(
                ShortString::from("x-stream-transaction-id"),
                headers
                    .inner()
                    .get("x-stream-transaction-id")
                    .unwrap()
                    .clone(),
            );

            messages.push(ReplayedMessage {
                headers: response_headers,
                data: delivery.data,
            });
        }
    }
    Ok(messages)
}

pub fn replay_header(pool: &deadpool_lapin::Pool, header_replay: HeaderReplay) {}

async fn get_queue_metadata(name: &str) -> Option<u64> {
    let client = reqwest::Client::new();

    let res = client
        .get("http://localhost:15672/api/queues/%2F/".to_string() + name)
        .basic_auth("guest", Some("guest"))
        .send()
        .await
        .ok()?
        .json::<serde_json::Value>()
        .await
        .ok()?;

    let message_count = res.get("messages");

    match message_count {
        Some(message_count) => Some(message_count.as_u64().unwrap()),
        _ => None,
    }
}

async fn publish_message(connection: &Connection, messages: Vec<Delivery>) -> Result<()> {
    let channel = connection.create_channel().await?;
    let mut s = stream::iter(messages);
    while let Some(message) = s.next().await {
        //TODO: enable as flag -> with transaction id, with timestamp
        let uuid = uuid::Uuid::new_v4().to_string();
        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        let mut headers = FieldTable::default();
        headers.insert(
            ShortString::from("x-stream-transaction-id"),
            AMQPValue::LongString(uuid.into()),
        );
        channel
            .basic_publish(
                message.exchange.as_str(),
                message.routing_key.as_str(),
                lapin::options::BasicPublishOptions::default(),
                message.data.as_slice(),
                lapin::BasicProperties::default()
                    .with_headers(headers)
                    .with_timestamp(timestamp),
            )
            .await?;
    }
    Ok(())
}

fn stream_consume_args(stream_offset: AMQPValue) -> FieldTable {
    let mut args = FieldTable::default();
    args.insert(ShortString::from("x-stream-offset"), stream_offset);
    args
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
                ShortString::from("x-stream-transaction-id"),
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
