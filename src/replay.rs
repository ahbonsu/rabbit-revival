use chrono::{TimeZone, Utc};
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

use crate::{HeaderReplay, MessageQuery, TimeFrameReplay};

#[derive(Serialize, Debug)]
pub struct ReplayedMessage {
    offset: u64,
    transaction_id: Option<String>,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
    data: String,
}

pub async fn replay_time_frame(
    pool: &deadpool_lapin::Pool,
    time_frame: TimeFrameReplay,
) -> Result<Vec<ReplayedMessage>> {
    let message_count = match get_queue_message_count(&time_frame.queue).await? {
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
            stream_consume_args(AMQPValue::LongString("first".into())),
        )
        .await?;

    let mut messages = Vec::new();
    let from = time_frame.from.timestamp_millis() as u64;
    let to = time_frame.to.timestamp_millis() as u64;

    while let Some(Ok(delivery)) = consumer.next().await {
        delivery.ack(BasicAckOptions::default()).await?;
        match delivery.properties.headers().as_ref() {
            Some(headers) => match headers.inner().get("x-stream-offset") {
                Some(AMQPValue::LongLongInt(offset)) => {
                    if let Some(timestamp) = *delivery.properties.timestamp() {
                        if *offset >= i64::try_from(message_count - 1)? {
                            if from <= timestamp && to >= timestamp {
                                messages.push(delivery);
                            }
                            break;
                        }
                        if from <= timestamp && to >= timestamp {
                            messages.push(delivery);
                        }
                    }
                }
                _ => {
                    return Err(anyhow!("x-stream-offset not found"));
                }
            },
            None => return Err(anyhow!("No headers found")),
        }
    }
    publish_message(&connection, messages).await
}

pub async fn fetch_messages(
    pool: &deadpool_lapin::Pool,
    message_query: MessageQuery,
) -> Result<Vec<ReplayedMessage>> {
    let message_count = match get_queue_message_count(message_query.queue.as_str()).await? {
        Some(message_count) => message_count,
        None => {
            return Err(anyhow!("Queue not found or empty"));
        }
    };

    let connection = pool.get().await?;
    let channel = connection.create_channel().await?;

    channel
        .basic_qos(1000u16, BasicQosOptions { global: false })
        .await?;

    let mut consumer = channel
        .basic_consume(
            &message_query.queue,
            "fetch_messages",
            BasicConsumeOptions::default(),
            stream_consume_args(AMQPValue::LongString("first".into())),
        )
        .await?;

    let mut messages = Vec::new();

    while let Some(Ok(delivery)) = consumer.next().await {
        delivery.ack(BasicAckOptions::default()).await?;
        match delivery.properties.headers().as_ref() {
            Some(headers) => {
                let transaction_id = match headers.inner().get("x-stream-transaction-id") {
                    Some(AMQPValue::LongString(transaction_id)) => Some(transaction_id.to_string()),
                    _ => None,
                };
                match headers.inner().get("x-stream-offset") {
                    Some(AMQPValue::LongLongInt(offset)) => {
                        let timestamp = *delivery.properties.timestamp();
                        if *offset >= i64::try_from(message_count - 1)? {
                            if is_within_timeframe(timestamp, message_query.from, message_query.to)
                            {
                                messages.push(ReplayedMessage {
                                    offset: *offset as u64,
                                    transaction_id,
                                    timestamp: Some(
                                        //unwrap is save here, because we checked if timestamp is set
                                        chrono::Utc.timestamp_millis(timestamp.unwrap() as i64),
                                    ),
                                    data: String::from_utf8(delivery.data)?,
                                });
                            }
                            break;
                        }
                        if is_within_timeframe(timestamp, message_query.from, message_query.to) {
                            messages.push(ReplayedMessage {
                                offset: *offset as u64,
                                transaction_id,
                                timestamp: Some(
                                    //unwrap is save here, because we checked if timestamp is set
                                    chrono::Utc.timestamp_millis(timestamp.unwrap() as i64),
                                ),
                                data: String::from_utf8(delivery.data)?,
                            });
                        }
                    }
                    _ => {
                        return Err(anyhow!("x-stream-offset not found"));
                    }
                }
            }
            _ => return Err(anyhow!("No headers found")),
        }
    }
    Ok(messages)
}

pub async fn replay_header(
    pool: &deadpool_lapin::Pool,
    header_replay: HeaderReplay,
) -> Result<Vec<ReplayedMessage>> {
    unimplemented!()
}

async fn get_queue_message_count(name: &str) -> Result<Option<u64>> {
    let client = reqwest::Client::new();

    let res = client
        .get("http://localhost:15672/api/queues/%2F/".to_string() + name)
        .basic_auth("guest", Some("guest"))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    if let Some(res) = res.get("type") {
        if res != "stream" {
            return Err(anyhow!("Queue is not a stream"));
        }
    }

    let message_count = res.get("messages");

    match message_count {
        Some(message_count) => Ok(Some(message_count.as_u64().unwrap())),
        None => Ok(None),
    }
}

async fn publish_message(
    connection: &Connection,
    messages: Vec<Delivery>,
) -> Result<Vec<ReplayedMessage>> {
    let channel = connection.create_channel().await?;
    let mut s = stream::iter(messages);
    let mut replayed_messages = Vec::new();
    while let Some(message) = s.next().await {
        //TODO: enable as flag -> with transaction id, with timestamp
        let uuid = uuid::Uuid::new_v4().to_string();
        let timestamp = chrono::Utc::now();
        let timestamp_u64 = timestamp.timestamp_millis() as u64;
        let mut headers = FieldTable::default();
        headers.insert(
            ShortString::from("x-stream-transaction-id"),
            AMQPValue::LongString(uuid.as_str().into()),
        );
        channel
            .basic_publish(
                message.exchange.as_str(),
                message.routing_key.as_str(),
                lapin::options::BasicPublishOptions::default(),
                message.data.as_slice(),
                lapin::BasicProperties::default()
                    .with_headers(headers)
                    .with_timestamp(timestamp_u64),
            )
            .await?;
        replayed_messages.push(ReplayedMessage {
            offset: 0,
            transaction_id: Some(uuid),
            timestamp: Some(timestamp),
            data: String::from_utf8(message.data)?,
        });
    }
    Ok(replayed_messages)
}

fn stream_consume_args(stream_offset: AMQPValue) -> FieldTable {
    let mut args = FieldTable::default();
    args.insert(ShortString::from("x-stream-offset"), stream_offset);
    args
}

fn is_within_timeframe(
    date: Option<u64>,
    from: Option<chrono::DateTime<chrono::Utc>>,
    to: Option<chrono::DateTime<chrono::Utc>>,
) -> bool {
    match date {
        Some(date) => {
            let date = Utc.timestamp_millis(date as i64);
            match (from, to) {
                (Some(from), Some(to)) => date >= from && date <= to,
                (Some(from), None) => date >= from,
                (None, Some(to)) => date <= to,
                (None, None) => true,
            }
        }
        None => false,
    }
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
