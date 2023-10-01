use lapin::options::QueueDeclareOptions;
use lapin::types::AMQPValue::{self};
use lapin::{
    options::{BasicConsumeOptions, BasicQosOptions},
    types::{FieldTable, ShortString},
    Channel,
};

use anyhow::{anyhow, Result};
use futures_lite::StreamExt;

use crate::{Timeframe, Transaction};
pub async fn replay_timeframe(channel: Channel, timeframe: Timeframe) -> Result<()> {
    channel
        .basic_qos(1000u16, BasicQosOptions { global: false })
        .await
        .unwrap();

    let queue = channel
        .queue_declare(
            &timeframe.queue,
            QueueDeclareOptions {
                durable: true,
                auto_delete: false,
                ..Default::default()
            },
            stream_declare_args(),
        )
        .await?;

    if queue.message_count() == 0 {
        return Err(anyhow!("No messages in queue"));
    }

    let mut consumer = channel
        .basic_consume(
            &timeframe.queue,
            "replay",
            BasicConsumeOptions::default(),
            stream_consume_args(AMQPValue::LongString("first".into())),
        )
        .await?;

    let mut messages = Vec::new();
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        let headers = delivery.properties.headers().as_ref().unwrap();
        if let AMQPValue::LongLongInt(offset) = headers.inner().get("x-stream-offset").unwrap() {
            if *offset >= (queue.message_count() - 1) as i64 {
                break;
            }
        }
        if let Some(timestamp) = *delivery.properties.timestamp() {
            if timeframe.from.timestamp_millis() as u64 <= timestamp
                && timeframe.to.timestamp_millis() as u64 >= timestamp
            {
                messages.push(
                    delivery
                        .data
                        .into_iter()
                        .map(|b| b as char)
                        .collect::<String>(),
                );
            }
        }
    }
    messages
        .into_iter()
        .map(|m| println!("{:?}", m))
        .for_each(drop);
    Ok(())
}

pub fn replay_transaction_id(channel: Channel, transaction: Transaction) {
    println!("{:?}", transaction);
}

fn stream_declare_args() -> FieldTable {
    let mut args = FieldTable::default();
    args.insert(
        ShortString::from("x-queue-type"),
        AMQPValue::LongString("stream".into()),
    );
    args
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
