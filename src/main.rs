mod config;
mod dataframe_service;
mod helper;
mod langchain_service;
mod leveraged_funds_service;
mod models;
mod mongo_service;
mod py03_service;
mod rest_client;
mod router;
mod s3_service;
use crate::{config::AppConfig, helper::sha256_hash, models::Message, router::Router};
use chrono::{Duration, Utc};
use env_logger::Env;
use langchain_service::LangChain;
use leveraged_funds_service::LeveragedFunds;
use log::{error, info};
use mongo_service::Mongo;
use mongodb::Client;
use rand::seq::SliceRandom;
use s3_service::S3Module;
use std::collections::HashSet;
use std::error::Error;
use std::fs::{self};
use tokio;
use tokio_nsq::{
    NSQChannel, NSQConsumerConfig, NSQConsumerConfigSources, NSQConsumerLookupConfig,
    NSQProducerConfig, NSQTopic,
};
mod services {
    pub mod earnings;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    // Create an AppConfig instance by fetching values from environment variables
    let config = match AppConfig::new() {
        Ok(config) => config,
        Err(err) => {
            error!("Error: {}", err);
            // Handle the error or exit the program
            std::process::exit(1);
        }
    };

    // Create data subdirectory
    let subdirectory_path = "/tmp/data/";

    // Create the subdirectory only if it doesn't exist
    if let Err(err) = fs::create_dir(subdirectory_path) {
        if err.kind() != std::io::ErrorKind::AlreadyExists {
            // If the error is not due to the directory already existing, print the error
            error!("Error creating subdirectory: {}", err);
            std::process::exit(1);
        }
    }

    let consumer_topic = NSQTopic::new(config.config.backend_rust_topic.clone())
        .expect("Failed to create consumer topic");
    let channel = NSQChannel::new(config.config.backend_rust_topic.clone())
        .expect("Failed to create NSQ channel");

    let client = Client::with_uri_str(config.mongolab_uri.clone())
        .await
        .expect("Unable to connect to MongoDB");

    let mut addresses = HashSet::new();
    let nsqlookupd_addresses: Vec<String> = config
        .nsq_lookupd_host
        .split(',')
        .map(String::from)
        .collect();

    for address in nsqlookupd_addresses.iter() {
        // addresses.insert("http://nsqlookupd-1:4161".to_string());
        addresses.insert(format!("http://{}", address));
        info!("Lookupd address: {}", format!("http://{}", address))
    }

    info!(
        "Listening to nsq topic {} - channel {:?}",
        &config.config.backend_rust_topic, &config.config.backend_rust_topic
    );

    let mut consumer = NSQConsumerConfig::new(consumer_topic, channel)
        .set_max_in_flight(15)
        .set_sources(NSQConsumerConfigSources::Lookup(
            NSQConsumerLookupConfig::new().set_addresses(addresses),
        ))
        .build();

    let mut rng: rand::prelude::ThreadRng = rand::thread_rng();
    let nsqd_hosts: Vec<String> = config.nsq_host.split(',').map(String::from).collect(); // vec!["nsqd-1:4150", "nsqd-2:4150", "nsqd-3:4150"];
    let mut nsqd_host = "";
    if let Some(random_element) = nsqd_hosts.choose(&mut rng) {
        nsqd_host = random_element;
        info!("Connecting to nsqd host {}", nsqd_host)
    } else {
        println!("nsqd_hosts vector empty");
    }

    let mut producer = NSQProducerConfig::new(nsqd_host).build();

    /* Create router dependencies */
    // TODO: Check proper dependency injection
    let mongo = Mongo {};
    let s3_module = S3Module::new();
    let mut leveraged_funds = LeveragedFunds::new(&config, &s3_module);
    let mut langchain: LangChain = LangChain::new(&config, &s3_module);

    let mut router = Router::new(mongo, &mut leveraged_funds, &mut langchain);
    loop {
        let message = consumer
            .consume_filtered()
            .await
            .expect("Failed to consume NSQ message");
        info!("Processing the message...");

        let message_body_str =
            std::str::from_utf8(&message.body).expect("Failed to get JSON string from NSQ Message");
        info!("Message received {}", message_body_str);

        let result: Result<Message, serde_json::Error> = serde_json::from_str(message_body_str);

        // Handle the result using pattern matching
        match result {
            Ok(mut _message) => {
                // Empty out error message if the client send an error already
                _message.e = "".to_string();
                _message.exception_message = "".to_string();
                _message.date = Utc::now();

                // Sort kwargs for consistent hashing
                let mut sorted_kwargs = serde_json::to_value(&_message.p_i.kwargs)
                    .expect("Failed to serialize kwargs");
                if let serde_json::Value::Object(ref mut map) = &mut sorted_kwargs {
                    // Convert keys and string values to lowercase
                    let lowercase_entries: Vec<(String, serde_json::Value)> = map
                        .iter()
                        .map(|(k, v)| {
                            let lowercase_value = match v {
                                serde_json::Value::String(s) => serde_json::Value::String(s.to_lowercase()),
                                _ => v.clone(),
                            };
                            (k.to_lowercase(), lowercase_value)
                        })
                        .collect();
                    
                    // Clear and repopulate the map with lowercase entries
                    map.clear();
                    for (k, v) in lowercase_entries {
                        map.insert(k, v);
                    }
                    
                    // Create a sorted map
                    let mut sorted: Vec<_> = map.iter().collect();
                    sorted.sort_by(|a, b| a.0.cmp(b.0));
                }

                // Get hash of the message
                let hashed_string = &sha256_hash(&format!(
                    "{:?}{:?}{}{:?}",
                    _message.p_i, _message.id, _message.fun_n, sorted_kwargs
                ));

                info!(
                    "Message received on NSQ = {} - SHA-256 = {}",
                    message_body_str, hashed_string
                );

                _message.key = hashed_string.to_string();

                let messages = router
                    .mongo
                    .find_by_key(hashed_string.to_string(), &client)
                    .await
                    .expect("Document not found");

                /* If the message exists and inserted less than 24 hours back - resend the same message else reprocess the message */
                if messages.len() > 0 && messages[0].date + Duration::days(1) > Utc::now() {
                    let mut message_from_router = messages[0].clone();
                    message_from_router.cid = _message.cid.clone();
                    let message_from_router_json = &serde_json::to_string(&message_from_router)
                        .expect("Failed to convert NSQ message to JSON string");

                    info!(
                        "Sending the existing message in mongodb {}",
                        message_from_router_json
                    );
                    
                    let producer_topic =
                        NSQTopic::new(_message.t_o).expect("Failed to create producer topic");
                    producer
                        .publish(&producer_topic, message_from_router_json.as_bytes().to_vec())
                        .await
                        .expect("Failed to publish NSQ message");
                    producer
                        .consume()
                        .await
                        .expect("Failed to consume NSQ message");                    
                    message.finish().await;
                    continue;
                } else {
                    // Clone the message so we have a reference in this routine
                    let mut original_message = _message.clone();
                    original_message.key = hashed_string.to_string();

                    // Set message in progress before processing
                    // Check if message is already in progress
                    let status = router.mongo.is_message_in_progress(
                        &original_message.key,
                        &client,
                        None
                    ).await;
                    if status.as_deref() != Some("IN_PROGRESS") {
                        router.mongo.set_message_in_progress(
                            &original_message.key,
                            &serde_json::to_string(&original_message).unwrap_or_default(),
                            "",
                            "IN_PROGRESS",
                            &client,
                            None
                        ).await;

                        // Pass the original message to the router
                        let router_result = router.process_message(_message).await;
                        match router_result {
                            Ok(processed_message) => {
                                // Reassign processed message to the original message
                                original_message = processed_message;
                            }
                            Err(err) => {
                                // Update original message
                                original_message.e = "Error".to_string();
                                original_message.exception_message = err.to_string();
                                error!("Router message processing error: {}", err);
                            }
                        }

                        // Save the message to mongodb cache
                        router
                            .mongo
                            .update_one(original_message.clone(), &client)
                            .await;
                        let message_body_str = &serde_json::to_string(&original_message)
                            .expect("Failed to convert message to JSON string");
                        info!("Sending the processed message on NSQ {}", message_body_str);                    

                    } else {
                        original_message.e = format!(
                            "Given message key: {} - ({}) is being processed. Please check after sometime",
                            &original_message.key,
                            serde_json::to_string(&original_message).unwrap_or_default()
                        );
                        info!("Message with key {} is already in progress. Skipping processing.", &original_message.key);
                    }       

                    // Send the message to NSQ
                    let producer_topic = NSQTopic::new(original_message.t_o.clone())
                        .expect("Failed to create producer topic");
                    producer
                        .publish(&producer_topic, message_body_str.as_bytes().to_vec())
                        .await
                        .expect("Failed to publish NSQ message");
                    producer
                        .consume()
                        .await
                        .expect("Failed to consume NSQ message");                
                    message.finish().await;
                    continue;                                 
                }
            }
            Err(err) => {
                error!("Deserialization failed: {}", err);
                message.finish().await;
            }
        }
    }

    // Ok(())
}
