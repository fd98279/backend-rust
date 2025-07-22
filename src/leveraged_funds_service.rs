use crate::config::AppConfig;
use crate::py03_service::run_py_module;
use crate::py03_service::PyMessage;
use crate::s3_service::S3Module;
use crate::{dataframe_service::DataFrameCache, models::Message};
use log::{error, info};
use polars::prelude::*;
use std::error::Error;
use std::io;

pub struct LeveragedFunds<'a> {
    pub(crate) dataframe_cache: DataFrameCache,
    pub(crate) s3_module: &'a S3Module,
    pub(crate) config: &'a AppConfig,
}

impl<'a> LeveragedFunds<'a> {
    pub fn new(config: &'a AppConfig, s3_module: &'a S3Module) -> Self {
        // TODO: Check proper dependency injection
        let dataframe_cache = DataFrameCache::new();
        LeveragedFunds {
            dataframe_cache,
            s3_module,
            config,
        }
    }

    pub async fn leverage_funds(
        &mut self,
        mut message: Message,
    ) -> Result<Message, Box<dyn Error>> {
        let object_keys = message.p_i.args.clone();
        let mut dataframe_vector: Vec<DataFrame> = Vec::new();
        for sravzid in object_keys {
            match self.dataframe_cache.get_dataframe(sravzid).await {
                Ok(dataframe) => match dataframe {
                    Some(value) => dataframe_vector.push(value),
                    None => {
                        log::error!("Dataframe not found");
                    }
                },
                Err(error) => {
                    return Err(error);
                }
            }
        }

        // Join DataFrames in the vector on the "id" column
        let mut joined_df: Option<DataFrame> = None;
        for df in dataframe_vector {
            let cloned_joined_df = joined_df.clone();
            if let Some(joined) = cloned_joined_df {
                let join_result = joined.inner_join(&df, ["DateTime"], ["DateTime"]);
                match join_result {
                    Ok(joined_result) => {
                        joined_df = Some(joined_result);
                    }
                    Err(err) => {
                        eprintln!("Error during join: {:?}", err);
                        return Err(Box::new(io::Error::new(
                            io::ErrorKind::Other,
                            format!("Service error: {:?}", err),
                        )));
                    }
                }
            } else {
                joined_df = Some(df.clone());
            }
        }

        // Display the final joined DataFrame
        if let Some(mut joined) = joined_df {
            let mut file =
                match std::fs::File::create(format!("/tmp/data/{}.parquet", &message.key)) {
                    Ok(value) => value,
                    Err(err) => {
                        error!("Cannot create parquet file {}", err);
                        return Ok(message);
                    }
                };

            match ParquetWriter::new(&mut file).finish(&mut joined) {
                Ok(_) => {
                    info!(
                        "Parquet file {} created",
                        format!("/tmp/data/{}.parquet", &message.key)
                    )
                }
                Err(err) => {
                    error!("Unable to write parquet file {}", err);
                    return Ok(message);
                }
            };
            // println!("{:?}", joined);

            match run_py_module(PyMessage::new(
                message.id.to_string(),
                message.key.to_string(),
                "".to_string(),
                "".to_string(),
                "".to_string(),
                message.p_i.kwargs.json_keys.clone(),
                message.p_i.kwargs.llm_query.clone(),
            )) {
                Ok(_) => {
                    // println!("Python code executed successfully");
                    self.s3_module
                        .upload_file(
                            "sravz",
                            &format!("rust-backend/{}.png", message.key),
                            &format!("/tmp/data/{}.png", message.key),
                        )
                        .await?;
                    message.update_s3_location(
                        self.config.contabo_bucket.clone(),
                        self.config.contabo_object_url_prefix.clone(),
                        format!("{}.png", message.key),
                    )
                }
                Err(err) => {
                    error!("Error executing Python code: {:?}", err);
                    return Err(Box::new(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Service error: {:?}", err),
                    )));
                }
            }
        }
        return Ok(message);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::AppConfig,
        leveraged_funds_service::LeveragedFunds,
        models::{Kwargs, Message},
        s3_service::S3Module,
    };
    use chrono::Utc;
    use log::{error, info};

    #[tokio::test]
    async fn test_leverage_funds() {
        let s3_module = S3Module::new();
        let config = match AppConfig::new() {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Error: {}", err);
                // Handle the error or exit the program
                std::process::exit(1);
            }
        };
        let mut leveraged_funds = LeveragedFunds::new(&config, &s3_module);
        let leveraged_fund_result = leveraged_funds
            .leverage_funds(Message {
                id: 1.0,
                p_i: crate::models::PI {
                    args: vec![
                        "etf_us_tqqq".to_string(),
                        "etf_us_qld".to_string(),
                        "etf_us_qqq".to_string(),
                    ],
                    kwargs: Kwargs {
                        device: String::new(),
                        upload_to_aws: true,
                        json_keys: String::new(),
                        llm_query: String::new(),
                    },
                },
                t_o: String::new(),
                cid: String::new(),
                cache_message: true,
                stopic: String::new(),
                ts: 1.0,
                fun_n: "leveraged_funds".to_string(),
                date: Utc::now(),
                e: String::new(),
                key: "1".to_string(),
                exception_message: String::new(),
                d_o: Some(crate::models::DO {
                    bucket_name: "Fake".to_string(),
                    key_name: "Fake".to_string(),
                    data: serde_json::Value::String("Fake".to_string()),
                    signed_url: "Fake".to_string(),
                }),
            })
            .await;

        match leveraged_fund_result {
            Ok(mut processed_message) => {
                processed_message.date = Utc::now();
                let message_body_str = &serde_json::to_string(&processed_message)
                    .expect("Failed to convert message to JSON string");
                info!("Sending the processed message on NSQ {}", message_body_str);
            }
            Err(err) => {
                error!("Router message processing error: {}", err);
            }
        }
    }
}
