use crate::config::AppConfig;
use crate::models::Message;
use crate::py03_service::{run_py_module, PyMessage};
use crate::s3_service::S3Module;
use log::error;
use std::error::Error;
use std::io;

pub struct LangChain<'a> {
    s3_module: &'a S3Module,
    config: &'a AppConfig,
}

impl<'a> LangChain<'a> {
    pub fn new(config: &'a AppConfig, s3_module: &'a S3Module) -> Self {
        LangChain { s3_module, config }
    }

    pub async fn query(&mut self, message: Message) -> Result<Message, Box<dyn Error>> {
        // let sravz_ids = message.p_i.args.clone();
        // Display the final joined DataFrame
        // "fund_us_fbgrx.json,fund_us_fsptx.json,fund_us_fgrcx.json,fund_us_ekoax.json,fund_us_fzalx.json".to_string()))
        match run_py_module(PyMessage::new(
            message.id.to_string(),
            message.key.to_string(),
            message.p_i.args.clone().join(","),
            "".to_string(),
            "".to_string(),
            message.p_i.kwargs.json_keys.join(","),
            message.p_i.kwargs.llm_query.clone(),
        )) {
            Ok(_) => {
                log::info!("Python code executed successfully");
            }
            Err(err) => {
                error!("Error executing Python code: {:?}", err);
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Service error: {:?}", err),
                )));
            }
        }
        return Ok(message);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::AppConfig,
        langchain_service::LangChain,
        models::{Kwargs, Message},
        s3_service::S3Module,
    };
    use chrono::Utc;
    use log::{error, info};

    #[tokio::test]
    async fn test_agent_supervisor() {
        let s3_module = S3Module::new();
        let config = match AppConfig::new() {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Error: {}", err);
                // Handle the error or exit the program
                std::process::exit(1);
            }
        };
        let mut lang_chain = LangChain::new(&config, &s3_module);
        let keys = vec!["Yield_1Year_YTD".to_string(), "Yield_3Year_YTD".to_string(), "Yield_5Year_YTD".to_string()];
        let llm_query = format!(
            "Check if yield has been deceasing or increasing over time and store in value Yield_Direction.\n\
            Order the funds by yield direction.\n\
            Output data with columns Code, {}, Yield_Direction.",
            keys.join(",")
        );
        let lang_chain_result = lang_chain
            .query(Message {
                id: 2.0,
                p_i: crate::models::PI {
                    args: vec![
                        "fund_us_fbgrx.json".to_string(),
                        "fund_us_fsptx.json".to_string(),
                        "fund_us_fgrcx.json".to_string(),
                    ],
                    kwargs: Kwargs {
                        device: String::new(),
                        upload_to_aws: true,
                        json_keys: keys,
                        llm_query, // <-- set here
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

        match lang_chain_result {
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
