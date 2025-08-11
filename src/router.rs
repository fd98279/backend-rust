use crate::{
    config::AppConfig, langchain_service::LangChain, leveraged_funds_service::LeveragedFunds,
    models::Message, mongo_service::Mongo, services::earnings::Earnings,
};
use std::error::Error;

pub struct Router<'a> {
    leveraged_funds: &'a mut LeveragedFunds<'a>,
    langchain: &'a mut LangChain<'a>,
    earnings: Earnings,
    pub(crate) mongo: Mongo,
}

impl<'a> Router<'a> {
    pub fn new(
        mongo: Mongo,
        leveraged_funds: &'a mut LeveragedFunds<'a>,
        langchain: &'a mut LangChain<'a>,
    ) -> Self {
        let config = match AppConfig::new() {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Error: {}", err);
                // Handle the error or exit the program
                std::process::exit(1);
            }
        };
        // Use shaku
        let earnings: Earnings = Earnings::new(config);
        Router {
            leveraged_funds,
            langchain,
            earnings,
            mongo,
        }
    }

    pub async fn process_message(
        &mut self,
        mut message: Message,
    ) -> Result<Message, Box<dyn Error>> {
        match message.id {
            n if (1.0..=1.009).contains(&n) => self.leveraged_funds.leverage_funds(message).await,
            n if (2.0..=2.009).contains(&n) => self.langchain.query(message).await,
            n if (3.0..=3.009).contains(&n) => self.earnings.get_earnings_plot(message).await,
            _ => {
                message.exception_message = "Message ID not implemented".to_owned();
                Err(Box::new(message))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::{models::Kwargs, s3_service::S3Module};

    #[tokio::test]
    async fn test_process_message_leveraged_funds() {
        let mongo = Mongo {};
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
        let mut langchain: LangChain = LangChain::new(&config, &s3_module);
        let mut router = Router::new(mongo, &mut leveraged_funds, &mut langchain);

        let message = Message {
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
                    json_keys: Some(vec![]),
                    llm_query: None,
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
            key: String::new(),
            exception_message: String::new(),
            d_o: Some(crate::models::DO {
                bucket_name: "Fake".to_string(),
                key_name: "Fake".to_string(),
                data: serde_json::Value::String("Fake".to_string()),
                signed_url: "Fake".to_string(),
            }),
        };

        // Act
        let result = router.process_message(message).await;

        // Assert
        assert!(result.is_ok());
        let processed_message = result.unwrap();
        assert_eq!(processed_message.exception_message, "");
        println!("Output {}", processed_message.d_o.unwrap().signed_url)
    }

    #[tokio::test]
    async fn test_process_message_langchain() {
        let mongo = Mongo {};
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
        let mut langchain: LangChain = LangChain::new(&config, &s3_module);
        let mut router = Router::new(mongo, &mut leveraged_funds, &mut langchain);
        let message = Message {
            id: 2.0,
            p_i: crate::models::PI {
                args: vec![
                    "etf_us_tqqq".to_string(),
                    "etf_us_qld".to_string(),
                    "etf_us_qqq".to_string(),
                ],
                kwargs: Kwargs {
                    device: String::new(),
                    upload_to_aws: true,
                    json_keys: Some(Vec::new()),
                    llm_query: None,
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
            key: String::new(),
            exception_message: String::new(),
            d_o: Some(crate::models::DO {
                bucket_name: "Fake".to_string(),
                key_name: "Fake".to_string(),
                data: serde_json::Value::String("Fake".to_string()),
                signed_url: "Fake".to_string(),
            }),
        };

        // Act
        let result = router.process_message(message).await;

        // Assert
        assert!(result.is_ok());
        let processed_message = result.unwrap();
        assert_eq!(processed_message.exception_message, "");
        println!("Output {}", processed_message.d_o.unwrap().signed_url)
    }

    #[tokio::test]
    async fn test_process_message_earnings() {
        let mongo = Mongo {};
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
        let mut langchain: LangChain = LangChain::new(&config, &s3_module);
        let mut router = Router::new(mongo, &mut leveraged_funds, &mut langchain);
        let message = Message {
            id: 3.0,
            p_i: crate::models::PI {
                args: vec!["stk_us_nvda".to_string(), "NVDA".to_string()],
                kwargs: Kwargs {
                    device: String::new(),
                    upload_to_aws: true,
                    json_keys: Some(Vec::new()),
                    llm_query: None,
                },
            },
            t_o: String::new(),
            cid: String::new(),
            cache_message: true,
            stopic: String::new(),
            ts: 1.0,
            fun_n: "earnings".to_string(),
            date: Utc::now(),
            e: String::new(),
            key: "3".to_string(),
            exception_message: String::new(),
            d_o: Some(crate::models::DO {
                bucket_name: "Fake".to_string(),
                key_name: "Fake".to_string(),
                data: serde_json::Value::String("Fake".to_string()),
                signed_url: "Fake".to_string(),
            }),
        };

        // Act
        let result = router.process_message(message).await;

        // Assert
        assert!(result.is_ok());
        let processed_message = result.unwrap();
        assert_eq!(processed_message.exception_message, "");
        println!("Output {}", processed_message.d_o.unwrap().signed_url)
    }

    #[tokio::test]
    async fn test_process_message_invalid_id() {
        let mongo = Mongo {};
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
        let mut langchain: LangChain = LangChain::new(&config, &s3_module);
        let mut router = Router::new(mongo, &mut leveraged_funds, &mut langchain);
        let message = Message {
            id: 4.0,
            p_i: crate::models::PI {
                args: vec![
                    "etf_us_tqqq".to_string(),
                    "etf_us_qld".to_string(),
                    "etf_us_qqq".to_string(),
                ],
                kwargs: Kwargs {
                    device: String::new(),
                    upload_to_aws: true,
                    json_keys: Some(Vec::new()),
                    llm_query: None,
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
            key: String::new(),
            exception_message: String::new(),
            d_o: Some(crate::models::DO {
                bucket_name: "Fake".to_string(),
                key_name: "Fake".to_string(),
                data: serde_json::Value::String("Fake".to_string()),
                signed_url: "Fake".to_string(),
            }),
        };

        // Act
        let result = router.process_message(message).await;

        // Assert
        assert!(result.is_err());
        let err_message = result.err().unwrap();
        assert_eq!(format!("{}", err_message), "Message ID not implemented");
    }
}
