use crate::{
    config::AppConfig,
    dataframe_service::DataFrameCache,
    models::Message,
    py03_service::{run_py_module, PyMessage},
    s3_service::S3Module,
};
use log::error;
use log::info;
use polars::prelude::*;
use std::error::Error;
use std::io;

pub struct Earnings {
    dataframe_service: DataFrameCache,
    s3_module: S3Module,
    config: AppConfig,
}

impl Earnings {
    pub fn new(config: AppConfig) -> Self {
        let dataframe_service = DataFrameCache::new();
        let s3_module = S3Module::new();
        Earnings {
            dataframe_service,
            s3_module,
            config,
        }
    }

    pub async fn get_earnings_s3_url(
        &mut self,
        sravz_id: &str,
        code: &str,
    ) -> Result<Option<String>, Box<dyn Error>> {
        let result = self.get_earnings(sravz_id, code).await;
        match result.unwrap() {
            Some(df) => {
                let result = self
                    .dataframe_service
                    .save_dataframe_to_s3(&df, &format!("historical/earnings/{}.json", sravz_id))
                    .await;

                match result.unwrap() {
                    presigned_url => Ok(presigned_url),
                }
            }
            None => Ok(None),
        }
    }

    pub async fn get_earnings_df_parquet(
        &mut self,
        sravz_id: &str,
        code: &str,
    ) -> Result<Option<String>, Box<dyn Error>> {
        let result = self.get_earnings(sravz_id, code).await;
        match result.unwrap() {
            Some(df) => {
                let result = self.dataframe_service.dataframe_to_parquet(df).await;

                match result.unwrap() {
                    parquet_file_path => Ok(parquet_file_path),
                }
            }
            None => Ok(None),
        }
    }

    /* Get earning dataframe */
    pub async fn get_earnings(
        &mut self,
        sravz_id: &str,
        code: &str,
    ) -> Result<Option<DataFrame>, Box<dyn Error>> {
        let mut data_frame_cache: DataFrameCache = DataFrameCache::new();
        let historical_result = data_frame_cache.get_dataframe(sravz_id.to_string()).await;

        // Handle error from get_dataframe
        let historical_df_opt = match historical_result {
            Ok(opt) => opt,
            Err(e) => {
                error!("Error fetching historical dataframe: {}", e);
                return Ok(None);
            }
        };


        match historical_df_opt {
            Some(historical_df) => {
                println!("Historical Dateframe Head {}", historical_df.head(Some(10)));
                let earnings_result = data_frame_cache.get_earnings_dataframe(code).await;
                match earnings_result.unwrap() {
                    Some(earnings_df) => {
                        info!("Earnings Dateframe Head {}", earnings_df.head(Some(10)));

                        let earnings_df = earnings_df
                            .clone()
                            .lazy()
                            .select([
                                col("report_date")
                                    .str()
                                    .to_datetime(
                                        Some(TimeUnit::Microseconds),
                                        None,
                                        StrptimeOptions::default(),
                                        lit("raise"),
                                    )
                                    .alias("ReportDateTime"),
                                col("*"),
                            ])
                            .collect()?;

                        // Perform the join on DateTime and report_date columns
                        let joined_df_result = historical_df.join(
                            &earnings_df,
                            ["DateTime"],
                            ["ReportDateTime"],
                            JoinArgs::new(JoinType::Outer),
                        );

                        let joined_df = joined_df_result.unwrap();
                        info!("Dateframe Head {}", joined_df.head(Some(10)));

                        // let joined_df = joined_df.slice(0, 10);
                        let adjusted_close_col_name = format!("{}_AdjustedClose", sravz_id);
                        let lazy_df = joined_df
                            .lazy()
                            .with_column(
                                ((col(&adjusted_close_col_name)
                                    - col(&adjusted_close_col_name).shift(lit(1)))
                                    / col(&adjusted_close_col_name).shift(lit(1))
                                    * lit(100.0))
                                .alias("1_day_pct_change"),
                            )
                            .with_column(
                                ((col(&adjusted_close_col_name)
                                    - col(&adjusted_close_col_name).shift(lit(7)))
                                    / col(&adjusted_close_col_name).shift(lit(7))
                                    * lit(100.0))
                                .alias("7_days_pct_change"),
                            )
                            .with_column(
                                ((col(&adjusted_close_col_name)
                                    - col(&adjusted_close_col_name).shift(lit(30)))
                                    / col(&adjusted_close_col_name).shift(lit(30))
                                    * lit(100.0))
                                .alias("1_month_pct_change"),
                            )
                            .with_column(
                                ((col(&adjusted_close_col_name)
                                    - col(&adjusted_close_col_name).shift(lit(90)))
                                    / col(&adjusted_close_col_name).shift(lit(90))
                                    * lit(100.0))
                                .alias("3_month_pct_change"),
                            )
                            .with_column(
                                ((col(&adjusted_close_col_name)
                                    - col(&adjusted_close_col_name).shift(lit(365)))
                                    / col(&adjusted_close_col_name).shift(lit(365))
                                    * lit(100.0))
                                .alias("1_year_pct_change"),
                            )
                            .with_column(
                                ((col(&adjusted_close_col_name)
                                    - col(&adjusted_close_col_name).shift(lit(1825)))
                                    / col(&adjusted_close_col_name).shift(lit(1825))
                                    * lit(100.0))
                                .alias("5_year_pct_change"),
                            )
                            .collect()?;

                        return Ok(Some(lazy_df.clone()));
                    }
                    None => {
                        info!("No DataFrame found");
                    }
                }
            }
            None => {
                info!("No DataFrame found");
            }
        }
        return Ok(None);
    }

    pub async fn get_earnings_plot(
        &mut self,
        mut message: Message,
    ) -> Result<Message, Box<dyn Error>> {
        let object_keys = message.p_i.args.clone();
        match &object_keys[..] {
            [sravz_id, code, ..] => {
                info!("sravz_id: {}, code: {}", sravz_id, code);
                let result = self.get_earnings_df_parquet(sravz_id, code).await;
                match result.unwrap() {
                    Some(url) => {
                        info!("Parquet file path: {}", url);
                        match run_py_module(PyMessage::new(
                            message.id.to_string(),
                            message.key.to_string(),
                            sravz_id.to_string(),
                            code.to_string(),
                            url.to_string(),
                            message.p_i.kwargs.json_keys.as_ref().map(|keys| keys.join(",")),
                            Some(message.p_i.kwargs.llm_query.clone().unwrap_or_default()),
                        )) {
                            Ok(_) => {
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
                    None => {
                        info!("No DataFrame found");
                    }
                };
            }
            _ => info!("Get earnings list of provided ticker codes is empty"),
        }

        return Ok(message);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::AppConfig,
        models::{Kwargs, Message},
        services::earnings::Earnings,
    };
    use chrono::Utc;
    use log::{error, info};

    #[tokio::test]
    async fn test_get_earnings() {
        let config = match AppConfig::new() {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Error: {}", err);
                // Handle the error or exit the program
                std::process::exit(1);
            }
        };
        let mut earnings: Earnings = Earnings::new(config);
        // Perform the GET request using the mock server URL
        let result = earnings.get_earnings("stk_us_nvda", "NVDA").await;
        match result.unwrap() {
            Some(df) => {
                println!("Joined Dateframe Head Test: {}", df.head(Some(10)));
                println!("Joined Dateframe Head Test: {}", df.tail(Some(10)));
            }
            None => {
                println!("No DataFrame found");
            }
        }
    }

    #[tokio::test]
    async fn test_get_earnings_json_string() {
        let config = match AppConfig::new() {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Error: {}", err);
                // Handle the error or exit the program
                std::process::exit(1);
            }
        };
        let mut earnings: Earnings = Earnings::new(config);

        // Perform the GET request using the mock server URL
        let result = earnings.get_earnings("stk_us_nvda", "NVDA").await;
        match result.unwrap() {
            Some(df) => {
                let result = earnings
                    .dataframe_service
                    .dataframe_to_json(&df.head(Some(10)))
                    .await;

                match result.unwrap() {
                    json_string => {
                        println!("Dateframe JSON {}", json_string);
                    }
                }
            }
            None => {
                println!("No DataFrame found");
            }
        }
    }

    #[tokio::test]
    async fn test_get_earnings_s3_url() {
        let config = match AppConfig::new() {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Error: {}", err);
                // Handle the error or exit the program
                std::process::exit(1);
            }
        };
        let mut earnings: Earnings = Earnings::new(config);

        // Perform the GET request using the mock server URL
        let result = earnings.get_earnings_s3_url("stk_us_nvda", "NVDA").await;
        match result.unwrap() {
            Some(url) => {
                println!("S3 Presigned URL: {}", url);
            }
            None => {
                println!("No DataFrame found");
            }
        }
    }

    #[tokio::test]
    async fn test_get_earnings_parquet_file() {
        let config = match AppConfig::new() {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Error: {}", err);
                // Handle the error or exit the program
                std::process::exit(1);
            }
        };
        let mut earnings: Earnings = Earnings::new(config);

        // Perform the GET request using the mock server URL
        let result = earnings
            .get_earnings_df_parquet("stk_us_nvda", "NVDA")
            .await;
        match result.unwrap() {
            Some(url) => {
                println!("Parquet file path: {}", url);
            }
            None => {
                println!("No DataFrame found");
            }
        }
    }

    #[tokio::test]
    async fn test_get_earnings_plot() {
        let config = match AppConfig::new() {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Error: {}", err);
                // Handle the error or exit the program
                std::process::exit(1);
            }
        };
        let mut earnings: Earnings = Earnings::new(config);

        // Perform the GET request using the mock server URL
        let result = earnings
            .get_earnings_plot(Message {
                id: 3.0,
                p_i: crate::models::PI {
                    args: vec!["stk_us_nvda".to_string(), "NVDA".to_string()],
                    kwargs: Kwargs {
                        device: String::new(),
                        upload_to_aws: true,
                        json_keys: Some(Vec::new()),
                        llm_query: Some(String::new()),
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
            })
            .await;

        match result {
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
