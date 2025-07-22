use crate::config::AppConfig;
use crate::rest_client::RestClient;
use crate::s3_service::S3Module;
use chrono::{Duration, Utc};
use log::{error, info};
use polars::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::error::{self, Error};
use std::io::Cursor;
use tempfile::NamedTempFile;

pub struct DataFrameCache {
    dataframe_map: HashMap<String, DataFrame>,
    s3_module: S3Module,
    rest_client: RestClient,
}

impl<'a> DataFrameCache {
    pub fn new() -> Self {
        let dataframe_map = HashMap::new();
        let s3_module = S3Module::new();
        let _config = match AppConfig::new() {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Error: {}", err);
                // Handle the error or exit the program
                std::process::exit(1);
            }
        };
        let rest_client = RestClient::new();
        DataFrameCache {
            dataframe_map,
            s3_module,
            rest_client,
        }
    }

    pub async fn dataframe_to_json(
        &mut self,
        df: &DataFrame,
    ) -> Result<String, Box<dyn std::error::Error>> {
        // Create a vector to store the JSON representation of each row
        let mut json_rows = Vec::new();
        let num_rows = df.height(); // Number of rows in the DataFrame

        // Iterate over the rows
        for i in 0..num_rows {
            let mut map = HashMap::new();

            // For each column in the DataFrame
            for col in df.get_columns() {
                // Get the column name
                let col_name = col.name();

                // Get the value for this row (i) from the column
                let value = col.get(i); // This returns an AnyValue

                match value.unwrap() {
                    col_val => {
                        // Insert the column name and value into the map (converting AnyValue to string)
                        map.insert(col_name.to_string(), col_val.to_string());
                    }
                }
            }

            // Push the row map into the json_rows vector
            json_rows.push(map);
        }

        // Serialize the vector of row maps to JSON
        let json_output = serde_json::to_string(&json_rows)?;

        // Return the JSON string
        Ok(json_output)
    }

    pub async fn dataframe_to_parquet(
        &mut self,
        mut df: DataFrame,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let tmp_file = NamedTempFile::new()?;
        let file_path = format!("{}.parquet", tmp_file.path().to_string_lossy().to_string());

        let mut file = match std::fs::File::create(file_path.clone()) {
            Ok(value) => value,
            Err(err) => {
                error!("Cannot create parquet file {}", err);
                return Ok(None);
            }
        };

        match ParquetWriter::new(&mut file).finish(&mut df) {
            Ok(_) => {
                info!("Parquet file {} created", file_path);
                return Ok(Some(file_path));
            }
            Err(err) => {
                error!("Unable to write parquet file {}", err);
                return Ok(None);
            }
        }
    }

    /* Get historical data dataframe */
    pub async fn get_dataframe(
        &mut self,
        sravz_id: String,
    ) -> Result<Option<DataFrame>, Box<dyn Error>> {
        let bucket_name = "sravz-data";
        if self.dataframe_map.contains_key(&sravz_id) {
            if let Some(value) = self.dataframe_map.get(&sravz_id) {
                return Ok(Some(value.clone()));
            }
        } else {
            match self
                .s3_module
                .download_object(bucket_name, &format!("historical/{}.json", sravz_id), false)
                .await
            {
                Ok(downloaded_content) => {
                    match self.s3_module.decompress_gzip(downloaded_content) {
                        Ok(decompressed_data) => {
                            let cursor: Cursor<Vec<u8>> = Cursor::new(decompressed_data);
                            let df = JsonReader::new(cursor).finish().unwrap();
                            let mut df = df.unnest(["Date"]).unwrap();
                            let df = df.rename("_isoformat", "Date").unwrap();
                            let mut df = df
                                .clone()
                                .lazy()
                                .select([
                                    col("Date")
                                        .str()
                                        .to_datetime(
                                            Some(TimeUnit::Microseconds),
                                            None,
                                            StrptimeOptions::default(),
                                            lit("raise"),
                                        )
                                        .alias("DateTime"),
                                    col("*"),
                                ])
                                .drop_columns(["Date"])
                                .collect()?;

                            let old_cols: Vec<String> = df
                                .get_column_names()
                                .iter()
                                .map(|s| s.to_owned().to_owned())
                                .collect();

                            old_cols.iter().for_each(|old| {
                                if old != "DateTime" {
                                    df.rename(old, &format!("{}_{}", sravz_id, old))
                                        .expect(format!("cannot rename column {}", old).as_str());
                                }
                            });

                            // Sort by date desc
                            df = df.sort(["DateTime"], false, true)?;
                            // let df_with_constant = df.apply(|name| format!("{}{}", constant_string, name));
                            self.dataframe_map.insert(sravz_id, df.clone());
                            info!("Dateframe Head {}", df.head(Some(10)));
                            // info!("Dateframe Tail {}", df.tail(Some(10)));
                            // dbg!(df);
                            return Ok(Some(df.clone()));
                        }
                        Err(err) => {
                            info!("Error during decompression: {:?}", err);
                        }
                    }
                }
                Err(error) => {
                    return Err(Box::new(error));
                }
            }
        }
        return Ok(None);
    }

    /* Save data dataframe to s3 */
    pub async fn save_dataframe_to_s3(
        &mut self,
        df: &DataFrame,
        object_key: &str,
    ) -> Result<Option<String>, Box<dyn Error>> {
        let bucket_name = "sravz-data";
        // Perform the GET request using the mock server URL
        let result = self.dataframe_to_json(&df).await;

        // Assert that the result is Ok and contains the expected response body
        // assert!(result.is_ok());
        match result.unwrap() {
            json_string => {
                self.s3_module
                    .upload_object(&bucket_name, object_key, &json_string)
                    .await
                    .unwrap();
                let presigned_url = self
                    .s3_module
                    .generate_presigned_url(bucket_name, object_key)
                    .await;
                match presigned_url.unwrap() {
                    url => return Ok(Some(url)),
                }
            }
        }
    }

    /* Get earning dataframe */
    pub async fn get_earnings_dataframe(
        &mut self,
        code: &str,
    ) -> Result<Option<DataFrame>, Box<dyn Error>> {
        let mut params = HashMap::new();
        let url_suffix = "api/calendar/earnings";
        params.insert("symbols", code);
        let today = Utc::now().naive_utc().date();
        let ten_years_ago = today - Duration::days(365 * 10);
        let formatted_date = ten_years_ago.format("%Y-%m-%d").to_string();
        params.insert("from", &formatted_date);
        // let result: Result<String, std::io::Error> = self.rest_client.get(url_suffix, &mut params).await;
        // assert!(result.is_ok());
        // assert_eq!(result.unwrap(), r#"{"result":"success"}"#);

        let result = self.rest_client.get(url_suffix, &mut params).await;
        if result.is_ok() {
            let data = result.unwrap();
            // Parse the JSON using serde_json
            let v: Value = serde_json::from_str(&data)?;

            // Extract the "earnings" array from the parsed JSON
            let earnings = &v["earnings"];
            if let Value::Array(earnings_array) = earnings {
                // Prepare vectors for each column
                let mut code = Vec::new();
                let mut report_date = Vec::new();
                let mut date = Vec::new();
                let mut before_after_market = Vec::new();
                let mut currency = Vec::new();
                let mut actual = Vec::new();
                let mut estimate = Vec::new();
                let mut difference = Vec::new();
                let mut percent = Vec::new();

                // Iterate over the "earnings" array and collect the data
                for entry in earnings_array {
                    if let (
                        Some(code_str),
                        Some(report_date_str),
                        Some(date_str),
                        Some(bam_str),
                        Some(currency_str),
                        Some(actual_val),
                        Some(estimate_val),
                        Some(difference_val),
                        Some(percent_val),
                    ) = (
                        entry["code"].as_str(),
                        entry["report_date"].as_str(),
                        entry["date"].as_str(),
                        entry["before_after_market"].as_str(),
                        entry["currency"].as_str(),
                        entry["actual"].as_f64(),
                        entry["estimate"].as_f64(),
                        entry["difference"].as_f64(),
                        entry["percent"].as_f64(),
                    ) {
                        code.push(code_str.to_string());
                        report_date.push(report_date_str.to_string());
                        date.push(date_str.to_string());
                        before_after_market.push(bam_str.to_string());
                        currency.push(currency_str.to_string());
                        actual.push(actual_val);
                        estimate.push(estimate_val);
                        difference.push(difference_val);
                        percent.push(percent_val);
                    } else {
                        eprintln!("Error: One or more fields are missing or have incorrect types in entry: {:?}", entry);
                    }
                }

                // Create the DataFrame from the collected vectors
                let df = DataFrame::new(vec![
                    Series::new("code", code),
                    Series::new("report_date", report_date),
                    Series::new("date", date),
                    Series::new("before_after_market", before_after_market),
                    Series::new("currency", currency),
                    Series::new("actual", actual),
                    Series::new("estimate", estimate),
                    Series::new("difference", difference),
                    Series::new("percent", percent),
                ])?;

                // Print the DataFrame
                println!("{:?}", df);
                return Ok(Some(df.clone()));
            } else {
                println!("Earnings field is not an array");
            }
        }
        return Ok(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_historical_dataframe() {
        let mut data_frame_cache: DataFrameCache = DataFrameCache::new();

        // Perform the GET request using the mock server URL
        let result = data_frame_cache
            .get_dataframe("stk_us_nvda".to_string())
            .await;

        // Assert that the result is Ok and contains the expected response body
        // assert!(result.is_ok());
        match result.unwrap() {
            Some(df) => {
                println!("Dateframe Head {}", df.head(Some(10)));
            }
            None => {
                println!("No DataFrame found");
            }
        }
    }

    #[tokio::test]
    async fn test_get_earnings_dataframe() {
        let mut data_frame_cache: DataFrameCache = DataFrameCache::new();

        // Perform the GET request using the mock server URL
        let result = data_frame_cache.get_earnings_dataframe("NVDA").await;

        // Assert that the result is Ok and contains the expected response body
        // assert!(result.is_ok());
        match result.unwrap() {
            Some(df) => {
                info!("Dateframe Head {}", df.head(Some(10)));
            }
            None => {
                println!("No DataFrame found");
            }
        }
    }

    #[tokio::test]
    async fn test_dataframe_to_json() {
        let mut data_frame_cache: DataFrameCache = DataFrameCache::new();

        let df = df![
            "DateTime" => &["2023-01-01", "2023-01-02"],
            "stk_us_nvda_AdjustedClose" => &[100.0, 105.0]
        ];

        // Perform the GET request using the mock server URL
        let result = data_frame_cache.dataframe_to_json(&df.unwrap()).await;

        // Assert that the result is Ok and contains the expected response body
        // assert!(result.is_ok());
        match result.unwrap() {
            json_string => {
                println!("Dateframe JSON {}", json_string);
            }
        }
    }

    #[tokio::test]
    async fn test_dataframe_to_parquet() {
        let mut data_frame_cache: DataFrameCache = DataFrameCache::new();

        let df = df![
            "DateTime" => &["2023-01-01", "2023-01-02"],
            "stk_us_nvda_AdjustedClose" => &[100.0, 105.0]
        ];

        // Perform the GET request using the mock server URL
        let result = data_frame_cache.dataframe_to_parquet(df.unwrap()).await;

        // Assert that the result is Ok and contains the expected response body
        // assert!(result.is_ok());
        match result.unwrap() {
            Some(file_path) => {
                println!("Parquet file path: {}", file_path);
            }
            None => {
                println!("No DataFrame found");
            }
        }
    }

    #[tokio::test]
    async fn test_save_dataframe_to_s3() {
        let mut data_frame_cache: DataFrameCache = DataFrameCache::new();

        let df = df![
            "DateTime" => &["2023-01-01", "2023-01-02"],
            "stk_us_nvda_AdjustedClose" => &[100.0, 105.0]
        ];

        // Perform the GET request using the mock server URL
        let result = data_frame_cache
            .save_dataframe_to_s3(&df.unwrap(), "trash/test-df.json")
            .await;

        match result.unwrap() {
            Some(presigned_url) => {
                println!("Dateframe presigned url {}", presigned_url);
            }
            None => {
                println!("No DataFrame found");
            }
        }
    }
}
