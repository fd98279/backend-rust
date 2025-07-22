use std::collections::HashMap;
use std::io;

use crate::config::AppConfig;
use crate::s3_service::S3Module;

pub struct RestClient {
    s3_module: S3Module,
    config: AppConfig,
    client: reqwest::Client,
}

impl<'a> RestClient {
    pub fn new() -> Self {
        let s3_module = S3Module::new();
        let config = match AppConfig::new() {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Error: {}", err);
                // Handle the error or exit the program
                std::process::exit(1);
            }
        };
        RestClient {
            s3_module,
            config,
            client: reqwest::Client::new(),
        }
    }
    pub async fn get<'b>(
        &'b self,
        url_suffix: &str,
        params: &'b mut HashMap<&'b str, &'b str>,
    ) -> Result<String, io::Error> {
        // The URL you want to send the GET request to
        // api/calendar/earnings
        let key = format!("eod/{}", url_suffix);

        match self
            .s3_module
            .is_blob_older_than_mins("sravz-data", &key, 3 * 30 * 24 * 60)
            .await
        {
            Ok(is_older) => {
                if is_older {
                    match self
                        .s3_module
                        .download_object("sravz-data", &key, true)
                        .await
                    {
                        Ok(data) => {
                            // Convert Vec<u8> to String
                            match String::from_utf8(data) {
                                Ok(string_data) => Ok(string_data),
                                Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
                            }
                        }
                        Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
                    }
                } else {
                    let url = format!("{}{}", self.config.data_provider_url, url_suffix);

                    params.insert("api_token", &self.config.eodhistoricaldata_api_key.as_str());
                    params.insert("fmt", "json");
                    // println!("URL: {}", url);
                    // println!("Params: {:?}", params);
                    let response = self.client.get(&url).query(&params).send().await;

                    match response {
                        Ok(resp) => {
                            if resp.status().is_success() {
                                let body = resp
                                    .text()
                                    .await
                                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                                // Upload the data to S3
                                match params.get("symbols") {
                                    Some(symbol) => {
                                        let s3_key = format!("eod/{}/{}.json", url_suffix, symbol);
                                        self.s3_module
                                            .upload_object("sravz-data", &s3_key, &body)
                                            .await
                                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                                    }
                                    None => println!("The key 'symbols' is not present."),
                                }
                                Ok(body)
                            } else {
                                Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("Request failed with status: {}", resp.status()),
                                ))
                            }
                        }
                        Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
                    }
                }
            }
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use mockito::{mock, Matcher};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_get_success() {
        // Set up the mock server
        let _mock_server = mock("GET", "/api/some_endpoint")
            .match_query(Matcher::UrlEncoded(
                "api_token".into(),
                "test_api_key".into(),
            ))
            .match_query(Matcher::UrlEncoded("fmt".into(), "json".into()))
            .with_status(200)
            .with_body(r#"{"result":"success"}"#)
            .create();

        let rest_client = RestClient::new();

        // Prepare the query parameters
        let mut params = HashMap::new();
        let url_suffix = "some_endpoint";

        // Perform the GET request using the mock server URL
        let result = rest_client.get(url_suffix, &mut params).await;

        // Assert that the result is Ok and contains the expected response body
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), r#"{"result":"success"}"#);

        // Ensure the mock server received the request
        _mock_server.assert();
    }

    #[tokio::test]
    async fn test_get_failure() {
        // Set up the mock server to return a 500 error
        let _mock_server = mock("GET", "/api/some_endpoint")
            .match_query(Matcher::UrlEncoded(
                "api_token".into(),
                "test_api_key".into(),
            ))
            .match_query(Matcher::UrlEncoded("fmt".into(), "json".into()))
            .with_status(500)
            .with_body("Internal Server Error")
            .create();

        let rest_client = RestClient::new();

        // Prepare the query parameters
        let mut params = HashMap::new();
        let url_suffix = "some_endpoint";

        // Perform the GET request using the mock server URL
        let result = rest_client.get(url_suffix, &mut params).await;

        // Assert that the result is an error
        assert!(result.is_err());

        // Ensure the mock server received the request
        _mock_server.assert();
    }

    #[tokio::test]
    async fn test_get_earning_nvidia() {
        let rest_client = RestClient::new();
        let mut params = HashMap::new();
        let url_suffix = "api/calendar/earnings";
        params.insert("symbols", "NVDA");
        let today = Utc::now().naive_utc().date();
        let ten_years_ago = today - Duration::days(365 * 10);
        let formatted_date = ten_years_ago.format("%Y-%m-%d").to_string();
        params.insert("from", &formatted_date);
        let result = rest_client.get(url_suffix, &mut params).await;
        assert!(result.is_ok());
        // assert_eq!(result.unwrap(), r#"{"result":"success"}"#);
    }
}
