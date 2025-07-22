use std::error::Error;

use chrono::DateTime;
use chrono::Utc;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use serde_derive::Deserialize;
use serde_json::Value;
use std::fmt;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    // #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    // pub id: Option<ObjectId>,
    #[serde(rename = "id")]
    pub id: f64,
    #[serde(rename = "p_i")]
    pub p_i: PI,
    #[serde(rename = "t_o")]
    pub t_o: String,
    pub cid: String,
    #[serde(rename = "cache_message")]
    pub cache_message: bool,
    pub stopic: String,
    #[serde(skip)]
    pub ts: f64,
    #[serde(skip, rename = "fun_n")]
    pub fun_n: String,
    #[serde(rename = "d_o")]
    pub d_o: Option<DO>,
    #[serde(default)]
    pub e: String,
    #[serde(
        default,
        with = "bson::serde_helpers::chrono_datetime_as_bson_datetime"
    )]
    pub date: DateTime<Utc>,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub exception_message: String,
}

impl Error for Message {}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.exception_message)
    }
}

impl Message {
    pub fn update_s3_location(
        &mut self,
        contabo_bucket: String,
        contabo_object_url_prefix: String,
        file_name: String,
    ) {
        self.d_o = Some(DO {
            bucket_name: contabo_bucket.clone(),
            key_name: format!("{}{}", contabo_bucket.clone(), file_name),
            signed_url: format!("{}{}", contabo_object_url_prefix.clone(), file_name),
            data: serde_json::Value::String("".to_string()),
        });
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PI {
    pub args: Vec<String>,
    pub kwargs: Kwargs,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Kwargs {
    pub device: String,
    #[serde(rename = "upload_to_aws")]
    pub upload_to_aws: bool,
    #[serde(rename = "json_keys")]
    pub json_keys: String,
    #[serde(rename = "llm_query")]
    pub llm_query: String,
}

#[derive(Default, Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalQuote {
    #[serde(rename = "Date")]
    pub date: Date,
    #[serde(rename = "Volume")]
    pub volume: i64,
    #[serde(rename = "Open")]
    pub open: f64,
    #[serde(rename = "High")]
    pub high: f64,
    #[serde(rename = "Low")]
    pub low: f64,
    #[serde(rename = "Close")]
    pub close: f64,
    #[serde(rename = "AdjustedClose")]
    pub adjusted_close: f64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Date {
    #[serde(rename = "_isoformat")]
    pub isoformat: String,
}

impl Serialize for HistoricalQuote {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HistoricalQuote", 1)?;
        state.serialize_field("Date", &self.date.isoformat)?;
        state.end()
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DO {
    #[serde(rename = "bucket_name")]
    pub bucket_name: String,
    #[serde(rename = "key_name")]
    pub key_name: String,
    pub data: Value,
    #[serde(rename = "signed_url")]
    pub signed_url: String,
}
