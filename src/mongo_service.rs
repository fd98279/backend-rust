use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::ReplaceOptions;
use mongodb::Client;

use crate::models::Message;

pub struct Mongo {}

impl Mongo {
    #[allow(dead_code)]
    pub async fn create(&self, _create: Message, _mdb: &Client) {
        let db = _mdb.database("sravz");
        let _col = db.collection::<Message>("nsq_message_cache");
        _col.insert_one(_create, None)
            .await
            .expect("Unable to insert message");
    }

    #[allow(dead_code)]
    pub async fn find(
        &self,
        _create: Message,
        _mdb: &Client,
    ) -> Result<Vec<Message>, Box<dyn std::error::Error>> {
        let db = _mdb.database("sravz");
        let _col = db.collection::<Message>("nsq_message_cache");
        let messages: Vec<Message> = _col
            .find(doc! { "key": _create.key }, None)
            .await
            .expect("Unable to get Cursor")
            .try_collect()
            .await
            .expect("Unable to collect from items Cursor");
        println!("messages: {:?}", messages);
        Ok(messages)
    }

    pub async fn find_by_key(
        &self,
        key: String,
        _mdb: &Client,
    ) -> Result<Vec<Message>, Box<dyn std::error::Error>> {
        let db = _mdb.database("sravz");
        let _col = db.collection::<Message>("nsq_message_cache");
        let messages: Vec<Message> = _col
            .find(doc! { "key": key }, None)
            .await
            .expect("Unable to get Cursor")
            .try_collect()
            .await
            .expect("Unable to collect from items Cursor");
        println!("messages: {:?}", messages);
        Ok(messages)
    }

    #[allow(dead_code)]
    async fn update_many(&self, _create: Message, _mdb: &Client) {
        let client = Client::with_uri_str("mongodb://mongo:root@localhost:27017")
            .await
            .expect("Unable to connect to MongoDB");
        let db = client.database("sravz");
        let _col = db.collection::<Message>("nsq_message_cache");
        _col.update_many(
            doc! { "key": "8631c843c43d7d746de2b80ca4db2cdc" },
            doc! { "$set": { "date": "ISODate(\"2023-10-24T13:47:13.550+0000\")" } },
            None,
        )
        .await
        .expect("Unable to update messages");
    }

    pub async fn update_one(&self, doc: Message, _mdb: &Client) {
        let db = _mdb.database("sravz");
        let _col = db.collection::<Message>("nsq_message_cache");
        let mut options = ReplaceOptions::default();
        options.upsert = Some(true);
        // let replacement_doc = bson::to_document(&doc).unwrap();
        _col.replace_one(doc! { "key": &doc.key}, &doc, options)
            .await
            .expect("Unable to update messages");
    }

    #[allow(dead_code)]
    async fn delete_many(&self, _create: Message, _mdb: &Client) {
        let db = _mdb.database("sravz");
        let _col = db.collection::<Message>("nsq_message_cache");
        _col.delete_many(doc! { "key": "8631c843c43d7d746de2b80ca4db2cdc" }, None)
            .await
            .expect("Unable to delete messages");
        let messages: Vec<Message> = _col
            .find(doc! { "key": "8631c843c43d7d746de2b80ca4db2cdc" }, None)
            .await
            .expect("Unable to get Cursor")
            .try_collect()
            .await
            .expect("Unable to collect items from Cursor");
        println!("messages: {:?}", messages);
    }
}

#[cfg(test)]
mod tests {
    use crate::{models::Message, mongo_service::Mongo};
    use mongodb::Client;

    // src/lib.rs
    #[tokio::test]
    async fn test_mongo_create() {
        let client: Client = Client::with_uri_str("mongodb://sravz:sravz@mongo:27017/sravz")
            .await
            .expect("Unable to connect to MongoDB");
        // Utc.ymd(2020, 2, 7).and_hms(0, 0, 0)
        // "date" : "2023-10-27T15:45:00Z",
        let json_str = r#"{
            "id" : 21.1,
            "p_i" : {
                "args" : [
                    "idx_us_ndx"
                ],
                "kwargs" : {
                    "device" : "mobile",
                    "upload_to_aws" : true
                }
            },
            "t_o" : "production_backend-node8c6b43063463",
            "cid" : "VTTT7Qu_jaQn-IhAAAGP",
            "cache_message" : true,
            "stopic" : "pca",
            "ts" : 1698155232.5238342,
            "fun_n" : "get_correlation_analysis_tear_sheet_user_asset",
            "e" : "Error",
            "key" : "8631c843c43d7d746de2b80ca4db2cdc",
            "exception_message" : "Traceback (most recent call last):\n  File \"/home/airflow/src/services/kafka_helpers/message_contracts.py\", line 381, in get_output_message\n    data = MessageContracts.MESSAGEID_FUNCTION_MAP.get(\n  File \"/home/airflow/.local/lib/python3.8/site-packages/decorator.py\", line 232, in fun\n    return caller(func, *(extras + args), **kw)\n  File \"/home/airflow/src/util/helper.py\", line 187, in save_data_to_contabo\n    args_,kwargs = func(*args, **kwargs)\n  File \"/home/airflow/src/analytics/portfolio.py\", line 361, in get_correlation_analysis_tear_sheet_user_asset\n    df = pe.get_df_from_s3(sravz_id)\n  File \"/home/airflow/src/services/price_queries.py\", line 149, in get_df_from_s3\n    data_df = pd.read_csv(io.BytesIO(historical_price))\nTypeError: a bytes-like object is required, not 'list'\n"
        }"#;
        let result: Result<Message, serde_json::Error> = serde_json::from_str(json_str);
        // Handle the result using pattern matching
        let mongo = Mongo {};
        match result {
            Ok(_message) => {
                mongo.create(_message, &client).await;
            }
            Err(err) => {
                eprintln!("Deserialization failed: {}", err);
            }
        }
    }

    #[tokio::test]
    async fn test_mongo_find() {
        let client = Client::with_uri_str("mongodb://sravz:sravz@mongo:27017/sravz")
            .await
            .expect("Unable to connect to MongoDB");
        // Utc.ymd(2020, 2, 7).and_hms(0, 0, 0)
        // "date" : "2023-10-27T15:45:00Z",
        let json_str = r#"{
            "id" : 21.1,
            "p_i" : {
                "args" : [
                    "idx_us_ndx"
                ],
                "kwargs" : {
                    "device" : "mobile",
                    "upload_to_aws" : true
                }
            },
            "t_o" : "production_backend-node8c6b43063463",
            "cid" : "VTTT7Qu_jaQn-IhAAAGP",
            "cache_message" : true,
            "stopic" : "pca",
            "ts" : 1698155232.5238342,
            "fun_n" : "get_correlation_analysis_tear_sheet_user_asset",
            "e" : "Error",
            "key" : "8631c843c43d7d746de2b80ca4db2cdc",
            "exception_message" : "Traceback (most recent call last):\n  File \"/home/airflow/src/services/kafka_helpers/message_contracts.py\", line 381, in get_output_message\n    data = MessageContracts.MESSAGEID_FUNCTION_MAP.get(\n  File \"/home/airflow/.local/lib/python3.8/site-packages/decorator.py\", line 232, in fun\n    return caller(func, *(extras + args), **kw)\n  File \"/home/airflow/src/util/helper.py\", line 187, in save_data_to_contabo\n    args_,kwargs = func(*args, **kwargs)\n  File \"/home/airflow/src/analytics/portfolio.py\", line 361, in get_correlation_analysis_tear_sheet_user_asset\n    df = pe.get_df_from_s3(sravz_id)\n  File \"/home/airflow/src/services/price_queries.py\", line 149, in get_df_from_s3\n    data_df = pd.read_csv(io.BytesIO(historical_price))\nTypeError: a bytes-like object is required, not 'list'\n"
        }"#;
        let result: Result<Message, serde_json::Error> = serde_json::from_str(json_str);
        let mongo = Mongo {};
        // Handle the result using pattern matching
        match result {
            Ok(_message) => {
                let messages = mongo
                    .find(_message, &client)
                    .await
                    .expect("Document not found");
                assert!(messages.len() > 0, "1 or more messages should be found.");
            }
            Err(err) => {
                eprintln!("Deserialization failed: {}", err);
            }
        }
    }
}
