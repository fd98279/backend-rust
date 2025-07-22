use chrono::{DateTime, Duration, Utc};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};

use rusoto_core::credential::AwsCredentials;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::util::{PreSignedRequest, PreSignedRequestOption};
use rusoto_s3::{
    CreateBucketRequest, DeleteObjectRequest, GetObjectRequest, HeadObjectError, HeadObjectRequest,
    ListObjectsV2Request, PutObjectRequest, S3Client, StreamingBody, S3,
};
use std::fs::File;
use std::io::Write;
use std::{
    env,
    io::{self, Read},
};
use tokio::io::AsyncReadExt;

pub struct S3Module {
    client: S3Client,
    region: Region,
    access_key: String,
    secret_key: String,
}

impl S3Module {
    pub fn new() -> Self {
        let custom_endpoint = "usc1.contabostorage.com";
        let access_key =
            env::var("CONTABO_KEY").expect("CONTABO_KEY not found in environment variables");
        let secret_key =
            env::var("CONTABO_SECRET").expect("CONTABO_SECRET not found in environment variables");
        let region = Region::Custom {
            name: "custom".to_owned(),
            endpoint: custom_endpoint.to_owned(),
        };
        let client = S3Client::new_with(
            rusoto_core::request::HttpClient::new().expect("Failed to create HTTP client"),
            rusoto_core::credential::StaticProvider::new_minimal(
                access_key.clone(),
                secret_key.clone(),
            ),
            region.clone(),
        );
        Self {
            client,
            region,
            access_key,
            secret_key,
        }
    }

    #[allow(dead_code)]
    pub async fn create_bucket(&self, bucket_name: &str) {
        // Create a new bucket
        let create_bucket_request = CreateBucketRequest {
            bucket: bucket_name.to_string(),
            ..Default::default()
        };

        self.client
            .create_bucket(create_bucket_request)
            .await
            .expect("Failed to create bucket");
    }

    #[allow(dead_code)]
    pub async fn list_objects(&self, bucket_name: &str) {
        // List objects in the bucket
        let list_objects_request = ListObjectsV2Request {
            bucket: bucket_name.to_string(),
            ..Default::default()
        };

        let result = self
            .client
            .list_objects_v2(list_objects_request)
            .await
            .expect("Failed to list objects");

        println!("Objects in the bucket:");
        for obj in result.contents.unwrap_or_default() {
            println!("{}", obj.key.unwrap_or_default());
        }
    }

    #[allow(dead_code)]
    pub async fn upload_object(
        &self,
        bucket_name: &str,
        object_key: &str,
        content: &str,
    ) -> Result<(), io::Error> {
        let put_object_request: PutObjectRequest = PutObjectRequest {
            bucket: bucket_name.to_string(),
            key: object_key.to_string(),
            body: Some(StreamingBody::from(self.compress_string(content).unwrap())),
            content_encoding: Some("gzip".to_owned()),
            content_type: Some("application/json".to_owned()),
            ..Default::default()
        };

        self.client
            .put_object(put_object_request)
            .await
            .expect("Failed to upload object");

        Ok(())
    }

    pub async fn generate_presigned_url(
        &self,
        bucket_name: &str,
        object_key: &str,
    ) -> Result<String, std::io::Error> {
        let options = PreSignedRequestOption {
            expires_in: std::time::Duration::from_secs(300),
        };
        let req = GetObjectRequest {
            bucket: bucket_name.to_string(),
            key: object_key.to_string(),
            ..Default::default()
        };

        // If you have a session token (for temporary credentials), add it here, otherwise use None.
        let session_token: Option<String> = None;

        // Optionally, you can set an expiration time. For example, set expiration 1 hour from now.
        let expiration = Utc::now() + Duration::hours(1);

        // Create an instance of AwsCredentials
        let credentials = AwsCredentials::new(
            self.access_key.to_string(), // AWS Access Key
            self.secret_key.to_string(), // AWS Secret Key
            session_token,               // Optional session token
            Some(expiration),            // Optional expiration time
        );
        let url = req.get_presigned_url(&self.region, &credentials, &options);

        Ok(url.to_string())
    }

    #[allow(dead_code)]
    pub fn compress_string(&self, input: &str) -> Result<Vec<u8>, std::io::Error> {
        // Create a Gzip encoder
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());

        // Write the input string to the encoder
        encoder.write_all(input.as_bytes())?;

        // Finish the encoding and retrieve the compressed data
        let compressed_data = encoder.finish()?;

        Ok(compressed_data)
    }

    pub fn decompress_gzip(&self, compressed_data: Vec<u8>) -> Result<Vec<u8>, io::Error> {
        // Create a GzDecoder and feed the compressed data into it
        let mut decoder = GzDecoder::new(compressed_data.as_slice());
        // Read the decompressed data into a Vec<u8>
        let mut decompressed_data = Vec::new();
        decoder.read_to_end(&mut decompressed_data)?;

        Ok(decompressed_data)
    }

    pub async fn download_object(
        &self,
        bucket_name: &str,
        object_key: &str,
        decompress: bool,
    ) -> Result<Vec<u8>, io::Error> {
        // Download an object from the bucket
        let get_object_request = GetObjectRequest {
            bucket: bucket_name.to_string(),
            key: object_key.to_string(),
            ..Default::default()
        };

        match self.client.get_object(get_object_request).await {
            Ok(response) => {
                let body = response.body.expect("Object body not found");

                let mut bytes = Vec::new();
                body.into_async_read()
                    .read_to_end(&mut bytes)
                    .await
                    .expect("Failed to read object body");

                if decompress {
                    match self.decompress_gzip(bytes) {
                        Ok(data) => {
                            return Ok(data);
                        }
                        Err(error) => {
                            return Err(error);
                        }
                    }
                }
                Ok(bytes)
            }
            Err(error) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Service error: {:?}", error),
            )),
        }
    }

    #[allow(dead_code)]
    pub async fn delete_object(&self, bucket_name: &str, object_key: &str) {
        // Delete an object from the bucket
        let delete_object_request = DeleteObjectRequest {
            bucket: bucket_name.to_string(),
            key: object_key.to_string(),
            ..Default::default()
        };

        self.client
            .delete_object(delete_object_request)
            .await
            .expect("Failed to delete object");
    }

    pub async fn read_local_file(&self, file_path: &str) -> Result<Vec<u8>, std::io::Error> {
        let mut file = File::open(file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    pub async fn upload_file(
        &self,
        bucket_name: &str,
        object_key: &str,
        file_path: &str,
    ) -> Result<(), std::io::Error> {
        // Read the local file content
        // let file_content = self.read_local_file(file_path).await?;
        let file_content = match self.read_local_file(file_path).await {
            Ok(value) => value,
            Err(error) => {
                return Err(error);
            }
        };

        let put_object_request = PutObjectRequest {
            bucket: bucket_name.to_string(),
            key: object_key.to_string(),
            body: Some(file_content.into()),
            ..Default::default()
        };

        match self.client.put_object(put_object_request).await {
            Ok(_) => Ok(()),
            Err(error) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to put object: {:?}", error),
            )),
        }
    }

    async fn object_exists(&self, bucket: &str, key: &str) -> Result<bool, std::io::Error> {
        let head_req = HeadObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        };

        match self.client.head_object(head_req).await {
            Ok(_) => Ok(true), // Object exists
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => Ok(false), // Object does not exist
            Err(RusotoError::Unknown(response)) if response.status.as_u16() == 404 => {
                Ok(false) // Handle other cases where a 404 status indicates the object or bucket doesn't exist
            }
            Err(e) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to check if object exists: {:?}", e),
            )), // Some other error occurred
        }
    }

    pub async fn is_blob_older_than_mins(
        &self,
        bucket: &str,
        key: &str,
        mins: i64,
    ) -> Result<bool, std::io::Error> {
        let head_req = HeadObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        };

        match self.client.head_object(head_req).await {
            Ok(result) => {
                // Get the LastModified date from the metadata
                if let Some(last_modified) = result.last_modified {
                    // Parse the LastModified date into a chrono::DateTime object
                    // match last_modified.parse::<chrono::DateTime<Utc>>() {
                    match DateTime::parse_from_rfc2822(&last_modified)
                        .map(|dt| dt.with_timezone(&Utc))
                    {
                        Ok(last_modified_date) => {
                            let cutoff_date = Utc::now() - Duration::minutes(mins);

                            // Compare the LastModified date with the cutoff date
                            Ok(last_modified_date < cutoff_date)
                        } // Successfully parsed the date
                        Err(e) => Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("Unable to parse data: {:?}", e),
                        )),
                    }
                } else {
                    Ok(false)
                }
            } // Object exists
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => Ok(false), // Object does not exist
            Err(RusotoError::Unknown(response)) if response.status.as_u16() == 404 => {
                Ok(false) // Handle other cases where a 404 status indicates the object or bucket doesn't exist
            }
            Err(e) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to check if object exists: {:?}", e),
            )), // Some other error occurred
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::{error, info};
    use serde_json::json;
    use std::error::Error;
    use std::io;
    use tempfile::NamedTempFile;
    use tokio::test;

    // Test the create_bucket method
    #[test]
    async fn test_create_bucket() {
        let s3_module = S3Module::new();

        // Generate a unique bucket name for testing
        let bucket_name = format!("test-bucket-{}", uuid::Uuid::new_v4());

        // Ensure the bucket is created without errors
        s3_module.create_bucket(&bucket_name).await;
    }

    // Test the list_objects method
    #[test]
    async fn test_list_objects() {
        let s3_module = S3Module::new();

        // Provide an existing bucket name for testing
        let bucket_name = "existing-bucket";

        // Ensure listing objects in the bucket doesn't result in errors
        s3_module.list_objects(&bucket_name).await;
    }

    // Test the upload_object and download_object methods
    #[test]
    async fn test_upload_and_download_object() {
        let s3_module = S3Module::new();

        // Provide a unique bucket and object key for testing
        let bucket_name = "sravz";
        let object_key = "trash/test-object.json";
        // let content = "{\"Message\": \"Hello, this is a test content.\"}";
        // Create a JSON string literal
        let json_string_literal = json!({
            "name": "John Doe",
            "age": 30,
            "city": "Example City",
            "is_student": false,
            "grades": [90, 85, 92]
        });

        // Convert the JSON string literal to a String
        let content = json_string_literal.to_string();
        // Upload an object to S3
        s3_module
            .upload_object(&bucket_name, object_key, &content)
            .await
            .unwrap();

        // Download the object from S3
        let downloaded_content = s3_module
            .download_object(&bucket_name, object_key, true)
            .await
            .unwrap();

        // Ensure the downloaded content matches the original content
        assert_eq!(downloaded_content, content.as_bytes().to_vec());
    }

    // Test the delete_object method
    #[test]
    async fn test_delete_object() {
        let s3_module = S3Module::new();

        // Provide a unique bucket and object key for testing
        let bucket_name = "sravz";
        let object_key = "trash/test-object-delete.json";

        // Upload an object to S3
        s3_module
            .upload_object(&bucket_name, object_key, "")
            .await
            .unwrap();

        // Ensure the object is deleted without errors
        s3_module.delete_object(&bucket_name, object_key).await;
    }

    #[tokio::test]
    async fn test_object_exists_true() {
        let s3_module = S3Module::new();

        // Provide a unique bucket and object key for testing
        let bucket_name = "sravz";
        let object_key = "trash/test-object.json";
        // let content = "{\"Message\": \"Hello, this is a test content.\"}";
        // Create a JSON string literal
        let json_string_literal = json!({
            "name": "John Doe",
            "age": 30,
            "city": "Example City",
            "is_student": false,
            "grades": [90, 85, 92]
        });

        // Convert the JSON string literal to a String
        let content = json_string_literal.to_string();
        // Upload an object to S3
        s3_module
            .upload_object(&bucket_name, object_key, &content)
            .await
            .unwrap();

        let exists = s3_module
            .object_exists(bucket_name, object_key)
            .await
            .unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn test_object_exists_false() {
        let s3_module = S3Module::new();
        let bucket_name = "sravz";
        let object_key = "trash/test-object1.json";

        let exists = s3_module
            .object_exists(bucket_name, object_key)
            .await
            .unwrap();
        assert!(!exists);
    }

    #[tokio::test]
    async fn test_generate_presigned_url() {
        let s3_module = S3Module::new();
        let bucket_name = "sravz";
        let object_key = "trash/test-object1.json";

        let presinged_url = s3_module
            .generate_presigned_url(bucket_name, object_key)
            .await
            .unwrap();
        println!("Genereated Presigned URL: {}", presinged_url);
    }

    // #[tokio::test]
    // async fn test_object_exists_error() {
    //     // Mocking S3 response with an error
    //     let mock_s3_client = S3Client::new_with(
    //         MockRequestDispatcher::default()
    //             .with_status(StatusCode::INTERNAL_SERVER_ERROR)
    //             .with_body("Internal Server Error"),
    //         MockResponseReader::new(),
    //         Region::UsEast1,
    //     );

    //     let bucket = "test-bucket";
    //     let key = "any-object";

    //     let result = object_exists(bucket, key).await;
    //     assert!(result.is_err());
    // }

    #[test]
    async fn test_upload_file() -> io::Result<()> {
        let s3_module = S3Module::new();

        // Provide a unique bucket and object key for testing
        let bucket_name = "sravz";
        let object_key = "trash/test-file.json";

        // Create a temporary file
        let mut temp_file = NamedTempFile::new()?;

        // Write some content to the temporary file
        writeln!(temp_file, "Test content")?;

        // Get the path to the temporary file
        let file_path = temp_file.path().to_owned();

        // Converting PathBuf to &str
        if let Some(path_str) = file_path.as_path().to_str() {
            // Now you can pass the &str to the function
            // Upload an object to S3
            match s3_module
                .upload_file(&bucket_name, object_key, path_str)
                .await
            {
                Ok(_) => {
                    info!("File uploaded");
                }
                Err(e) => {
                    error!("Error {:?}", e)
                }
            }
        } else {
            println!("Invalid path");
        }

        // Ensure the object is deleted without errors
        s3_module.delete_object(&bucket_name, object_key).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_is_blob_older_than_mins_blob_is_older() -> Result<(), Box<dyn Error>> {
        // Mock the S3 client
        let s3_module = S3Module::new();

        // Provide a unique bucket and object key for testing
        let bucket_name = "sravz";
        let object_key = "trash/test-object.json";

        // Delete if the object exists
        let exists = s3_module
            .object_exists(bucket_name, object_key)
            .await
            .unwrap();

        if exists {
            s3_module.delete_object(&bucket_name, object_key).await;
        }

        // let content = "{\"Message\": \"Hello, this is a test content.\"}";
        // Create a JSON string literal
        let json_string_literal = json!({
            "name": "John Doe",
            "age": 30,
            "city": "Example City",
            "is_student": false,
            "grades": [90, 85, 92]
        });

        // Convert the JSON string literal to a String
        let content = json_string_literal.to_string();
        // Upload an object to S3
        s3_module
            .upload_object(&bucket_name, object_key, &content)
            .await
            .unwrap();

        // Call the method and verify the result
        let is_older = s3_module
            .is_blob_older_than_mins("sravz", "trash/test-object.json", 5)
            .await?;
        assert!(!is_older);

        // Delete if the object exists
        let exists = s3_module
            .object_exists(bucket_name, object_key)
            .await
            .unwrap();

        if exists {
            s3_module.delete_object(&bucket_name, object_key).await;
        }

        Ok(())
    }
}
