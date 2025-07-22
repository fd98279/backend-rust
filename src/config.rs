use serde_derive::Deserialize;
use std::env;
use std::fs;
use std::process::exit;

// Top level struct to hold the TOML data.
#[derive(Deserialize)]
struct Data {
    config: Config,
}

// Config struct holds to data from the `[config]` section.
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub backend_rust_topic: String,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub node_env: String,
    pub nsq_host: String,
    pub nsq_lookupd_host: String,
    pub mongolab_uri: String,
    pub contabo_bucket: String,
    pub contabo_bucket_key: String,
    pub contabo_object_url_prefix: String,
    pub eodhistoricaldata_api_key: String,
    pub eodhistoricaldata_api_key2: String,
    pub data_provider_url: String,
    pub config: Config,
}

// Helper function to fetch environment variables
fn env_var(key: &str) -> Result<String, &'static str> {
    env::var(key).or_else(|_| Err("Environment variable not set"))
}

fn read_config_file(file_path: &str) -> Result<Data, toml::de::Error> {
    let contents = match fs::read_to_string(file_path) {
        // If successful return the files text as `contents`.
        // `c` is a local variable.
        Ok(c) => c,
        // Handle the `error` case.
        Err(_) => {
            // Write `msg` to `stderr`.
            eprintln!("Could not read the config file `{}`", file_path);
            // Exit the program with exit code `1`.
            exit(1);
        }
    };
    let data: Data = toml::from_str(&contents)?;
    Ok(data)
}

impl AppConfig {
    pub fn new() -> Result<Self, &'static str> {
        // Fetch environment variables
        let nsq_host = env_var("NSQ_HOST")?;
        let nsq_lookupd_host = env_var("NSQ_LOOKUPD_HOST")?;
        let mongolab_uri = env_var("MONGOLAB_URI")?;
        let node_env = env_var("NODE_ENV")?;
        let contabo_bucket = "sravz".to_string();
        let contabo_bucket_key = "rust-backend".to_string();
        let contabo_object_url_prefix = format!(
            "https://usc1.contabostorage.com/adc59f4bb6a74373a1ebd286a7b11b60:{}/{}/",
            contabo_bucket, contabo_bucket_key
        );
        let eodhistoricaldata_api_key = env_var("EODHISTORICALDATA_API_KEY")?;
        let eodhistoricaldata_api_key2 = env_var("EODHISTORICALDATA_API_KEY2")?;

        let config_file_name = format!("config.{}.toml", node_env);

        let data: Data = match read_config_file(&config_file_name) {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Error reading config: {}", err);
                exit(1);
            }
        };

        // Create and return an AppConfig instance
        Ok(AppConfig {
            node_env,
            nsq_host,
            nsq_lookupd_host,
            mongolab_uri,
            contabo_bucket,
            contabo_bucket_key,
            contabo_object_url_prefix,
            config: data.config,
            eodhistoricaldata_api_key,
            eodhistoricaldata_api_key2,
            data_provider_url: "https://eodhistoricaldata.com/".to_string(),
        })
    }
}
