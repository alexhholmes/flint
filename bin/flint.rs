use flintdb::config::Config;
use flintdb::server::Server;

#[tokio::main]
pub async fn main() {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "flintdb=info".into())
        )
        .init();

    let config = Config::from_args();
    let server = Server::new(config);
    server.start().await;
}