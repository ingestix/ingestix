use arc_swap::ArcSwap;
use ingestix::{
    ApiKeyConfig, FlowWorker, HttpConfig, HttpIngestor, HttpQueuePolicy, Ingestix, SharedContext,
};
use serde_json::Value;
use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use surrealdb::{
    Surreal,
    engine::any::{Any, connect},
    opt::auth::Root,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AppConfig {
    endpoint: String,
    namespace: String,
    database: String,
    root_user: String,
    root_password: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            endpoint: "127.0.0.1:8000".to_string(),
            namespace: "ingestix".to_string(),
            database: "ingestix".to_string(),
            root_user: "root".to_string(),
            // Kept for backwards compatibility with older README instructions.
            // Prefer `SURREALDB_ROOT_PASSWORD` in new deployments.
            root_password: "CHANGE_ME".to_string(),
        }
    }
}

impl AppConfig {
    fn from_env() -> anyhow::Result<Self> {
        let endpoint =
            env::var("SURREALDB_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:8000".to_string());
        let namespace = env::var("SURREALDB_NAMESPACE").unwrap_or_else(|_| "ingestix".to_string());
        let database = env::var("SURREALDB_DATABASE").unwrap_or_else(|_| "ingestix".to_string());
        let root_user = env::var("SURREALDB_ROOT_USER").unwrap_or_else(|_| "root".to_string());
        let root_password = env::var("SURREALDB_ROOT_PASSWORD").map_err(|_| {
            anyhow::anyhow!(
                "Missing SURREALDB_ROOT_PASSWORD env var (required to sign in to SurrealDB)"
            )
        })?;

        Ok(Self {
            endpoint,
            namespace,
            database,
            root_user,
            root_password,
        })
    }
}

#[derive(Clone)]
struct AppState {
    db: Arc<ArcSwap<Surreal<Any>>>,
}

impl AppState {
    fn surreal(&self) -> Arc<Surreal<Any>> {
        self.db.load_full()
    }
}

async fn init_surreal_database(config: &AppConfig) -> anyhow::Result<AppState> {
    let endpoint = format!("ws://{}", config.endpoint);
    let db = connect(endpoint.as_str()).await?;

    db.signin(Root {
        username: config.root_user.clone(),
        password: config.root_password.clone(),
    })
    .await?;

    db.query(format!(
        "DEFINE NAMESPACE IF NOT EXISTS {};",
        config.namespace
    ))
    .await?;

    db.use_ns(&config.namespace)
        .use_db(&config.database)
        .await?;

    db.query(format!(
        "DEFINE DATABASE IF NOT EXISTS {};",
        config.database
    ))
    .await?;

    // Cheap startup health-check so the example fails fast on bad DB config.
    db.query("RETURN 1;").await?;

    // Ensure the `ingest` table exists, but avoid errors when the table already exists.
    // The worker stage will later save POST bodies into this table.
    let mut result = db.query("INFO FOR DB").await?;
    let info: Option<serde_json::Value> = result.take(0)?;
    let existing_tables: HashSet<String> = info
        .as_ref()
        .and_then(|i| i.get("tables"))
        .and_then(|t| t.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default();

    if !existing_tables.contains("ingest") {
        db.query("DEFINE TABLE ingest SCHEMAFULL;").await?;

        db.query("DEFINE FIELD time ON TABLE ingest TYPE datetime VALUE time::now();")
            .await?;
        db.query("DEFINE FIELD body ON TABLE ingest TYPE object FLEXIBLE;")
            .await?;

        tracing::info!("Created SurrealDB table 'ingest' with fields time + body.");
    } else {
        tracing::info!("SurrealDB table 'ingest' already exists; skipping schema creation.");
    }

    tracing::info!(
        "Connected to SurrealDB at ws://{} (ns='{}', db='{}')",
        config.endpoint,
        config.namespace,
        config.database
    );

    Ok(AppState {
        db: Arc::new(ArcSwap::new(Arc::new(db))),
    })
}

#[derive(FlowWorker)]
#[flow(message = "Value", config = "AppConfig", state = "AppState")]
struct LoggingWorker;

impl LoggingWorker {
    async fn handle(
        &self,
        msg: Value,
        ctx: Arc<SharedContext<AppConfig, AppState>>,
    ) -> anyhow::Result<()> {
        // Access the shared SurrealDB handle from Ingestix state.
        let db = ctx.state.surreal();

        // `body` is defined as TYPE object in the schema.
        // If a non-object JSON value arrives, wrap it to keep inserts valid.
        let body = if msg.is_object() {
            msg
        } else {
            serde_json::json!({ "value": msg })
        };

        db.query("CREATE ingest CONTENT { body: $body };")
            .bind(("body", body))
            .await?;

        tracing::info!("Worker stored payload in SurrealDB table 'ingest'");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Enable structured logs for both the ingestor and worker.
    tracing_subscriber::fmt::init();

    let config = AppConfig::from_env()?;
    let state = init_surreal_database(&config).await?;

    // Ingestix: concurrency=10 workers, channel buffer=100 messages.
    let runner = Ingestix::new(config, state, 10, 100);

    // Expose metrics and health endpoints.
    runner.spawn_monitor_server(8080).await?;
    tracing::info!("Monitor server running at http://localhost:8080");

    // HTTP endpoint with request guards:
    // - `require_json: true` rejects non-JSON Content-Type with 415.
    // - HTTP auth is enabled when `INGESTIX_API_KEY` is set.
    let ingestor = HttpIngestor::with_config(
        "0.0.0.0:3000".parse()?,
        "/ingest",
        HttpConfig {
            api_key: api_key_from_env(),
            request_timeout: Duration::from_secs(15),
            enqueue_timeout: Duration::from_secs(3),
            queue_policy: HttpQueuePolicy::Reject429,
            ..HttpConfig::default()
        },
    );

    if ingestor.api_key.is_none() {
        tracing::warn!("INGESTIX_API_KEY not set; HTTP auth is disabled for this example");
    }
    tracing::info!("Webhook ingestor listening at http://localhost:3000/ingest");

    // Start ingestion + worker processing loop.
    runner.launch(ingestor, LoggingWorker).await
}

fn api_key_from_env() -> Option<ApiKeyConfig> {
    let Ok(value) = env::var("INGESTIX_API_KEY") else {
        return None;
    };

    let value = value.trim().to_string();
    if value.is_empty() {
        None
    } else {
        Some(ApiKeyConfig::x_api_key(value))
    }
}
