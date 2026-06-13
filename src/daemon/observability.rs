//! Tracing subscriber init with optional OTel/OTLP export.
//!
//! Default: stdout `fmt` only, gated by `RUST_LOG`/`grimoire=info`. With
//! `--features otel` and `OTEL_EXPORTER_OTLP_ENDPOINT` set, also installs a
//! `tracing_opentelemetry` layer shipping spans over OTLP/HTTP. Standard OTel
//! env vars apply:
//!
//! - `OTEL_EXPORTER_OTLP_ENDPOINT` — base URL (e.g. `http://localhost:4318`).
//! - `OTEL_SERVICE_NAME` — resource attribute; defaults to `grimoire`.
//! - `OTEL_TRACES_SAMPLER_ARG` — TraceIdRatio fraction `[0.0, 1.0]`; default 1.0.
//!
//! Exporter-construction errors fall back to fmt-only rather than aborting
//! boot — observability must never take the system down.

use tracing_subscriber::EnvFilter;

fn default_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "grimoire=info".parse().expect("valid static env filter"))
}

/// Initialise the global subscriber. Call exactly once before any tracing
/// macros fire.
pub fn init() {
    #[cfg(feature = "otel")]
    {
        if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
            match init_with_otel() {
                Ok(()) => return,
                Err(e) => {
                    // No subscriber yet, so `eprintln!` not `tracing::warn!`.
                    #[allow(clippy::print_stderr)]
                    {
                        eprintln!("otel init failed, falling back to fmt-only: {e}");
                    }
                }
            }
        }
    }
    init_fmt_only();
}

fn init_fmt_only() {
    tracing_subscriber::fmt()
        .with_env_filter(default_env_filter())
        .init();
}

/// Holds the active provider so [`shutdown`] can flush it. opentelemetry 0.30
/// dropped the global `shutdown_tracer_provider()`, so the owner must keep the
/// provider and call `.shutdown()` explicitly.
#[cfg(feature = "otel")]
static TRACER_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

#[cfg(feature = "otel")]
fn init_with_otel() -> anyhow::Result<()> {
    use opentelemetry::KeyValue;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")?;
    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "grimoire".to_string());
    let sampler_arg: f64 = std::env::var("OTEL_TRACES_SAMPLER_ARG")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1.0);

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/traces", endpoint.trim_end_matches('/')))
        .build()?;

    let resource = Resource::builder()
        .with_service_name(service_name)
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build();

    // 0.30+ batch processor has its own thread, so no runtime arg.
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::TraceIdRatioBased(sampler_arg.clamp(0.0, 1.0)))
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("grimoire");
    opentelemetry::global::set_tracer_provider(provider.clone());
    let _ = TRACER_PROVIDER.set(provider);

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let fmt_layer = tracing_subscriber::fmt::layer();

    tracing_subscriber::registry()
        .with(default_env_filter())
        .with(fmt_layer)
        .with(otel_layer)
        .try_init()?;

    Ok(())
}

/// Flush + shut down the global tracer provider. Call from the daemon
/// shutdown path so in-flight spans reach the Collector before exit.
/// No-op when the `otel` feature is disabled.
#[allow(clippy::missing_const_for_fn)] // non-const under `--features otel`
pub fn shutdown() {
    #[cfg(feature = "otel")]
    {
        if let Some(provider) = TRACER_PROVIDER.get()
            && let Err(e) = provider.shutdown()
        {
            #[allow(clippy::print_stderr)]
            {
                eprintln!("otel tracer provider shutdown failed: {e}");
            }
        }
    }
}
