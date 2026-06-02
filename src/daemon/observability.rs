//! Tracing subscriber init with optional OTel/OTLP export.
//!
//! Default build: stdout `fmt` layer only, gated by the existing
//! `RUST_LOG`/`grimoire=info` filter — byte-equivalent to the original
//! `tracing_subscriber::fmt().init()` call that lived inline in `main.rs`.
//!
//! With `--features otel` + the `OTEL_EXPORTER_OTLP_ENDPOINT` environment
//! variable set: also installs a `tracing_opentelemetry` layer that ships
//! spans over OTLP/HTTP to the configured Collector. Tunables follow the
//! OTel convention so existing collector docs apply:
//!
//! - `OTEL_EXPORTER_OTLP_ENDPOINT` — base URL (e.g. `http://localhost:4318`).
//!   When unset (the common case), this module behaves exactly like the
//!   inline init it replaced.
//! - `OTEL_SERVICE_NAME` — resource attribute. Defaults to `grimoire`.
//! - `OTEL_TRACES_SAMPLER_ARG` — TraceIdRatio fraction in `[0.0, 1.0]`.
//!   Defaults to `1.0` (sample everything; the daemon's volume is low).
//!
//! Errors during exporter construction are logged and fall back to the
//! fmt-only subscriber rather than aborting daemon startup — observability
//! must never take the system down.

use tracing_subscriber::EnvFilter;

fn default_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "grimoire=info".parse().expect("valid static env filter"))
}

/// Initialise the global subscriber. Call exactly once, early in
/// `main()` before any tracing macros fire.
pub fn init() {
    #[cfg(feature = "otel")]
    {
        if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
            match init_with_otel() {
                Ok(()) => return,
                Err(e) => {
                    // Can't use `tracing::warn!` here — the subscriber
                    // doesn't exist yet. Fall through to fmt-only and
                    // emit the warning once it does.
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

/// Holds the active provider so [`shutdown`] can flush it. As of
/// opentelemetry 0.30 the global `shutdown_tracer_provider()` free
/// function is gone; the owner must keep the provider and call
/// `.shutdown()` on it explicitly.
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

    // 0.30+ batch processor runs on a dedicated background thread, so
    // `with_batch_exporter` no longer takes a runtime argument.
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::TraceIdRatioBased(sampler_arg.clamp(0.0, 1.0)))
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("grimoire");
    opentelemetry::global::set_tracer_provider(provider.clone());
    // Best-effort: a second init within one process keeps the first
    // provider for shutdown; init() only runs once at boot regardless.
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
