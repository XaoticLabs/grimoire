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

#[cfg(feature = "otel")]
fn init_with_otel() -> anyhow::Result<()> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::trace::{Sampler, TracerProvider};
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

    // `Resource::new` is the documented constructor on
    // opentelemetry_sdk 0.27 (the `Resource::builder` API only landed
    // in 0.28+). Switch to the builder when we bump the OTel crates.
    let resource = Resource::new(vec![
        KeyValue::new("service.name", service_name),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_sampler(Sampler::TraceIdRatioBased(sampler_arg.clamp(0.0, 1.0)))
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("grimoire");
    opentelemetry::global::set_tracer_provider(provider);

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
        opentelemetry::global::shutdown_tracer_provider();
    }
}
