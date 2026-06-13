//! HTTP auth middleware and the cookie-based login flow: bearer/cookie token
//! extraction, the `route_layer` guard, the `/auth/login` + `/auth/logout`
//! handlers, and the standalone sign-in page.

use std::sync::Arc;

use axum::response::IntoResponse;

use crate::shared::auth::AuthToken;

use super::AppState;

const AUTH_COOKIE_NAME: &str = "grim_auth";

/// Extract the auth token: `Authorization: Bearer …` header, then `grim_auth`
/// cookie (precedence matches the CLI).
fn extract_request_token(headers: &axum::http::HeaderMap) -> Option<String> {
    if let Some(h) = headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(s) = h.to_str()
        && let Some(rest) = s.strip_prefix("Bearer ")
    {
        return Some(rest.trim().to_string());
    }
    if let Some(cookie) = headers.get(axum::http::header::COOKIE)
        && let Ok(s) = cookie.to_str()
    {
        for part in s.split(';') {
            let kv = part.trim();
            if let Some(v) = kv.strip_prefix(&format!("{AUTH_COOKIE_NAME}=")) {
                return Some(v.to_string());
            }
        }
    }
    None
}

pub(super) async fn http_auth_middleware(
    axum::extract::State(token): axum::extract::State<Arc<AuthToken>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let presented = extract_request_token(req.headers());
    match presented {
        Some(tok) if token.verify(&tok) => next.run(req).await,
        _ => unauthorized_response(req.uri().path()),
    }
}

/// Test-only router wrapping one protected route with the production auth
/// middleware, so tests can exercise it without the rest of `AppState`.
#[cfg(test)]
pub(super) fn test_auth_router(token: Arc<AuthToken>) -> axum::Router {
    use axum::Router;
    use axum::routing::get;

    let protected = Router::new()
        .route(
            "/api/ping",
            get(|| async { axum::Json(serde_json::json!({"pong": true})) }),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            token,
            http_auth_middleware,
        ));
    let public = Router::new().route("/auth/ping-open", get(|| async { "open" }));
    Router::new().merge(protected).merge(public)
}

/// 401: JSON body for `/api/*`, the login page HTML for everything else.
fn unauthorized_response(path: &str) -> axum::response::Response {
    use axum::http::StatusCode;
    if path.starts_with("/api/") {
        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            r#"{"error":"unauthenticated"}"#,
        )
            .into_response()
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
            LOGIN_PAGE_HTML,
        )
            .into_response()
    }
}

/// `GET /auth/login`. A valid `?t=<token>` (from `grim dashboard --open`) sets
/// the cookie and redirects to `/`; otherwise renders the login form.
pub(super) async fn http_login_get(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    if let Some(tok) = q.get("t")
        && state.auth_token.verify(tok)
    {
        return login_success_response(tok);
    }
    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        LOGIN_PAGE_HTML,
    )
        .into_response()
}

/// `POST /auth/login`. Form-encoded `token=…` from the login page.
pub(super) async fn http_login_post(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::Form(form): axum::Form<LoginForm>,
) -> axum::response::Response {
    if state.auth_token.verify(&form.token) {
        login_success_response(&form.token)
    } else {
        (
            axum::http::StatusCode::UNAUTHORIZED,
            [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
            LOGIN_PAGE_HTML,
        )
            .into_response()
    }
}

pub(super) async fn http_logout() -> axum::response::Response {
    (
        axum::http::StatusCode::OK,
        [
            (
                axum::http::header::SET_COOKIE,
                format!("{AUTH_COOKIE_NAME}=; Max-Age=0; Path=/; HttpOnly; SameSite=Strict"),
            ),
            (axum::http::header::CONTENT_TYPE, "text/plain".to_string()),
        ],
        "logged out",
    )
        .into_response()
}

#[derive(serde::Deserialize)]
pub(super) struct LoginForm {
    token: String,
}

fn login_success_response(token: &str) -> axum::response::Response {
    // HttpOnly + SameSite=Strict: no JS access, no cross-site CSRF. No `Secure`
    // because the daemon listens on plain HTTP loopback.
    let cookie =
        format!("{AUTH_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400");
    (
        axum::http::StatusCode::SEE_OTHER,
        [
            (axum::http::header::SET_COOKIE, cookie),
            (axum::http::header::LOCATION, "/".to_string()),
        ],
        "",
    )
        .into_response()
}

const LOGIN_PAGE_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>grimoire sign in</title>
<style>
  body { font-family: -apple-system, system-ui, sans-serif; background: #0e0e10;
         color: #d8d8d8; display: flex; min-height: 100vh; align-items: center;
         justify-content: center; margin: 0; }
  form { background: #1a1a1d; padding: 2rem; border-radius: 0.5rem;
         border: 1px solid #2a2a2e; min-width: 320px; }
  h1 { font-size: 1rem; font-weight: 500; letter-spacing: 0.05em;
       text-transform: uppercase; margin: 0 0 1rem; color: #888; }
  input { width: 100%; padding: 0.6rem; box-sizing: border-box;
          background: #0e0e10; border: 1px solid #2a2a2e; color: #d8d8d8;
          font-family: ui-monospace, monospace; font-size: 0.9rem;
          border-radius: 0.25rem; }
  button { margin-top: 0.75rem; width: 100%; padding: 0.6rem;
           background: #6b46c1; color: white; border: 0; border-radius: 0.25rem;
           cursor: pointer; font-weight: 500; }
  button:hover { background: #7c54d6; }
  p { color: #666; font-size: 0.8rem; margin: 0.75rem 0 0; }
  code { background: #0e0e10; padding: 0.1rem 0.35rem; border-radius: 0.2rem;
         font-size: 0.8rem; }
</style>
</head>
<body>
<form method="post" action="/auth/login">
  <h1>◆ grimoire</h1>
  <input type="password" name="token" placeholder="auth token" autofocus autocomplete="off">
  <button type="submit">sign in</button>
  <p>token lives in <code>~/.grimoire/auth.token</code></p>
</form>
</body>
</html>"#;

#[cfg(test)]
mod auth_tests {
    use super::*;

    // --- HTTP header / cookie extraction ---

    fn headers_bearer(t: &str) -> axum::http::HeaderMap {
        let mut h = axum::http::HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {t}").parse().unwrap(),
        );
        h
    }

    fn headers_cookie(raw: &str) -> axum::http::HeaderMap {
        let mut h = axum::http::HeaderMap::new();
        h.insert(axum::http::header::COOKIE, raw.parse().unwrap());
        h
    }

    #[test]
    fn extract_bearer_header() {
        let h = headers_bearer("xyz");
        assert_eq!(extract_request_token(&h).as_deref(), Some("xyz"));
    }

    #[test]
    fn extract_bearer_ignores_other_schemes() {
        let mut h = axum::http::HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            "Basic dXNlcjpwYXNz".parse().unwrap(),
        );
        assert_eq!(extract_request_token(&h), None);
    }

    #[test]
    fn extract_cookie_alone() {
        let h = headers_cookie(&format!("{AUTH_COOKIE_NAME}=tok1"));
        assert_eq!(extract_request_token(&h).as_deref(), Some("tok1"));
    }

    #[test]
    fn extract_cookie_among_others() {
        let h = headers_cookie(&format!("other=foo; {AUTH_COOKIE_NAME}=tok2; trailing=bar"));
        assert_eq!(extract_request_token(&h).as_deref(), Some("tok2"));
    }

    #[test]
    fn extract_bearer_beats_cookie() {
        let mut h = headers_bearer("from-header");
        h.insert(
            axum::http::header::COOKIE,
            format!("{AUTH_COOKIE_NAME}=from-cookie").parse().unwrap(),
        );
        assert_eq!(extract_request_token(&h).as_deref(), Some("from-header"));
    }

    #[test]
    fn extract_no_credentials() {
        let h = axum::http::HeaderMap::new();
        assert_eq!(extract_request_token(&h), None);
    }

    // --- HTTP middleware end-to-end (against test_auth_router) ---

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn router_with(token: &str) -> axum::Router {
        test_auth_router(Arc::new(AuthToken::new(token)))
    }

    async fn status_of(router: axum::Router, req: Request<Body>) -> StatusCode {
        router.oneshot(req).await.unwrap().status()
    }

    #[tokio::test]
    async fn protected_route_rejects_missing_credentials() {
        let req = Request::builder()
            .uri("/api/ping")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            status_of(router_with("secret"), req).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn protected_route_rejects_wrong_bearer() {
        let req = Request::builder()
            .uri("/api/ping")
            .header("authorization", "Bearer nope")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            status_of(router_with("secret"), req).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn protected_route_accepts_correct_bearer() {
        let req = Request::builder()
            .uri("/api/ping")
            .header("authorization", "Bearer secret")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status_of(router_with("secret"), req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn protected_route_accepts_cookie() {
        let req = Request::builder()
            .uri("/api/ping")
            .header("cookie", format!("{AUTH_COOKIE_NAME}=secret"))
            .body(Body::empty())
            .unwrap();
        assert_eq!(status_of(router_with("secret"), req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn protected_route_rejects_cookie_with_wrong_value() {
        let req = Request::builder()
            .uri("/api/ping")
            .header("cookie", format!("{AUTH_COOKIE_NAME}=wrong"))
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            status_of(router_with("secret"), req).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn public_route_does_not_require_auth() {
        let req = Request::builder()
            .uri("/auth/ping-open")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status_of(router_with("secret"), req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn api_path_unauthorized_responds_with_json_body() {
        let req = Request::builder()
            .uri("/api/ping")
            .body(Body::empty())
            .unwrap();
        let resp = router_with("secret").oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.starts_with("application/json"), "got {ct}");
    }
}
