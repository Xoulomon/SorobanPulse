use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
    Json,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use subtle::ConstantTimeEq;

/// Middleware to extract request_id from headers and store in thread-local
pub async fn request_id_middleware(req: Request, next: Next) -> Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    crate::error::set_request_id(request_id);
    next.run(req).await
}

/// The resolved tenant for the current request.
/// Injected as a request extension by `auth_middleware` when multi-tenant mode
/// is enabled.  Handlers read this via `req.extensions().get::<TenantId>()`.
#[derive(Clone, Debug)]
pub struct TenantId(pub String);

#[derive(Clone)]
pub struct AuthState {
    pub api_keys: Vec<String>,
    /// Admin API keys. These are accepted by the regular auth layer (so admin
    /// requests pass the global gate) and are additionally required by
    /// `admin_auth_middleware` to reach `/v1/admin/*` endpoints.
    pub admin_api_keys: Vec<String>,
    /// key_hash → tenant_id mapping; populated only in multi-tenant mode.
    pub tenant_map: Arc<std::collections::HashMap<String, String>>,
    pub multi_tenant: bool,
}

/// Constant-time check whether `key` is one of `candidates`.
fn key_matches_any(key: &str, candidates: &[String]) -> bool {
    candidates.iter().any(|expected| {
        let m: bool = key.as_bytes().ct_eq(expected.as_bytes()).into();
        m
    })
}

/// Extract the API key from either the `Authorization: Bearer <key>` header or
/// the `X-Api-Key` header.
fn extract_api_key(req: &Request) -> Option<&str> {
    let bearer = req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    let x_api_key = req.headers().get("X-Api-Key").and_then(|h| h.to_str().ok());
    bearer.or(x_api_key)
}

/// SHA-256 hex digest of a raw API key — used as the lookup key in tenant_map.
pub fn hash_api_key(key: &str) -> String {
    let mut h = Sha256::new();
    h.update(key.as_bytes());
    format!("{:x}", h.finalize())
}

pub async fn auth_middleware(
    State(state): State<Arc<AuthState>>,
    mut req: Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let path = req.uri().path();

    // Exclude /health and /healthz/*
    if path == "/health" || path.starts_with("/healthz/") {
        return Ok(next.run(req).await);
    }

    if !state.api_keys.is_empty() {
        let provided_key = extract_api_key(&req);

        // Admin keys are also valid at the global gate so admin requests can
        // reach the admin-only layer (which performs the privilege check).
        let is_admin = provided_key.map_or(false, |k| key_matches_any(k, &state.admin_api_keys));
        let is_valid =
            is_admin || provided_key.map_or(false, |k| key_matches_any(k, &state.api_keys));

        if !is_valid {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "unauthorized" })),
            ));
        }

        // In multi-tenant mode, resolve and inject the tenant_id extension.
        // Admin keys are global (not tenant-scoped), so they skip resolution.
        if state.multi_tenant && !is_admin {
            let key = provided_key.unwrap_or("");
            let key_hash = hash_api_key(key);
            match state.tenant_map.get(&key_hash) {
                Some(tid) => {
                    req.extensions_mut().insert(TenantId(tid.clone()));
                }
                None => {
                    return Err((
                        StatusCode::FORBIDDEN,
                        Json(serde_json::json!({
                            "error": "api key is not associated with a tenant"
                        })),
                    ));
                }
            }
        }
    }

    Ok(next.run(req).await)
}

/// State for the admin-only authentication layer applied to `/v1/admin/*`.
#[derive(Clone)]
pub struct AdminAuthState {
    /// Keys that grant admin access. When empty, the admin layer is a no-op and
    /// admin routes fall back to whatever the regular auth layer enforced (this
    /// preserves backward compatibility for deployments that gate admin routes
    /// with API_KEY only).
    pub admin_api_keys: Vec<String>,
}

/// Middleware that gates admin endpoints. Independent of API_KEY: even when no
/// regular API key is configured, a configured ADMIN_API_KEY is still required.
///
/// - 401 Unauthorized when no key is provided.
/// - 403 Forbidden when a key is provided but it is not an admin key
///   (e.g. a regular API key).
/// - passes through when the provided key matches a configured admin key.
pub async fn admin_auth_middleware(
    State(state): State<Arc<AdminAuthState>>,
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    // No admin keys configured → don't add a second gate; defer to regular auth.
    if state.admin_api_keys.is_empty() {
        return Ok(next.run(req).await);
    }

    match extract_api_key(&req) {
        None => Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "admin authentication required" })),
        )),
        Some(key) if key_matches_any(key, &state.admin_api_keys) => Ok(next.run(req).await),
        Some(_) => Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "admin privileges required" })),
        )),
    }
}

pub async fn security_headers_middleware(req: Request, next: Next) -> Response {
    let path = req.uri().path().to_owned();
    let mut response = next.run(req).await;

    // Set standard security headers for all responses
    response
        .headers_mut()
        .insert("X-Content-Type-Options", "nosniff".parse().unwrap());

    response
        .headers_mut()
        .insert("X-Frame-Options", "DENY".parse().unwrap());

    response
        .headers_mut()
        .insert("Referrer-Policy", "no-referrer".parse().unwrap());

    // Set CSP header with different policies for /docs vs other routes.
    let csp = if path == "/docs" {
        // Swagger UI bootstraps via an inline <script> and loads the library
        // assets from unpkg.com, so the docs policy must permit both
        // 'unsafe-inline' and the unpkg origin for scripts/styles. Framing is
        // still denied via frame-ancestors.
        "default-src 'self'; script-src 'self' 'unsafe-inline' https://unpkg.com; style-src 'self' 'unsafe-inline' https://unpkg.com; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none';"
    } else {
        // Strict policy for API endpoints: they only ever return JSON, so no
        // resource loading or framing is permitted.
        "default-src 'none'; frame-ancestors 'none';"
    };

    response
        .headers_mut()
        .insert("Content-Security-Policy", csp.parse().unwrap());

    response
}

pub async fn cache_middleware(req: Request, next: Next) -> Response {
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();

    let mut response = next.run(req).await;

    // Add cache headers based on endpoint
    let cache_control = if path.ends_with("/tx/") || (path.contains("/tx/") && !path.contains("?"))
    {
        // GET /v1/events/tx/:hash - immutable, cache for 1 hour
        "public, max-age=3600, immutable"
    } else if path == "/v1/events" || path == "/events" {
        // GET /v1/events - check for filters
        if query.contains("to_ledger") {
            // With to_ledger filter - cache for 60 seconds
            "public, max-age=60"
        } else {
            // No filters - cache for 5 seconds with stale-while-revalidate
            "public, max-age=5, stale-while-revalidate=10"
        }
    } else if path.contains("/contract/") {
        // GET /v1/events/contract/:id - cache for 5 seconds with stale-while-revalidate
        "public, max-age=5, stale-while-revalidate=10"
    } else {
        // Default - no caching
        return response;
    };

    response.headers_mut().insert(
        "Cache-Control",
        cache_control
            .parse()
            .unwrap_or_else(|_| "no-cache".parse().unwrap()),
    );

    // Add ETag header based on response body hash
    let (mut parts, body) = response.into_parts();
    parts.headers.insert(
        "Cache-Control",
        cache_control
            .parse()
            .unwrap_or_else(|_| "no-cache".parse().unwrap()),
    );
    if let Ok(body_bytes) = axum::body::to_bytes(body, usize::MAX).await {
        let mut hasher = Sha256::new();
        hasher.update(&body_bytes);
        let hash = format!("{:x}", hasher.finalize());
        let etag = format!("\"{}\"", &hash[..16]);
        parts.headers.insert(
            "ETag",
            etag.parse()
                .unwrap_or_else(|_| "\"unknown\"".parse().unwrap()),
        );
        Response::from_parts(parts, axum::body::Body::from(body_bytes))
    } else {
        Response::from_parts(parts, axum::body::Body::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{HeaderValue, Request, StatusCode};
    use axum::response::Response;
    use axum::{routing::get, Router};
    use tower::ServiceExt;

    async fn setup_app(api_keys: Vec<String>) -> Router {
        let auth_state = Arc::new(AuthState {
            api_keys,
            admin_api_keys: Vec::new(),
            tenant_map: Arc::new(std::collections::HashMap::new()),
            multi_tenant: false,
        });
        Router::new()
            .route("/test", get(|| async { "OK" }))
            .route("/health", get(|| async { "OK" }))
            .route("/healthz/live", get(|| async { "OK" }))
            .route_layer(axum::middleware::from_fn_with_state(
                auth_state,
                auth_middleware,
            ))
    }

    #[tokio::test]
    async fn test_auth_bypassed_when_no_key_configured() {
        let app = setup_app(vec![]).await;

        let response: Response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_success_with_bearer_token() {
        let app = setup_app(vec!["secret123".to_string()]).await;

        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("Authorization", "Bearer secret123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_success_with_x_api_key() {
        let app = setup_app(vec!["secret123".to_string()]).await;

        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("X-Api-Key", "secret123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_failure_with_invalid_key() {
        let app = setup_app(vec!["secret123".to_string()]).await;

        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("Authorization", "Bearer wrongkey")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_failure_with_missing_key() {
        let app = setup_app(vec!["secret123".to_string()]).await;

        let response: Response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_success_with_secondary_key() {
        let app = setup_app(vec!["primary".to_string(), "secondary".to_string()]).await;

        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("Authorization", "Bearer secondary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_failure_with_correct_prefix_wrong_suffix() {
        let app = setup_app(vec!["secret123".to_string()]).await;

        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("Authorization", "Bearer secret999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_health_endpoints_bypass_auth() {
        let app = setup_app(vec!["secret123".to_string()]).await;

        let response: Response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn setup_admin_app(admin_api_keys: Vec<String>) -> Router {
        let admin_state = Arc::new(AdminAuthState { admin_api_keys });
        Router::new()
            .route("/v1/admin/indexer/pause", get(|| async { "OK" }))
            .route_layer(axum::middleware::from_fn_with_state(
                admin_state,
                admin_auth_middleware,
            ))
    }

    #[tokio::test]
    async fn admin_returns_401_without_key() {
        let app = setup_admin_app(vec!["admin-secret".to_string()]).await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/indexer/pause")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_returns_403_with_non_admin_key() {
        let app = setup_admin_app(vec!["admin-secret".to_string()]).await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/indexer/pause")
                    .header("Authorization", "Bearer regular-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn admin_returns_200_with_admin_key() {
        let app = setup_admin_app(vec!["admin-secret".to_string()]).await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/indexer/pause")
                    .header("X-Api-Key", "admin-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_layer_noop_when_no_admin_keys_configured() {
        // With no admin keys, the layer must not add a second gate.
        let app = setup_admin_app(vec![]).await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/indexer/pause")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn setup_security_test_app() -> Router {
        Router::new()
            .route("/test", get(|| async { "OK" }))
            .route("/docs", get(|| async { "Swagger UI" }))
            .layer(axum::middleware::from_fn(security_headers_middleware))
    }

    #[tokio::test]
    async fn test_security_headers_on_regular_route() {
        let app = setup_security_test_app().await;

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify X-Content-Type-Options header
        assert_eq!(
            response.headers().get("X-Content-Type-Options"),
            Some(&HeaderValue::from_static("nosniff"))
        );

        // Verify X-Frame-Options header
        assert_eq!(
            response.headers().get("X-Frame-Options"),
            Some(&HeaderValue::from_static("DENY"))
        );

        // Verify Referrer-Policy header
        assert_eq!(
            response.headers().get("Referrer-Policy"),
            Some(&HeaderValue::from_static("no-referrer"))
        );

        // Verify strict CSP header for regular (API) routes
        assert_eq!(
            response.headers().get("Content-Security-Policy"),
            Some(&HeaderValue::from_static(
                "default-src 'none'; frame-ancestors 'none';"
            ))
        );
    }

    #[tokio::test]
    async fn test_security_headers_on_docs_route() {
        let app = setup_security_test_app().await;

        let response = app
            .oneshot(Request::builder().uri("/docs").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify standard security headers are still present
        assert_eq!(
            response.headers().get("X-Content-Type-Options"),
            Some(&HeaderValue::from_static("nosniff"))
        );

        assert_eq!(
            response.headers().get("X-Frame-Options"),
            Some(&HeaderValue::from_static("DENY"))
        );

        assert_eq!(
            response.headers().get("Referrer-Policy"),
            Some(&HeaderValue::from_static("no-referrer"))
        );

        // Verify permissive CSP header for /docs route that allows unpkg.com and
        // inline scripts/styles (required for the Swagger UI bootstrap).
        let expected_csp = "default-src 'self'; script-src 'self' 'unsafe-inline' https://unpkg.com; style-src 'self' 'unsafe-inline' https://unpkg.com; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none';";
        assert_eq!(
            response.headers().get("Content-Security-Policy"),
            Some(&HeaderValue::from_static(expected_csp))
        );
    }

    #[tokio::test]
    async fn test_all_security_headers_present() {
        let app = setup_security_test_app().await;

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let headers = response.headers();

        // Verify all required security headers are present
        assert!(headers.contains_key("X-Content-Type-Options"));
        assert!(headers.contains_key("X-Frame-Options"));
        assert!(headers.contains_key("Referrer-Policy"));
        assert!(headers.contains_key("Content-Security-Policy"));

        // Verify header values are not empty
        assert!(!headers.get("X-Content-Type-Options").unwrap().is_empty());
        assert!(!headers.get("X-Frame-Options").unwrap().is_empty());
        assert!(!headers.get("Referrer-Policy").unwrap().is_empty());
        assert!(!headers.get("Content-Security-Policy").unwrap().is_empty());
    }
}
