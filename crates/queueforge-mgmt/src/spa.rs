//! Embedded React SPA assets (`ui/dist`) served from the management listener.
//!
//! Registered API and health routes take precedence on the router. This
//! fallback serves static files and `index.html` for client routes, but:
//! - unknown `/api/*` (and stray health paths) return JSON 404
//! - missing asset-like paths (contain `.`) return 404 instead of the SPA shell

use axum::http::{header, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use rust_embed::Embed;
use serde_json::json;

/// Production build of the management UI (`ui/dist` relative to this crate).
#[derive(Embed)]
#[folder = "../../ui/dist"]
struct Assets;

/// Serve an embedded static asset, or `index.html` for SPA client-side routes.
pub async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');

    // Prefer exact asset (e.g. assets/index-….js).
    if !path.is_empty() {
        if let Some(file) = Assets::get(path) {
            return embedded_response(path, file.data.as_ref());
        }
    }

    // Unknown API / health paths must not return the SPA shell (JSON clients).
    if is_api_or_health_path(path) {
        return json_not_found();
    }

    // Missing file-like assets (extension present) → 404, not index.html.
    // Matches the common rust-embed SPA pattern: only extension-less paths
    // fall through to the client router.
    if path_looks_like_file(path) {
        return StatusCode::NOT_FOUND.into_response();
    }

    match Assets::get("index.html") {
        Some(file) => embedded_response("index.html", file.data.as_ref()),
        None => (
            StatusCode::NOT_FOUND,
            "management UI assets missing; run `npm run build` in ui/",
        )
            .into_response(),
    }
}

fn is_api_or_health_path(path: &str) -> bool {
    path == "api" || path.starts_with("api/") || path == "healthz" || path == "readyz"
}

/// True when the last path segment contains a `.` (e.g. `foo.js`, `a.b/c.css`).
fn path_looks_like_file(path: &str) -> bool {
    path.rsplit('/')
        .next()
        .is_some_and(|segment| segment.contains('.'))
}

fn json_not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))).into_response()
}

fn embedded_response(path: &str, bytes: &[u8]) -> Response {
    let mime = mime_guess::from_path(path)
        .first_or_octet_stream()
        .essence_str()
        .to_string();
    let mut res = (StatusCode::OK, bytes.to_vec()).into_response();
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&mime)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    res.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn content_type(res: &Response) -> &str {
        res.headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
    }

    #[tokio::test]
    async fn spa_serves_index_at_root() {
        let app = axum::Router::new().fallback(static_handler);
        let res = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(
            content_type(&res).contains("text/html"),
            "ct={}",
            content_type(&res)
        );
        assert_eq!(
            res.headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .and_then(|v| v.to_str().ok()),
            Some("nosniff")
        );
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let html = String::from_utf8_lossy(&body);
        assert!(
            html.contains("root") || html.contains("QueueForge"),
            "unexpected index: {html}"
        );
    }

    #[tokio::test]
    async fn spa_serves_hashed_assets_with_nosniff() {
        // Discover a real hashed asset path from the embed set.
        let asset_path = Assets::iter()
            .find(|p| p.starts_with("assets/"))
            .expect("ui/dist should contain assets/");
        let app = axum::Router::new().fallback(static_handler);
        let res = app
            .oneshot(
                Request::builder()
                    .uri(format!("/{asset_path}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "asset {asset_path}");
        assert_eq!(
            res.headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .and_then(|v| v.to_str().ok()),
            Some("nosniff")
        );
        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert!(!body.is_empty());
    }

    #[tokio::test]
    async fn unknown_client_route_falls_back_to_index() {
        let app = axum::Router::new().fallback(static_handler);
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/login")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(content_type(&res).contains("text/html"));
    }

    #[tokio::test]
    async fn unknown_api_path_returns_json_404() {
        let app = axum::Router::new().fallback(static_handler);
        for uri in ["/api/does-not-exist", "/api/nope", "/api"] {
            let res = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::NOT_FOUND, "uri={uri}");
            assert!(
                content_type(&res).contains("application/json"),
                "uri={uri} ct={}",
                content_type(&res)
            );
            let body = res.into_body().collect().await.unwrap().to_bytes();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["error"], "not found", "uri={uri}");
        }
    }

    #[tokio::test]
    async fn missing_asset_returns_404_not_spa() {
        let app = axum::Router::new().fallback(static_handler);
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/assets/missing-file.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let ct = content_type(&res);
        assert!(
            !ct.contains("text/html"),
            "missing asset must not be SPA shell, ct={ct}"
        );
    }
}
