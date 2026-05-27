use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use sqlx::PgPool;
use std::sync::Arc;
use tower::ServiceExt;

use soroban_pulse::config::{HealthState, IndexerState};
use soroban_pulse::metrics::init_metrics;
use soroban_pulse::routes::create_router;

fn make_router(pool: PgPool, api_key: Option<String>) -> axum::Router {
    let health_state = Arc::new(HealthState::new(60));
    health_state.update_last_poll();
    let indexer_state = Arc::new(IndexerState::new());
    let prometheus_handle = init_metrics();
    let api_keys = api_key.into_iter().collect();
    let config = soroban_pulse::config::Config::default();
    create_router(
        pool,
        api_keys,
        &[],
        60,
        health_state,
        indexer_state,
        prometheus_handle,
        15000,
        config,
    )
}

// --- Health ---

#[sqlx::test(migrations = "./migrations")]
async fn health_ready_with_live_db_returns_200(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/healthz/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["db"], "ok");
    assert_eq!(body["indexer"], "ok");
}

// --- Auth middleware ---

#[sqlx::test(migrations = "./migrations")]
async fn request_without_api_key_returns_401_when_key_configured(pool: PgPool) {
    let app = make_router(pool, Some("secret".to_string()));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "./migrations")]
async fn request_with_bearer_token_passes_auth(pool: PgPool) {
    let app = make_router(pool, Some("secret".to_string()));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .header(header::AUTHORIZATION, "Bearer secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[sqlx::test(migrations = "./migrations")]
async fn request_with_x_api_key_header_passes_auth(pool: PgPool) {
    let app = make_router(pool, Some("secret".to_string()));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .header("X-Api-Key", "secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[sqlx::test(migrations = "./migrations")]
async fn health_endpoint_bypasses_auth(pool: PgPool) {
    let app = make_router(pool, Some("secret".to_string()));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
}

// --- Deprecation headers on unversioned routes ---

#[sqlx::test(migrations = "./migrations")]
async fn unversioned_events_route_returns_deprecation_header(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("Deprecation").unwrap(), "true");
    assert!(resp
        .headers()
        .get("Link")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("/v1/events"));
}

// --- Metrics endpoint ---

#[sqlx::test(migrations = "./migrations")]
async fn metrics_endpoint_returns_prometheus_text(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("soroban_pulse"));
}

// --- Issue #185: from_ledger / to_ledger on contract endpoint ---

async fn insert_contract_events(pool: &PgPool, contract_id: &str, ledgers: &[i64]) {
    for (i, &ledger) in ledgers.iter().enumerate() {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, NOW(), $5)",
        )
        .bind(contract_id)
        .bind("contract")
        .bind(format!("{:0>63}{}", i, ledger))
        .bind(ledger)
        .bind(serde_json::json!({}))
        .execute(pool)
        .await
        .unwrap();
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn contract_ledger_range_filters_correctly(pool: PgPool) {
    let contract_id = "C1234567890123456789012345678901234567890123456789012345";
    insert_contract_events(&pool, contract_id, &[100, 200, 300, 400, 500]).await;

    let app = make_router(pool, None);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/events/contract/{}?from_ledger=200&to_ledger=400",
                    contract_id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();

    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 3);
    for event in data {
        let ledger = event["ledger"].as_i64().unwrap();
        assert!((200..=400).contains(&ledger));
    }
    assert_eq!(body["from_ledger"], 200);
    assert_eq!(body["to_ledger"], 400);
}

#[sqlx::test(migrations = "./migrations")]
async fn contract_ledger_range_inverted_returns_400(pool: PgPool) {
    let contract_id = "C1234567890123456789012345678901234567890123456789012345";
    insert_contract_events(&pool, contract_id, &[100]).await;

    let app = make_router(pool, None);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/events/contract/{}?from_ledger=500&to_ledger=100",
                    contract_id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("from_ledger must be <= to_ledger"));
}

#[sqlx::test(migrations = "./migrations")]
async fn contract_without_ledger_range_returns_all_events(pool: PgPool) {
    let contract_id = "C1234567890123456789012345678901234567890123456789012345";
    insert_contract_events(&pool, contract_id, &[100, 200, 300]).await;

    let app = make_router(pool, None);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/events/contract/{}", contract_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 3);
    assert!(body.get("from_ledger").is_none());
    assert!(body.get("to_ledger").is_none());
}

// --- SSE Streaming ---

#[sqlx::test(migrations = "./migrations")]
async fn sse_contract_stream_invalid_contract_id_returns_400(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/contract/INVALID/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[sqlx::test(migrations = "./migrations")]
async fn sse_contract_stream_establishes_successfully(pool: PgPool) {
    let contract_id = "C1234567890123456789012345678901234567890123456789012345";
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/events/contract/{}/stream", contract_id))
                .header(header::ACCEPT, "text/event-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn sse_deprecated_contract_stream_unversioned_alias_works(pool: PgPool) {
    let contract_id = "C1234567890123456789012345678901234567890123456789012345";
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/events/contract/{}/stream", contract_id))
                .header(header::ACCEPT, "text/event-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("Deprecation").unwrap(), "true");
}

// --- Issue #186: sort parameter ---

async fn insert_events_with_ledgers(pool: &PgPool, ledgers: &[i64]) {
    for (i, &ledger) in ledgers.iter().enumerate() {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, NOW(), $5)",
        )
        .bind(format!("C{:0>55}", i))
        .bind("contract")
        .bind(format!("{:0>63}{}", i, ledger))
        .bind(ledger)
        .bind(serde_json::json!({}))
        .execute(pool)
        .await
        .unwrap();
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn get_events_sort_desc_returns_newest_first(pool: PgPool) {
    insert_events_with_ledgers(&pool, &[100, 200, 300]).await;

    let app = make_router(pool, None);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events?sort=desc")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 3);
    assert!(data[0]["ledger"].as_i64().unwrap() >= data[1]["ledger"].as_i64().unwrap());
    assert!(data[1]["ledger"].as_i64().unwrap() >= data[2]["ledger"].as_i64().unwrap());
}

#[sqlx::test(migrations = "./migrations")]
async fn get_events_sort_asc_returns_oldest_first(pool: PgPool) {
    insert_events_with_ledgers(&pool, &[100, 200, 300]).await;

    let app = make_router(pool, None);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events?sort=asc")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 3);
    assert!(data[0]["ledger"].as_i64().unwrap() <= data[1]["ledger"].as_i64().unwrap());
    assert!(data[1]["ledger"].as_i64().unwrap() <= data[2]["ledger"].as_i64().unwrap());
}

#[sqlx::test(migrations = "./migrations")]
async fn get_events_default_sort_is_desc(pool: PgPool) {
    insert_events_with_ledgers(&pool, &[100, 200, 300]).await;

    let app = make_router(pool, None);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    let data = body["data"].as_array().unwrap();
    // Default is desc — first element should have the highest ledger
    assert!(
        data[0]["ledger"].as_i64().unwrap() >= data[data.len() - 1]["ledger"].as_i64().unwrap()
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn get_events_invalid_sort_returns_400(pool: PgPool) {
    let app = make_router(pool, None);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events?sort=random")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[sqlx::test(migrations = "./migrations")]
async fn get_events_by_contract_sort_asc_returns_oldest_first(pool: PgPool) {
    let contract_id = "C1234567890123456789012345678901234567890123456789012345";
    insert_contract_events(&pool, contract_id, &[100, 200, 300]).await;

    let app = make_router(pool, None);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/events/contract/{}?sort=asc", contract_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 3);
    assert!(data[0]["ledger"].as_i64().unwrap() <= data[1]["ledger"].as_i64().unwrap());
}

#[sqlx::test(migrations = "./migrations")]
async fn get_events_by_contract_sort_desc_returns_newest_first(pool: PgPool) {
    let contract_id = "C1234567890123456789012345678901234567890123456789012345";
    insert_contract_events(&pool, contract_id, &[100, 200, 300]).await;

    let app = make_router(pool, None);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/events/contract/{}?sort=desc", contract_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 3);
    assert!(data[0]["ledger"].as_i64().unwrap() >= data[1]["ledger"].as_i64().unwrap());
}

// --- GET /v1/events/stats ---

async fn insert_stats_seed_data(pool: &PgPool) {
    // 3 contract events for contract A
    let contract_a = "CA23456789012345678901234567890123456789012345678901234567";
    // 2 contract events for contract B
    let contract_b = "CB23456789012345678901234567890123456789012345678901234567";

    for i in 0..3i64 {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, 'contract', $2, $3, NOW(), '{}'::jsonb)",
        )
        .bind(contract_a)
        .bind(format!("{:0>63}{}", i, "a"))
        .bind(100 + i)
        .execute(pool)
        .await
        .unwrap();
    }
    for i in 0..2i64 {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, 'diagnostic', $2, $3, NOW(), '{}'::jsonb)",
        )
        .bind(contract_b)
        .bind(format!("{:0>63}{}", i, "b"))
        .bind(200 + i)
        .execute(pool)
        .await
        .unwrap();
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn stats_returns_200_with_correct_totals(pool: PgPool) {
    insert_stats_seed_data(&pool).await;
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();

    assert_eq!(body["total_events"], 5);
    assert_eq!(body["events_by_type"]["contract"], 3);
    assert_eq!(body["events_by_type"]["diagnostic"], 2);
    assert_eq!(body["events_by_type"]["system"], 0);
    assert!(body["computed_at"].is_string());
    assert_eq!(body["min_ledger"], 100);
    assert_eq!(body["max_ledger"], 201);
}

#[sqlx::test(migrations = "./migrations")]
async fn stats_top_contracts_ordered_by_count(pool: PgPool) {
    insert_stats_seed_data(&pool).await;
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();

    let top = body["top_contracts"].as_array().unwrap();
    assert_eq!(top.len(), 2);
    // Contract A has 3 events, contract B has 2 — A should be first.
    assert_eq!(top[0]["event_count"], 3);
    assert_eq!(top[1]["event_count"], 2);
}

#[sqlx::test(migrations = "./migrations")]
async fn stats_returns_cache_control_header(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let cc = resp
        .headers()
        .get("cache-control")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        cc.contains("max-age=60"),
        "expected max-age=60 in Cache-Control, got: {cc}"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn stats_empty_db_returns_zeros(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();

    assert_eq!(body["total_events"], 0);
    assert!(body["min_ledger"].is_null());
    assert!(body["max_ledger"].is_null());
    assert_eq!(body["top_contracts"].as_array().unwrap().len(), 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn stats_requires_auth_when_key_configured(pool: PgPool) {
    let app = make_router(pool, Some("secret".to_string()));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// --- GET /v1/events with empty DB ---

#[sqlx::test(migrations = "./migrations")]
async fn get_events_empty_db_returns_200_with_empty_data(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
    assert_eq!(body["total"], 0);
    assert_eq!(body["page"], 1);
    assert_eq!(body["limit"], 20);
}

// --- GET /v1/events/{contract_id} with invalid contract ID ---

#[sqlx::test(migrations = "./migrations")]
async fn get_events_by_contract_invalid_id_returns_400(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/contract/INVALID_CONTRACT_ID")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body["error"].as_str().unwrap().contains("invalid contract_id"));
}

// --- GET /v1/events/tx/{tx_hash} with valid hash ---

#[sqlx::test(migrations = "./migrations")]
async fn get_events_by_tx_hash_returns_200_with_empty_data_for_unknown_hash(pool: PgPool) {
    let app = make_router(pool, None);
    let tx_hash = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";

    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/events/tx/{}", tx_hash))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn get_events_by_tx_hash_invalid_hash_returns_400(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/tx/invalid_hash")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// --- GET /openapi.json returns valid JSON ---

#[sqlx::test(migrations = "./migrations")]
async fn openapi_json_returns_valid_spec(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body.get("paths").is_some());
    assert!(body.get("info").is_some());
    assert_eq!(body["info"]["title"], "Soroban Pulse API");
}

// --- GET /v1/contracts endpoint ---

#[sqlx::test(migrations = "./migrations")]
async fn get_contracts_empty_db_returns_200_with_empty_data(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/contracts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
    assert_eq!(body["total"], 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn get_contracts_returns_contract_list_with_counts(pool: PgPool) {
    let contract_a = "CA23456789012345678901234567890123456789012345678901234567";
    let contract_b = "CB23456789012345678901234567890123456789012345678901234567";

    // Insert 3 events for contract A
    for i in 0..3i64 {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, 'contract', $2, $3, NOW(), '{}'::jsonb)",
        )
        .bind(contract_a)
        .bind(format!("{:0>63}{}", i, "a"))
        .bind(100 + i)
        .execute(&pool)
        .await
        .unwrap();
    }

    // Insert 2 events for contract B
    for i in 0..2i64 {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, 'contract', $2, $3, NOW(), '{}'::jsonb)",
        )
        .bind(contract_b)
        .bind(format!("{:0>63}{}", i, "b"))
        .bind(200 + i)
        .execute(&pool)
        .await
        .unwrap();
    }

    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/contracts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
    assert_eq!(body["total"], 2);

    // Verify contract summaries have required fields
    for contract in data {
        assert!(contract.get("contract_id").is_some());
        assert!(contract.get("event_count").is_some());
        assert!(contract.get("first_seen_ledger").is_some());
        assert!(contract.get("last_seen_ledger").is_some());
        assert!(contract.get("last_event_at").is_some());
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn get_contracts_requires_auth_when_key_configured(pool: PgPool) {
    let app = make_router(pool, Some("secret".to_string()));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/contracts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "./migrations")]
async fn deprecated_contracts_route_returns_deprecation_header(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/contracts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("Deprecation").unwrap(), "true");
    assert!(resp
        .headers()
        .get("Link")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("/v1/contracts"));
}
