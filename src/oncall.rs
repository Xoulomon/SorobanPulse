/// On-call rotation integration (Issue #494).
///
/// Supports PagerDuty, OpsGenie, and VictorOps as `oncall_provider` values.
/// The resolved schedule is cached for `oncall_schedule_cache_ttl_secs` (default 5 minutes)
/// to avoid excessive API calls during high-volume notification periods.
use reqwest::Client;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::config::Config;

/// Contact information for the current on-call engineer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnCallContact {
    pub user_name: String,
    pub user_email: Option<String>,
    pub user_phone: Option<String>,
    pub provider: String,
    pub schedule_id: Option<String>,
}

/// Cached on-call schedule entry.
struct CachedSchedule {
    contact: OnCallContact,
    fetched_at: Instant,
    ttl: Duration,
}

impl CachedSchedule {
    fn is_expired(&self) -> bool {
        self.fetched_at.elapsed() > self.ttl
    }
}

/// On-call schedule resolver with in-process TTL cache (Issue #494).
pub struct OnCallScheduler {
    client: Client,
    provider: String,
    api_key: Option<String>,
    schedule_id: Option<String>,
    cache_ttl: Duration,
    cache: Arc<RwLock<Option<CachedSchedule>>>,
}

impl OnCallScheduler {
    pub fn new(
        provider: String,
        api_key: Option<String>,
        schedule_id: Option<String>,
        cache_ttl_secs: u64,
    ) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to build on-call HTTP client");

        Self {
            client,
            provider,
            api_key,
            schedule_id,
            cache_ttl: Duration::from_secs(cache_ttl_secs),
            cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Build an OnCallScheduler from the application config.
    /// Returns None when `oncall_provider` is not configured.
    pub fn from_config(config: &Config) -> Option<Self> {
        let provider = config.oncall_provider.clone()?;
        let api_key = config
            .oncall_pagerduty_api_key
            .as_ref()
            .map(|s| s.expose_secret().clone());

        Some(Self::new(
            provider,
            api_key,
            config.oncall_schedule_id.clone(),
            config.oncall_schedule_cache_ttl_secs,
        ))
    }

    /// Return the current on-call contact, using the cache when available.
    pub async fn current_oncall(&self) -> Option<OnCallContact> {
        // Check cache first.
        {
            let cache = self.cache.read().await;
            if let Some(ref cached) = *cache {
                if !cached.is_expired() {
                    return Some(cached.contact.clone());
                }
            }
        }

        // Cache miss or expired — fetch from provider.
        let contact = match self.provider.as_str() {
            "pagerduty" => self.fetch_pagerduty_oncall().await,
            "opsgenie" => self.fetch_opsgenie_oncall().await,
            "victorops" => self.fetch_victorops_oncall().await,
            unknown => {
                warn!(provider = %unknown, "Unknown on-call provider");
                return None;
            }
        };

        if let Some(ref c) = contact {
            let mut cache = self.cache.write().await;
            *cache = Some(CachedSchedule {
                contact: c.clone(),
                fetched_at: Instant::now(),
                ttl: self.cache_ttl,
            });
            info!(
                provider = %self.provider,
                user = %c.user_name,
                "On-call schedule resolved and cached"
            );
        }

        contact
    }

    /// Fetch the current on-call engineer from the PagerDuty Schedules API.
    async fn fetch_pagerduty_oncall(&self) -> Option<OnCallContact> {
        let api_key = self.api_key.as_deref()?;
        let schedule_id = self.schedule_id.as_deref()?;

        let url = format!(
            "https://api.pagerduty.com/schedules/{}/users",
            schedule_id
        );

        let response = match self
            .client
            .get(&url)
            .header("Accept", "application/vnd.pagerduty+json;version=2")
            .header("Authorization", format!("Token token={}", api_key))
            .query(&[("since", chrono::Utc::now().to_rfc3339()),
                     ("until", (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339())])
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "PagerDuty on-call API request failed");
                return None;
            }
        };

        if !response.status().is_success() {
            warn!(status = %response.status(), "PagerDuty on-call API returned non-2xx");
            return None;
        }

        let body: serde_json::Value = match response.json().await {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "Failed to parse PagerDuty on-call response");
                return None;
            }
        };

        let user = body.get("users")?.get(0)?;
        let user_name = user.get("name")?.as_str()?.to_string();
        let user_email = user
            .get("email")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Some(OnCallContact {
            user_name,
            user_email,
            user_phone: None,
            provider: "pagerduty".to_string(),
            schedule_id: self.schedule_id.clone(),
        })
    }

    /// Fetch the current on-call engineer from the OpsGenie On-Call API.
    async fn fetch_opsgenie_oncall(&self) -> Option<OnCallContact> {
        let api_key = self.api_key.as_deref()?;
        let schedule_id = self.schedule_id.as_deref()?;

        let url = format!(
            "https://api.opsgenie.com/v2/schedules/{}/on-calls",
            schedule_id
        );

        let response = match self
            .client
            .get(&url)
            .header("Authorization", format!("GenieKey {}", api_key))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "OpsGenie on-call API request failed");
                return None;
            }
        };

        if !response.status().is_success() {
            warn!(status = %response.status(), "OpsGenie on-call API returned non-2xx");
            return None;
        }

        let body: serde_json::Value = match response.json().await {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "Failed to parse OpsGenie on-call response");
                return None;
            }
        };

        let recipient = body
            .get("data")?
            .get("onCallRecipients")?
            .get(0)?;

        let user_name = recipient.as_str().unwrap_or("unknown").to_string();

        Some(OnCallContact {
            user_name,
            user_email: None,
            user_phone: None,
            provider: "opsgenie".to_string(),
            schedule_id: self.schedule_id.clone(),
        })
    }

    /// Fetch the current on-call engineer from the VictorOps (Splunk On-Call) API.
    async fn fetch_victorops_oncall(&self) -> Option<OnCallContact> {
        let api_key = self.api_key.as_deref()?;

        let url = "https://api.victorops.com/api-public/v1/oncall/current";

        let response = match self
            .client
            .get(url)
            .header("X-VO-Api-Id", api_key)
            .header("X-VO-Api-Key", api_key)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "VictorOps on-call API request failed");
                return None;
            }
        };

        if !response.status().is_success() {
            warn!(status = %response.status(), "VictorOps on-call API returned non-2xx");
            return None;
        }

        let body: serde_json::Value = match response.json().await {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "Failed to parse VictorOps on-call response");
                return None;
            }
        };

        let user_name = body
            .get("teamsOnCall")?
            .get(0)?
            .get("onCallNow")?
            .get(0)?
            .get("users")?
            .get(0)?
            .get("onCallUser")?
            .get("username")?
            .as_str()?
            .to_string();

        Some(OnCallContact {
            user_name,
            user_email: None,
            user_phone: None,
            provider: "victorops".to_string(),
            schedule_id: self.schedule_id.clone(),
        })
    }

    /// Invalidate the cached schedule, forcing a fresh lookup on the next call.
    pub async fn invalidate_cache(&self) {
        let mut cache = self.cache.write().await;
        *cache = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oncall_scheduler_creation() {
        let scheduler = OnCallScheduler::new(
            "pagerduty".to_string(),
            Some("test-api-key".to_string()),
            Some("SCHEDULE123".to_string()),
            300,
        );
        assert_eq!(scheduler.provider, "pagerduty");
        assert_eq!(scheduler.cache_ttl, Duration::from_secs(300));
    }

    #[test]
    fn test_cached_schedule_expiry() {
        let cached = CachedSchedule {
            contact: OnCallContact {
                user_name: "Alice".to_string(),
                user_email: Some("alice@example.com".to_string()),
                user_phone: None,
                provider: "pagerduty".to_string(),
                schedule_id: None,
            },
            fetched_at: Instant::now() - Duration::from_secs(400),
            ttl: Duration::from_secs(300),
        };
        assert!(cached.is_expired());
    }

    #[test]
    fn test_cached_schedule_not_expired() {
        let cached = CachedSchedule {
            contact: OnCallContact {
                user_name: "Bob".to_string(),
                user_email: None,
                user_phone: None,
                provider: "opsgenie".to_string(),
                schedule_id: None,
            },
            fetched_at: Instant::now(),
            ttl: Duration::from_secs(300),
        };
        assert!(!cached.is_expired());
    }
}
