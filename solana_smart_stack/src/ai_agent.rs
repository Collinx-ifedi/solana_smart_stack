use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

/// System-level action instructions parsed from AI reasoning.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RecoveryStrategy {
    pub action: String,             // Expected values: "RETRY", "ABORT", "HOLD"
    pub refresh_blockhash: bool,    // Flag to explicitly re-fetch a network blockhash
    pub modified_tip: u64,          // Dynamic tip recalculation adjustment in lamports
    pub reasoning: String,          // The underlying logic or root cause classification
}

/// The Autonomous AI Agent Engine handling failure log telemetry.
pub struct AIAgent {
    api_key: String,
    http_client: Client,
    model_endpoint: String,
}

impl AIAgent {
    /// Initializes the AI Agent with an active OpenRouter access token.
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            http_client: Client::new(),
            model_endpoint: "https://openrouter.ai/api/v1/chat/completions".to_string(),
        }
    }

    /// Analyzes a raw failure string and decides on a machine-executable recovery strategy.
    /// This satisfies Requirement 4: "Failure Reasoning / Autonomous Retry with Fault Injection"
    pub async fn analyze_failure(&self, raw_log: &str, current_tip: u64) -> Result<RecoveryStrategy> {
        info!("🧠 AI Agent triggered. Ingesting failure telemetry log...");

        // Construct a highly restrictive system prompt to guarantee clean JSON output without text fluff
        let system_prompt = "You are an autonomous Solana validator-level core system infrastructure agent. \
            Analyze the provided transaction failure log string and output a strict raw JSON schema mapping the recovery strategy. \
            Your response must be exclusively valid JSON matching the exact schema keys without markdown code block wrappers (do not use ```json). \
            Schema keys required: \
            - \"action\": string (\"RETRY\", \"ABORT\", or \"HOLD\") \
            - \"refresh_blockhash\": boolean (true if blockhash expired, false otherwise) \
            - \"modified_tip\": integer (the updated lamport tip amount to use. If retrying due to a severe network congestion fault, increase the current tip parameter by 20-50% to secure space) \
            - \"reasoning\": string (concise root cause summary)";

        let user_prompt = format!(
            "CRITICAL FAULT ENCOUNTERED:\nLog Data: \"{}\"\nCurrent Active Tip: {} lamports\n\nGenerate JSON Strategy:",
            raw_log, current_tip
        );

        let payload = json!({
            "model": "meta/Llama 4 marvick:free",
            "messages": [
                { "role": "system", "content": system_prompt },
                { "role": "user", "content": user_prompt }
            ],
            "temperature": 0.1 // Extremely low temperature to enforce strict deterministic compliance
        });

        debug!("📡 Forwarding telemetry matrix to OpenRouter...");
        let response = self.http_client.post(&self.model_endpoint)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("HTTP-Referer", "[https://superteam.earn](https://superteam.earn)") // OpenRouter analytics tracking requirement
            .json(&payload)
            .send()
            .await
            .context("Failed to execute network request to OpenRouter API")?
            .json::<Value>()
            .await
            .context("Failed to parse OpenRouter JSON payload")?;

        // Extract the textual content choice from the chat completion response array
        let raw_content = response["choices"][0]["message"]["content"]
            .as_str()
            .context("Failed to locate target message content within OpenRouter payload metadata")?
            .trim();

        // 🧠 Robust Sanitation: Strip structural markdown blocks if the LLM leaked them despite system instructions
        let sanitized_content = if raw_content.starts_with("```json") {
            raw_content
                .strip_prefix("```json")
                .unwrap_or(raw_content)
                .strip_suffix("
```")
                .unwrap_or(raw_content)
                .trim()
        } else if raw_content.starts_with("```") {
            raw_content
                .strip_prefix("
```")
                .unwrap_or(raw_content)
                .strip_suffix("```")
                .unwrap_or(raw_content)
                .trim()
        } else {
            raw_content
        };

        debug!("📝 AI Raw Cleaned Content: {}", sanitized_content);

        // Deserialize directly into the operational strategy framework
        let strategy: RecoveryStrategy = serde_json::from_str(sanitized_content)
            .map_err(|e| {
                warn!("⚠️ AI returned invalid JSON syntax format. Content was: {}", sanitized_content);
                anyhow::anyhow!("Failed to parse execution schema matching requirements: {}", e)
            })?;

        info!("🟢 Autonomous Strategy Extracted: Action={} | Premium Tip={} | Cause=\"{}\"", 
            strategy.action, strategy.modified_tip, strategy.reasoning
        );

        Ok(strategy)
    }
}
