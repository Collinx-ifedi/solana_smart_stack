use anyhow::{Context, Result};
use rand::seq::SliceRandom;
use reqwest::Client;
use serde_json::{json, Value};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    hash::Hash,
    message::{v0::Message, VersionedMessage},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::VersionedTransaction,
};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// High-performance execution core for compiling and submitting Jito Bundles.
pub struct JitoExecutor {
    jito_rpc_url: String,     // Exclusively used for state queries (e.g., getTipAccounts)
    jito_bundle_url: String,  // Exclusively used for POST submissions (e.g., sendBundle)
    http_client: Client,
    solana_rpc: Arc<RpcClient>,
}

impl JitoExecutor {
    /// Initializes a new JitoExecutor with the dual-routing network configuration.
    pub fn new(jito_rpc_url: String, jito_bundle_url: String, solana_rpc: Arc<RpcClient>) -> Self {
        Self {
            jito_rpc_url,
            jito_bundle_url,
            http_client: Client::new(),
            solana_rpc,
        }
    }

    /// Fetches live Jito tip accounts dynamically via JSON-RPC.
    /// Safely routes to the standard RPC endpoint to avoid JSON parsing crashes on the POST-only bundle route.
    pub async fn fetch_tip_accounts(&self) -> Result<Vec<Pubkey>> {
        debug!("🎯 Fetching live Jito tip accounts...");
        
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTipAccounts",
            "params": []
        });

        // FIXED: Routes strictly to self.jito_rpc_url 
        let response = self.http_client.post(&self.jito_rpc_url)
            .json(&payload)
            .send()
            .await
            .context("Failed to send getTipAccounts request")?
            .json::<Value>()
            .await
            .context("Failed to parse getTipAccounts JSON response")?;

        let accounts: Vec<Pubkey> = response["result"]
            .as_array()
            .context("Invalid getTipAccounts format returned by Jito")?
            .iter()
            .filter_map(|v| v.as_str().and_then(|s| s.parse::<Pubkey>().ok()))
            .collect();

        if accounts.is_empty() {
            anyhow::bail!("CRITICAL: No Jito tip accounts returned from Block Engine");
        }

        Ok(accounts)
    }

    /// Calculates a dynamic tip based on recent prioritization fees from the Solana network.
    pub fn calculate_dynamic_tip(&self) -> Result<u64> {
        debug!("💸 Calculating dynamic tip floor based on network congestion...");
        
        let fees = self.solana_rpc.get_recent_prioritization_fees(&[])?;
        if fees.is_empty() {
            return Ok(10_000); // Baseline fallback tip if network is completely idle
        }

        // Calculate the 75th percentile of recent network fees to ensure high landing probability
        let mut sorted_fees: Vec<u64> = fees.into_iter().map(|f| f.prioritization_fee).collect();
        sorted_fees.sort_unstable();
        
        let index = (sorted_fees.len() as f64 * 0.75).floor() as usize;
        let p75_fee = sorted_fees.get(index).copied().unwrap_or(10_000);
        
        // Jito's minimum is strictly 1000 lamports, but we enforce a higher minimum to stay competitive
        let dynamic_tip = p75_fee.max(10_000); 
        
        Ok(dynamic_tip)
    }

    /// Builds the VersionedTransaction and submits the Jito bundle to the Block Engine.
    pub async fn send_bundle(
        &self,
        keypair: &Keypair,
        blockhash: Hash,
        tip_amount: u64,
    ) -> Result<String> {
        let tip_accounts = self.fetch_tip_accounts().await?;
        
        // Randomly rotate tip accounts to reduce contention across the network
        let mut rng = rand::thread_rng();
        let tip_account = tip_accounts.choose(&mut rng).context("No tip accounts available")?;

        info!("💎 Injecting Dynamic Tip: {} lamports | Target: {}", tip_amount, tip_account);

        // 1. Core Instruction: 0-lamport self transfer 
        // (Allows us to push txs on Devnet without burning real SOL balance)
        let transfer_ix = system_instruction::transfer(
            &keypair.pubkey(),
            &keypair.pubkey(),
            0,
        );

        // 2. Tip Instruction: Pay the Jito validator network
        let tip_ix = system_instruction::transfer(
            &keypair.pubkey(),
            tip_account,
            tip_amount,
        );

        let instructions = vec![transfer_ix, tip_ix];

        // 3. Compile the highly optimized Versioned Message
        let message = VersionedMessage::V0(Message::try_compile(
            &keypair.pubkey(),
            &instructions,
            &[],
            blockhash,
        )?);

        let transaction = VersionedTransaction::try_new(message, &[keypair])?;
        
        // Serialize and encode to Base58 for Jito's JSON-RPC wire format
        let raw_tx = bincode::serialize(&transaction)?;
        let encoded_tx = bs58::encode(raw_tx).into_string();

        // 4. Dispatch the Bundle to Jito Block Engine
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [
                [encoded_tx]
            ]
        });

        debug!("🚀 Transmitting bundle to Frankfurt Block Engine...");
        
        // FIXED: Routes the bundle execution strictly to the JITO_BUNDLE_URL
        let response = self.http_client.post(&self.jito_bundle_url)
            .json(&payload)
            .send()
            .await?
            .json::<Value>()
            .await?;

        // 5. Fault Interception: Route network rejection directly back to the AI loop
        if let Some(err) = response.get("error") {
            let err_msg = err["message"].as_str().unwrap_or("Unknown simulation error");
            warn!("🛑 Bundle Simulation Failed: {}", err_msg);
            
            // Throw the error so `main.rs` can catch it and pass it to OpenRouter
            anyhow::bail!("{}", err_msg);
        }

        let bundle_id = response["result"]
            .as_str()
            .context("Valid bundle_id not found in successful response")?
            .to_string();
            
        info!("✅ Bundle dispatched successfully! Inflight ID: {}", bundle_id);

        Ok(bundle_id)
    }
}
