use anyhow::{Context, Result};
use futures_util::{sink::SinkExt, stream::StreamExt};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions,
};

/// High-performance gRPC monitor for tracking transaction lifecycle across Solana commitment levels.
pub struct GeyserStreamMonitor {
    // Clean, simple layout exposed by the v13.1.0 client (No InterceptorXToken required)
    client: GeyserGrpcClient,
    wallet_pubkey: Pubkey,
}

impl GeyserStreamMonitor {
    /// Establishes a secure connection to the Yellowstone Geyser endpoint.
    pub async fn new(endpoint: &str, x_token: &str, wallet_pubkey: Pubkey) -> Result<Self> {
        info!("🔌 Connecting to Yellowstone gRPC at {}...", endpoint);

        // Uses the modernized v13 builder pattern
        let client = GeyserGrpcClient::build_from_shared(endpoint.to_string())
            .context("Invalid gRPC endpoint URL structure configuration")?
            .x_token(Some(x_token.to_string()))
            .context("Failed to assign authentication x-token payload mapping")?
            .connect()
            .await
            .context("CRITICAL: Failed to establish a secure gRPC connection channel to Geyser node")?;

        info!("✅ gRPC Stream Connected. Tracking target wallet: {}", wallet_pubkey);

        Ok(Self {
            client,
            wallet_pubkey,
        })
    }

    /// Opens a bidirectional stream and blocks until the specified signature achieves the `Confirmed` commitment level.
    pub async fn await_transaction_confirmation(
        &mut self,
        target_signature: &Signature,
        start_time: Instant,
    ) -> Result<u64> {
        // Establishes the bidirectional communication streams
        let (mut sink, mut stream) = self.client
            .subscribe()
            .await
            .context("Failed to open bidirectional stream channel over Geyser subscription loop")?;

        // Instantiate transactional filters for your specific runtime address target
        let mut tx_filter = HashMap::new();
        tx_filter.insert(
            "target_wallet_tracker".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                account_include: vec![self.wallet_pubkey.to_string()],
                account_exclude: vec![],
                account_required: vec![],
                signature: None,
                // SOLUTION: Use functional struct update syntax to cleanly assign protocol-dependent fields 
                // like `token_accounts` automatically based on the underlying compiled crate version specifications.
                ..Default::default()
            },
        );

        // Create the stream structural parameters configuration object
        let request = SubscribeRequest {
            slots: HashMap::new(),
            accounts: HashMap::new(),
            transactions: tx_filter,
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: Some(CommitmentLevel::Confirmed as i32),
            accounts_data_slice: vec![],
            ping: None,
            ..Default::default() // Gracefully handles proto expansion fields (like from_epoch) automatically
        };

        // Transmit your explicit parameters to the remote server filter pipeline
        sink.send(request)
            .await
            .context("Failed to transmit subscription transaction filters to Geyser endpoint")?;

        debug!("📡 Subscription filter sent. Awaiting target Signature {} via gRPC...", target_signature);

        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => {
                    if let Some(update) = msg.update_oneof {
                        match update {
                            UpdateOneof::Transaction(tx_update) => {
                                // Extract the underlying transaction byte payload
                                let sig_bytes = tx_update
                                    .transaction
                                    .as_ref()
                                    .and_then(|t| Some(t.signature.as_slice()))
                                    .unwrap_or_default();

                                if let Ok(sig) = Signature::try_from(sig_bytes) {
                                    if sig == *target_signature {
                                        let latency_ms = start_time.elapsed().as_millis();
                                        info!(
                                            "🟢 gRPC STREAM CONFIRMATION: Signature {} landed inside Slot {}! Track Latency: {}ms",
                                            sig, tx_update.slot, latency_ms
                                        );
                                        return Ok(tx_update.slot);
                                    }
                                }
                            }
                            UpdateOneof::Ping(_) => {
                                // Yellowstone nodes periodically broadcast Pings to keep connections warm
                                debug!("💓 gRPC Ping intercepted. Pipeline frame stream connection healthy.");
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    warn!("⚠️ Yellowstone gRPC pipeline stream interrupted: {}. Validating fallback state tracker...", e);
                    anyhow::bail!("Connection lost during real-time tracking loops: {}", e);
                }
            }
        }

        anyhow::bail!("Yellowstone gRPC Stream closed unexpectedly before target signature land validation.")
    }
}
