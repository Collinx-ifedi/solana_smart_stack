use anyhow::{Context, Result};
use futures_util::{sink::SinkExt, stream::StreamExt};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, info, warn};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient, InterceptorXToken};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions,
};

/// High-performance gRPC monitor for tracking transaction lifecycle across Solana commitment levels.
pub struct GeyserStreamMonitor {
    client: GeyserGrpcClient<InterceptorXToken>,
    wallet_pubkey: Pubkey,
}

impl GeyserStreamMonitor {
    /// Establishes a secure TLS connection to the Yellowstone Geyser endpoint.
    pub async fn new(endpoint: &str, x_token: &str, wallet_pubkey: Pubkey) -> Result<Self> {
        info!("🔌 Connecting to Yellowstone gRPC at {}...", endpoint);

        let client = GeyserGrpcClient::build_from_shared(endpoint)
            .context("Invalid gRPC endpoint URL format")?
            .x_token(Some(x_token))?
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .context("Failed to configure native TLS roots")?
            .connect()
            .await
            .context("CRITICAL: Failed to establish gRPC connection to Geyser node")?;

        info!("✅ gRPC Stream Connected. Tracking wallet: {}", wallet_pubkey);

        Ok(Self {
            client,
            wallet_pubkey,
        })
    }

    /// Opens a bidirectional stream and blocks until the specified signature achieves the `Confirmed` commitment level.
    /// Captures the latency delta required for the lifecycle log.
    pub async fn await_transaction_confirmation(
        &mut self,
        target_signature: &Signature,
        start_time: Instant,
    ) -> Result<u64> {
        let (mut sink, mut stream) = self.client
            .subscribe()
            .await
            .context("Failed to open bidirectional gRPC stream")?;

        // 1. Construct the Network Filter
        // To prevent network overload, we filter exclusively for our wallet's successful, non-vote transactions.
        let mut tx_filter = HashMap::new();
        tx_filter.insert(
            "target_wallet_tracker".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                account_include: vec![self.wallet_pubkey.to_string()],
                ..Default::default() // Future-proofs against newer protobuf schema fields
            },
        );

        // 2. Compile the Subscription Schema
        let request = SubscribeRequest {
            slots: HashMap::new(),
            accounts: HashMap::new(),
            transactions: tx_filter,
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: Some(CommitmentLevel::Confirmed as i32), // Monitor up to Confirmed stage
            accounts_data_slice: vec![],
            ping: None,
            from_epoch: None,
        };

        // 3. Dispatch the Filter Request
        sink.send(request)
            .await
            .context("Failed to transmit subscription filter to Geyser node")?;

        debug!("📡 Awaiting Signature {} over gRPC stream...", target_signature);

        // 4. Ingest and Process the Stream Telemetry
        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => {
                    if let Some(update) = msg.update_oneof {
                        match update {
                            UpdateOneof::Transaction(tx_update) => {
                                // Extract the 64-byte signature array from the raw protobuf transaction
                                let sig_bytes = tx_update
                                    .transaction
                                    .as_ref()
                                    .and_then(|t| Some(t.signature.as_slice()))
                                    .unwrap_or_default();

                                if let Ok(sig) = Signature::try_from(sig_bytes) {
                                    if sig == *target_signature {
                                        let latency_ms = start_time.elapsed().as_millis();
                                        info!(
                                            "🟢 gRPC STREAM CONFIRMATION: Signature {} landed in Slot {}! Latency: {}ms",
                                            sig, tx_update.slot, latency_ms
                                        );
                                        return Ok(tx_update.slot);
                                    }
                                }
                            }
                            UpdateOneof::Ping(_) => {
                                // Yellowstone nodes periodically send Pings to prevent load balancer timeouts
                                debug!("💓 gRPC Ping received. Stream healthy.");
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    warn!("⚠️ gRPC stream interrupted: {}. Validating final state...", e);
                    anyhow::bail!("Connection lost during tracking: {}", e);
                }
            }
        }

        anyhow::bail!("gRPC Stream terminated unexpectedly before confirmation.")
    }
}
