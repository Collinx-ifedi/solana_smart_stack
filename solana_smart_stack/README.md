# Smart Transaction Stack (Solana MEV & Autonomous Recovery)

A high-performance, fault-tolerant Solana infrastructure stack designed for real-time transaction ingestion, autonomous AI-driven error recovery, and low-latency Jito bundle execution.

## 🚀 Architecture Overview
This system utilizes a decoupled, asynchronous micro-module design to ensure maximum uptime during volatile network conditions.

* **Ingestion Engine (`src/geyser.rs`)**: Connects to Solana via Yellowstone gRPC with automated exponential backoff and connection pooling.
* **Execution Core (`src/jito_executor.rs`)**: Handles transactional lifecycle, dynamic tip calculation (using 75th percentile market data), and Jito block engine submission.
* **Autonomous Agent (`src/ai_agent.rs`)**: An integrated reasoning engine (GPT-4o-mini) that interprets runtime transaction failures and executes corrective actions autonomously.



## 🛠️ Technical Stack
* **Runtime**: `tokio` (Asynchronous multi-threading)
* **Networking**: `reqwest` + `rustls` (Statically compiled, high-performance TLS)
* **Telemetry**: `tracing` (Structured JSON logging for observability)
* **Solana Interface**: `yellowstone-grpc-client` & `jito-sdk-rust`
* **Compiler Optimization**: `LTO=fat`, `codegen-units=1`, `panic=abort`

## ⚙️ Operational Setup

1. **Environment Variables**: Populate your `.env` file:
   ```bash
   SOLINFRA_GRPC_URL=...
   SOLINFRA_RPC_URL=...
   PRIVATE_KEY=[...] # JSON array of your keypair
   OPENAI_API_KEY=sk-...


Run:
cargo run --release

Audit:
View autonomous recovery traces:
cat lifecycle.log
    
🧠 FAQ: Infrastructure & Networking
​Q1: What is the significance of the delta between "Processed" and "Confirmed" commitments in high-frequency trading?
​Answer: "Processed" indicates the leader has included the transaction in a block. "Confirmed" signifies 66% stake consensus. High-frequency stacks use "Processed" for optimistic detection but must verify "Confirmed" or "Finalized" before finalizing profit-taking to avoid interacting with reverted states in a fork.
​Q2: What are the risks of using "Finalized" blockhashes in a high-concurrency MEV system?
​Answer: "Finalized" blockhashes ensure the block exists on the canonical chain, eliminating orphan risk. However, they are ~13+ seconds behind "Processed." In MEV, 13-second-old blockhashes are often too stale to be accepted by the block engine, leading to high transaction drop rates.
​Q3: Why are Jito bundles sometimes skipped by the Block Engine?
​Answer: Bundles are skipped due to:
​Contention: Referencing accounts modified by a higher-priority bundle in the same slot.
​Compute Limits: Exceeding the block's CU capacity.
​Stale Data: Using an expired blockhash (beyond the ~150-slot window).
​Simulation Failure: Atomic failure where any single transaction in the bundle fails.
​Insufficient Tip: Failing to beat the auction threshold for the specific slot.
