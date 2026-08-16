# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project Overview
Real-time 5G cell tower telemetry monitoring demo. Streams mock telemetry into Confluent Cloud (Kafka), aggregates anomalies via Apache Flink SQL, and surfaces them in a Node.js web dashboard.

## Technology Stack
- **Infrastructure:** Terraform (Confluent Cloud Provider)
- **Data Streaming & Processing:** Confluent Kafka, Flink SQL
- **Streaming AI Agent:** Confluent Flink Streaming Agents (`CREATE CONNECTION` → `CREATE MODEL` → `CREATE AGENT` → `AI_RUN_AGENT`) powered by Google Gemini (`googleai` provider, `gemini-2.0-flash`)
- **Mock Data Generator:** Node.js (KafkaJS)
- **Frontend/Backend:** Express, Socket.io, Leaflet.js
- **AI Integration:** `@confluentinc/mcp-confluent` via Node.js MCP Client

## Expected Project Structure (greenfield — not yet created)
```
terraform/          # Confluent Cloud infra (topics, connectors, Flink, SA, RBAC)
generator/          # Node.js KafkaJS mock data producer
server/             # Express + Socket.io backend + MCP client
public/             # Leaflet.js frontend
flink/              # Flink SQL statements
```

## Critical Conventions

### Security
- **Never commit** `.env`, `.tfvars`, `.tfstate`, or `.tfstate.backup` — all are in `.bobignore`/`.gitignore`.
- Terraform must use Service Accounts with least-privilege RBAC (not API keys with broad scope).
- All secrets (Kafka bootstrap URL, API keys, MCP tokens) go in `.env` files, never hardcoded.

### Kafka / Flink
- Flink uses **60-second tumbling windows** for anomaly aggregation — do not change window size without updating alert thresholds.
- Topic naming: use lowercase kebab-case (e.g., `cell-tower-telemetry`, `anomaly-alerts`, `ai-agent-responses`).
- Flink SQL statements live in `flink/` and are applied via Terraform or Confluent Cloud UI — not run ad-hoc.

### Flink Streaming Agent
- `flink/gemini_agent.sql` is a **compound statement**: it contains `CREATE CONNECTION`, `CREATE MODEL`, `CREATE AGENT`, and a continuous `INSERT INTO … AI_RUN_AGENT(…)` — all applied as a single `confluent_flink_statement` resource in `terraform/agent.tf`.
- The Google AI API key is stored in `var.google_api_key` (sensitive) and interpolated via `templatefile()` in Terraform — it is **never** in the `.sql` source file.
- The agent session key is `CONCAT(tower_id, '_', window_start)` — isolates each 60-second window, preventing cross-alert context bleed.
- `flink/gemini_agent.sql` must **not** be executed ad-hoc.

### Node.js Services
- Package manager: **npm** (use `npm install`, not `yarn` or `pnpm`).
- Generator and server are separate Node.js packages — each has its own `package.json`.
- MCP client connects to `@confluentinc/mcp-confluent` via stdio transport (spawns a child process).
- Socket.io emits real-time events from the Kafka consumer to the browser — do not poll REST APIs for live data.

### Terraform
- Run all `terraform` commands from the `terraform/` directory.
- Use `terraform.tfvars` (gitignored) to supply `confluent_cloud_api_key`, `confluent_cloud_api_secret`, `google_api_key`, and environment IDs.
- Resource naming convention: `<project>-<component>-<env>` (e.g., `celltower-producer-sa-dev`).

## Commands
| Task | Command |
|---|---|
| Install generator deps | `cd generator && npm install` |
| Install server deps | `cd server && npm install` |
| Run generator | `cd generator && npm start` |
| Run server | `cd server && npm start` |
| Terraform init | `cd terraform && terraform init` |
| Terraform plan | `cd terraform && terraform plan -var-file=terraform.tfvars` |
| Terraform apply | `cd terraform && terraform apply -var-file=terraform.tfvars` |
