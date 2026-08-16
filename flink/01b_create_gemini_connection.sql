-- =============================================================================
-- 01b_create_gemini_connection.sql
-- Step 1b: Create the Google AI connection using the Gemini endpoint.
-- Applied as confluent_flink_statement.agent_connection in terraform/agent.tf.
-- Do NOT execute ad-hoc.
--
-- Uses gemini-2.0-flash-lite: 1500 RPD / 30 RPM on the free tier vs
-- gemini-2.0-flash's 200 RPD — avoids exhausting the daily quota during demos.
--
-- Injected via templatefile() so google_api_key is never stored in plain text.
-- The Confluent Flink statement API accepts exactly ONE statement per submission.
-- This file handles the CREATE; 01a_drop_gemini_connection.sql handles the DROP.
-- =============================================================================

CREATE CONNECTION IF NOT EXISTS celltower_gemini_connection
WITH (
  'type'     = 'googleai',
  'endpoint' = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-lite:generateContent',
  'api-key'  = '${google_api_key}'
);
