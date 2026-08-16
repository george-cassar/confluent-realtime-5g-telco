-- =============================================================================
-- 02b_create_gemini_model.sql
-- Step 2 of 4: Register the Gemini model.
--
-- Applied as confluent_flink_statement.agent_model in terraform/agent.tf.
-- Do NOT execute ad-hoc.
--
-- provider=googleai uses the GEMINI-GENERATE wire format by default.
-- temperature=0.1 keeps NOC action plans maximally deterministic.
-- max_tokens=1024 gives comfortable headroom for the full 7-line runbook.
--
-- IMPORTANT: the system_prompt must be a single-line SQL string — Flink SQL
-- single-quoted literals do not preserve embedded newlines. Use \n escapes;
-- the googleai provider forwards them to Gemini as actual newlines.
-- =============================================================================

CREATE MODEL celltower_gemini_model
INPUT  (prompt STRING)
OUTPUT (response STRING)
WITH (
  'provider'                    = 'googleai',
  'task'                        = 'text_generation',
  'googleai.connection'         = 'celltower_gemini_connection',
  'googleai.system_prompt'      = 'You are a 5G NOC agent. Reply using ONLY these 5 lines, nothing else:\nSEVERITY: CRITICAL|HIGH|MEDIUM|LOW\nDIAGNOSIS: <10 words max>\nACTION1: <action>\nACTION2: <action>\nACTION3: <action>\nESCALATION: <who and when>\nTICKET: P1|P2|P3|P4',
  'googleai.PARAMS.temperature' = '0.1',
  'googleai.PARAMS.max_tokens'  = '1024'
);
