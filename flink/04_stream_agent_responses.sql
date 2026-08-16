-- =============================================================================
-- 04_stream_agent_responses.sql
-- Step 4 of 4: Call Gemini on every anomaly-alerts event and write the
-- AI diagnosis to ai-agent-responses.
--
-- Applied as confluent_flink_statement.agent_insert in terraform/agent.tf.
-- Do NOT execute ad-hoc.
--
-- Uses ML_PREDICT (AI_COMPLETE) rather than AI_RUN_AGENT — plain prompt→response
-- with zero framework overhead.
--
-- Rate-limit strategy (free-tier Gemini API):
--   • WHERE filter: only windows with avg_temperature > 88 OR avg_signal < -93
--     are sent to the model. Normal anomaly windows (just over the 85°C / -90dBm
--     thresholds) are skipped. This cuts call volume ~60% at steady state and
--     almost eliminates catch-up replay calls on restart.
--   • async_enabled=false: one in-flight call at a time; Flink backpressures
--     the source automatically.
--   • retry_count=3: retry up to 3 times on transient 429s (Confluent runtime
--     applies its own exponential back-off between attempts).
--   • client_timeout=30: per-call timeout in seconds.
-- =============================================================================

INSERT INTO `ai-agent-responses`
SELECT
  CAST(tower_id AS BYTES)                      AS key,
  tower_id,
  tower_name,
  window_start,
  window_end,
  avg_temperature,
  avg_signal,
  max_temperature,
  min_signal,
  event_count,

  -- ML_PREDICT returns a ROW — response field maps to the model OUTPUT column name
  response                                     AS agent_response,

  -- Structured fields parsed server-side; written as NULL by Flink.
  CAST(NULL AS STRING)                         AS severity_level,
  CAST(NULL AS STRING)                         AS diagnosis,
  CAST(NULL AS STRING)                         AS immediate_actions,
  CAST(NULL AS STRING)                         AS escalation,
  CAST(NULL AS STRING)                         AS ticket_priority,

  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS processed_at

FROM `anomaly-alerts`,
LATERAL TABLE(
  ML_PREDICT(
    'celltower_gemini_model',
    CONCAT(
      'Tower:', tower_id, ' ', tower_name, CHR(10),
      'AvgTemp:', CAST(ROUND(avg_temperature, 1) AS VARCHAR), 'C',
      ' MaxTemp:', CAST(ROUND(max_temperature, 1) AS VARCHAR), 'C', CHR(10),
      'AvgSig:', CAST(ROUND(avg_signal, 1) AS VARCHAR), 'dBm',
      ' MinSig:', CAST(ROUND(min_signal, 1) AS VARCHAR), 'dBm', CHR(10),
      'Events:', CAST(event_count AS VARCHAR)
    ),
    MAP[
      'async_enabled',  'false',
      'retry_count',    '3',
      'client_timeout', '30'
    ]
  )
)
-- Only escalate genuinely severe windows to the AI agent.
-- Windows that only barely cross the 85°C / -90 dBm anomaly threshold are
-- recorded in anomaly-alerts for dashboarding but do not consume Gemini quota.
WHERE avg_temperature > 88.0
   OR avg_signal < -93.0;
