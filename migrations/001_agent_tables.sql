-- FlycommC2 Agent Tables
-- Run this against your ClickHouse instance before starting the agent.
-- Usage: clickhouse-client --multiquery < 001_agent_tables.sql

-- Agent watermark state (tracks last processed timestamp)
CREATE TABLE IF NOT EXISTS agent_state (
  id UUID DEFAULT generateUUIDv4(),
  last_processed_ts DateTime64(3),
  updated_at DateTime64(3) DEFAULT now()
) ENGINE = MergeTree()
ORDER BY updated_at;

-- Confirmed threat events detected by the agent
CREATE TABLE IF NOT EXISTS threat_events (
  id UUID DEFAULT generateUUIDv4(),
  detected_at DateTime64(3) DEFAULT now(),
  cell_id String,
  cell_ecgi String,
  location_lat Float64,
  location_lng Float64,
  threat_type String,
  severity LowCardinality(String),
  score Float32,
  confidence Float32,
  reasoning String,
  sample_id String,
  raw_flags Array(String),
  is_confirmed UInt8 DEFAULT 1
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(detected_at)
ORDER BY (detected_at, cell_id, threat_type);

-- Alert dispatch log
CREATE TABLE IF NOT EXISTS alert_log (
  id UUID DEFAULT generateUUIDv4(),
  threat_event_id String,
  channel LowCardinality(String),
  sent_at DateTime64(3) DEFAULT now(),
  payload String
) ENGINE = MergeTree()
ORDER BY sent_at;
