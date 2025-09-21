/*

STREAMING QUERIES

*/

-- Updated anomalies_streaming table
-- Static dim
CREATE TABLE IF NOT EXISTS dim_device (
  device_id STRING,
  temp_anomaly_threshold DOUBLE,
  location_id STRING,
  model STRING,
  status STRING
)
PRIMARY KEY(device_id)
DISTRIBUTED BY HASH(device_id);
PROPERTIES (
    "enable_persistent_index" = "true",
    "replication_num" = "1"
);


-- Seed 11 devices with thresholds (°C) + simple labels
INSERT INTO dim_device (device_id, temp_anomaly_threshold, location_id, model, status) VALUES
('device_1',  35.0, 'plant_a', 'Model-A', 'active'),
('device_2',  34.0, 'plant_b', 'Model-A', 'active'),
('device_3',  36.0, 'plant_c', 'Model-B', 'active'),
('device_4',  33.0, 'plant_a', 'Model-B', 'active'),
('device_5',  35.0, 'plant_b', 'Model-A', 'active'),
('device_6',  37.0, 'plant_c', 'Model-C', 'active'),
('device_7',  32.0, 'plant_a', 'Model-C', 'active'),
('device_8',  38.0, 'plant_b', 'Model-D', 'active'),
('device_9',  35.0, 'plant_c', 'Model-D', 'active'),
('device_10', 34.5, 'plant_a', 'Model-B', 'active'),
('device_11', 36.5, 'plant_b', 'Model-A', 'active');


-- Aggregates for dashboard
CREATE TABLE IF NOT EXISTS fact_telemetry_5min (
  window_start DATETIME,
  window_end   DATETIME,
  device_id    STRING,
  cnt_points   INT,
  avg_temperature DOUBLE,
  min_temperature DOUBLE,
  max_temperature DOUBLE,
  incomplete_flag BOOLEAN,
  anomaly_flag    BOOLEAN,
  anomaly_reason  STRING,
  threshold_used  DOUBLE,
  location_id     STRING,
  model           STRING,
  updated_at      DATETIME DEFAULT NOW()
)
PRIMARY KEY(device_id, window_start)
DISTRIBUTED BY HASH(device_id)
PROPERTIES (
    "enable_persistent_index" = "true",
    "replication_num" = "1"
);
