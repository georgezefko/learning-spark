
-- Scatter Plot - Is behavior anomalous vs threshold

SELECT
  device_id,
  window_start AS ts,
  avg_temperature,
  max_temperature,
  threshold_used,
  (max_temperature - threshold_used) AS delta_over,
  anomaly_flag
FROM fact_telemetry_5min
--WHERE window_start >= NOW() - INTERVAL 120 MINUTE;


-- Table - What operational events occured

SELECT
  SUM(events_failure)     AS failures,
  SUM(events_maintenance) AS maintenance,
  SUM(events_inspection)  AS inspections,
  SUM(events_sev_high)    AS sev_high,
  SUM(events_total)       AS events_total
FROM fact_telemetry_5min
--WHERE window_start >= NOW() - INTERVAL 120 MINUTE;


-- Bar Plot - Where should we look first

SELECT
  device_id,
  SUM(incomplete_flag) AS incomplete,
  SUM(anomaly_flag)    AS anomalies,
  SUM(events_failure)  AS failures
FROM fact_telemetry_5min
--WHERE window_start >= NOW() - INTERVAL 120 MINUTE
GROUP BY device_id
ORDER BY (incomplete + anomalies + failures) DESC
LIMIT 10;


-- Timeseries plot - Have enough data arrived

SELECT
  window_start AS ts,
  SUM(incomplete_flag) AS incomplete_windows,
  SUM(anomaly_flag)    AS anomalous_windows,
  SUM(events_total)    AS events
FROM fact_telemetry_5min
--WHERE window_start >= NOW() - INTERVAL 120 MINUTE
GROUP BY ts
ORDER BY ts;
