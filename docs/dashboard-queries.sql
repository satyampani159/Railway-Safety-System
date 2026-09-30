-- ============================================================================
-- Railway Safety System — Dashboard SQL Queries
-- Database : railway_safety (MySQL 8.0, sda-mysql-1 on localhost:3306)
-- Dashboard: Grafana http://localhost:3001/d/railway-safety (uid: railway-safety)
-- Window   : every query is locked to 05:30-05:45 IST
--            (2024-01-01 00:00:00 - 00:15:00 UTC)
--
-- Run everything:
--   docker exec -i sda-mysql-1 mysql -uroot -proot railway_safety < docs/dashboard-queries.sql
-- Run one panel (example P5):
--   docker exec -it sda-mysql-1 mysql -uroot -proot railway_safety -e "SELECT ..."
-- NOTE on P8: Grafana substitutes the $defect variable in the browser.
--   Below it is baked in with the "All" value (all 5 defect types).
--   A single-type variant (Flat only) is included commented out.
-- Expected live results: 1101 records, 150 defects, max severity 0.992
-- ============================================================================

-- ----------------------------------------------------------------------------
-- P1 - Total Records (Stat, live cumulative wave - headline: 1101)
-- Per-minute cumulative total via window function; last row = total.
-- ----------------------------------------------------------------------------
SELECT t AS time, SUM(c) OVER (ORDER BY t) AS value FROM (
  SELECT FLOOR(UNIX_TIMESTAMP(`timestamp`)/60)*60000 AS t, COUNT(*) AS c
  FROM vibration_records
  WHERE `timestamp` BETWEEN '2024-01-01 00:00:00' AND '2024-01-01 00:15:00'
  GROUP BY 1) s ORDER BY 1;

-- ----------------------------------------------------------------------------
-- P2 - Total Defects Detected (Stat, live cumulative wave - headline: 150)
-- Same pattern, defective records only.
-- ----------------------------------------------------------------------------
SELECT t AS time, SUM(c) OVER (ORDER BY t) AS value FROM (
  SELECT FLOOR(UNIX_TIMESTAMP(`timestamp`)/60)*60000 AS t, COUNT(*) AS c
  FROM vibration_records
  WHERE wheel_defect_flag = 1
    AND `timestamp` BETWEEN '2024-01-01 00:00:00' AND '2024-01-01 00:15:00'
  GROUP BY 1) s ORDER BY 1;

-- ----------------------------------------------------------------------------
-- P3 - Max Defect Severity (Stat, running-max wave - headline: 0.992)
-- Running maximum; last row = global worst case in the window.
-- ----------------------------------------------------------------------------
SELECT t AS time, MAX(m) OVER (ORDER BY t) AS value FROM (
  SELECT FLOOR(UNIX_TIMESTAMP(`timestamp`)/60)*60000 AS t, MAX(defect_severity) AS m
  FROM vibration_records
  WHERE `timestamp` BETWEEN '2024-01-01 00:00:00' AND '2024-01-01 00:15:00'
  GROUP BY 1) s ORDER BY 1;

-- ----------------------------------------------------------------------------
-- P4 - Defect Rate Over Time (Time series, 1-minute gaps - 10 points)
-- % of records carrying a defect, per minute. Grafana plots `time` (epoch ms).
-- ----------------------------------------------------------------------------
SELECT FLOOR(UNIX_TIMESTAMP(`timestamp`) / 60) * 60000 AS time,
       AVG(wheel_defect_flag) * 100 AS defect_rate
FROM vibration_records
WHERE `timestamp` BETWEEN '2024-01-01 00:00:00' AND '2024-01-01 00:15:00'
GROUP BY 1 ORDER BY 1;

-- ----------------------------------------------------------------------------
-- P5 - Defects by Type (Bar chart - Corrosion 37, Flat 32, Spall 31,
--      Crack 26, Shelling 24)
-- ----------------------------------------------------------------------------
SELECT defect_type AS DefectType, COUNT(*) AS Count
FROM vibration_records
WHERE wheel_defect_flag = 1 AND defect_type != 'None'
  AND `timestamp` BETWEEN '2024-01-01 00:00:00' AND '2024-01-01 00:15:00'
GROUP BY defect_type ORDER BY Count DESC;

-- ----------------------------------------------------------------------------
-- P6 - Avg Vibration RMS by Speed Band (Grouped bars, Healthy vs Defective)
-- One row per speed band, ordered slow -> fast via MIN(speed_kmh).
-- ----------------------------------------------------------------------------
SELECT CASE WHEN speed_kmh < 40 THEN '0-40' WHEN speed_kmh < 80 THEN '40-80'
            WHEN speed_kmh < 120 THEN '80-120' WHEN speed_kmh < 160 THEN '120-160'
            ELSE '160+' END AS SpeedBand,
       AVG(CASE WHEN wheel_defect_flag = 0 THEN acceleration_x_rms END) AS Healthy,
       AVG(CASE WHEN wheel_defect_flag = 1 THEN acceleration_x_rms END) AS Defective
FROM vibration_records
WHERE `timestamp` BETWEEN '2024-01-01 00:00:00' AND '2024-01-01 00:15:00'
GROUP BY SpeedBand ORDER BY MIN(speed_kmh);

-- ----------------------------------------------------------------------------
-- P7 - Defects by Train Type (Bar gauge - Freight 28, Metro 28,
--      Passenger 26, DMU 24, HighSpeed 23, EMU 21)
-- ----------------------------------------------------------------------------
SELECT train_type AS metric, COUNT(*) AS value
FROM vibration_records
WHERE wheel_defect_flag = 1
  AND `timestamp` BETWEEN '2024-01-01 00:00:00' AND '2024-01-01 00:15:00'
GROUP BY train_type ORDER BY value DESC;

-- ----------------------------------------------------------------------------
-- P8 - Defect Severity Distribution Over Time (Heatmap - 150 points)
-- $defect baked in with the "All" selection (all 5 defect types).
-- ----------------------------------------------------------------------------
SELECT UNIX_TIMESTAMP(`timestamp`) * 1000 AS time, defect_severity AS severity
FROM vibration_records
WHERE wheel_defect_flag = 1
  AND defect_type IN ('Flat','Spall','Crack','Corrosion','Shelling')
  AND `timestamp` BETWEEN '2024-01-01 00:00:00' AND '2024-01-01 00:15:00'
ORDER BY time;

-- P8 single-type variant (Flat only - mirrors picking "Flat" in the dropdown):
-- SELECT UNIX_TIMESTAMP(`timestamp`) * 1000 AS time, defect_severity AS severity
-- FROM vibration_records
-- WHERE wheel_defect_flag = 1
--   AND defect_type IN ('Flat')
--   AND `timestamp` BETWEEN '2024-01-01 00:00:00' AND '2024-01-01 00:15:00'
-- ORDER BY time;

-- ----------------------------------------------------------------------------
-- Dashboard variable `defect` (dropdown above the heatmap - 5 values)
-- ----------------------------------------------------------------------------
SELECT DISTINCT defect_type FROM vibration_records WHERE wheel_defect_flag = 1 ORDER BY defect_type;
