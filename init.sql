-- Railway Safety System - Database Initialization
-- Run: mysql -u root -proot < init.sql

CREATE DATABASE IF NOT EXISTS railway_safety
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE railway_safety;

CREATE TABLE IF NOT EXISTS vibration_records (
    id INT AUTO_INCREMENT PRIMARY KEY,
    record_id VARCHAR(36) NOT NULL,
    timestamp DATETIME NOT NULL,
    sensor_id VARCHAR(20),
    sensor_location VARCHAR(100),
    train_id VARCHAR(10),
    train_type VARCHAR(20),
    direction VARCHAR(15),
    speed_kmh FLOAT,
    acceleration_x_mean FLOAT,
    acceleration_y_mean FLOAT,
    acceleration_z_mean FLOAT,
    acceleration_x_std FLOAT,
    acceleration_y_std FLOAT,
    acceleration_z_std FLOAT,
    acceleration_x_max FLOAT,
    acceleration_y_max FLOAT,
    acceleration_z_max FLOAT,
    acceleration_x_rms FLOAT,
    acceleration_y_rms FLOAT,
    acceleration_z_rms FLOAT,
    vibration_kurtosis FLOAT,
    vibration_skewness FLOAT,
    peak_to_rms_ratio FLOAT,
    sampling_rate_hz INT,
    temperature_c FLOAT,
    humidity_pct FLOAT,
    wheel_defect_flag BOOLEAN DEFAULT FALSE,
    defect_type VARCHAR(20) DEFAULT 'None',
    defect_severity FLOAT DEFAULT 0,
    track_section_id VARCHAR(10),
    weather_condition VARCHAR(20),
    signal_quality FLOAT,
    INDEX idx_timestamp (timestamp),
    INDEX idx_defect_flag (wheel_defect_flag),
    INDEX idx_defect_type (defect_type),
    INDEX idx_train_type (train_type),
    INDEX idx_weather (weather_condition)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
