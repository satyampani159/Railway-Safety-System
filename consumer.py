"""
consumer.py
===========
Kafka Consumer → MySQL for Railway Safety System

Reads trackside vibration messages from the Kafka topic
'trackside.vibration.raw' and inserts them into MySQL
(railway_safety.vibration_records) in batches.

Usage:
    python consumer.py [--bootstrap-servers SERVERS] [--topic TOPIC]
                       [--batch-size N] [--flush-interval SECONDS]

Requirements:
    pip install kafka-python mysql-connector-python
"""

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime
from typing import List, Tuple, Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError

import mysql.connector
from mysql.connector import pooling, Error as MySQLError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS railway_safety.vibration_records (
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
"""

INSERT_SQL = """
INSERT INTO vibration_records (
    record_id, timestamp, sensor_id, sensor_location, train_id, train_type,
    direction, speed_kmh,
    acceleration_x_mean, acceleration_y_mean, acceleration_z_mean,
    acceleration_x_std, acceleration_y_std, acceleration_z_std,
    acceleration_x_max, acceleration_y_max, acceleration_z_max,
    acceleration_x_rms, acceleration_y_rms, acceleration_z_rms,
    vibration_kurtosis, vibration_skewness, peak_to_rms_ratio,
    sampling_rate_hz, temperature_c, humidity_pct,
    wheel_defect_flag, defect_type, defect_severity,
    track_section_id, weather_condition, signal_quality
) VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s
)
"""

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "railway_safety",
    "autocommit": False,
}


def parse_timestamp(ts_str: str) -> datetime:
    """Parse ISO 8601 timestamp string to datetime."""
    if not ts_str:
        return datetime.utcnow()
    try:
        cleaned = ts_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except (ValueError, AttributeError):
        return datetime.utcnow()


def flatten_record(msg: dict) -> Tuple:
    """Flatten nested Kafka message into a flat MySQL row tuple."""
    accel = msg.get("acceleration", {})
    vib = msg.get("vibration_features", {})
    env = msg.get("environment", {})
    defect = msg.get("defect", {})

    return (
        msg.get("record_id", ""),
        parse_timestamp(msg.get("timestamp", "")),
        msg.get("sensor_id", ""),
        msg.get("sensor_location", ""),
        msg.get("train_id", ""),
        msg.get("train_type", ""),
        msg.get("direction", ""),
        float(msg.get("speed_kmh", 0) or 0),
        float(accel.get("x_mean", 0) or 0),
        float(accel.get("y_mean", 0) or 0),
        float(accel.get("z_mean", 0) or 0),
        float(accel.get("x_std", 0) or 0),
        float(accel.get("y_std", 0) or 0),
        float(accel.get("z_std", 0) or 0),
        float(accel.get("x_max", 0) or 0),
        float(accel.get("y_max", 0) or 0),
        float(accel.get("z_max", 0) or 0),
        float(accel.get("x_rms", 0) or 0),
        float(accel.get("y_rms", 0) or 0),
        float(accel.get("z_rms", 0) or 0),
        float(vib.get("kurtosis", 0) or 0),
        float(vib.get("skewness", 0) or 0),
        float(vib.get("peak_to_rms_ratio", 0) or 0),
        int(msg.get("sampling_rate_hz", 10000) or 10000),
        float(env.get("temperature_c", 25) or 25),
        float(env.get("humidity_pct", 50) or 50),
        bool(defect.get("flag", False)),
        str(defect.get("type", "None") or "None"),
        float(defect.get("severity", 0) or 0),
        msg.get("track_section_id", ""),
        str(env.get("weather_condition", "Clear") or "Clear"),
        float(msg.get("signal_quality", 0.8) or 0.8),
    )


class VibrationConsumer:

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "trackside.vibration.raw",
        batch_size: int = 50,
        flush_interval: float = 5.0,
    ):
        self.topic = topic
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.running = True
        self.messages_consumed = 0
        self.messages_inserted = 0
        self.batch: List[Tuple] = []
        self.last_flush = time.time()
        self.start_time = None

        try:
            self.db = mysql.connector.connect(**DB_CONFIG)
            cursor = self.db.cursor()
            cursor.execute(CREATE_TABLE_SQL)
            self.db.commit()
            cursor.close()
            logger.info(f"MySQL connected: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        except MySQLError as e:
            logger.error(f"MySQL connection failed: {e}")
            raise

        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="railway-safety-consumer",
                max_poll_records=100,
            )
            logger.info(f"Kafka consumer subscribed to '{topic}' on {bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Kafka connection failed: {e}")
            raise

    def flush(self):
        """Insert batch into MySQL."""
        if not self.batch:
            return

        try:
            cursor = self.db.cursor()
            cursor.executemany(INSERT_SQL, self.batch)
            self.db.commit()
            self.messages_inserted += len(self.batch)
            logger.info(
                f"[BATCH] Inserted {len(self.batch)} rows | "
                f"Total: {self.messages_inserted} | "
                f"Rate: {self.messages_inserted / max(time.time() - self.start_time, 1):.1f} rows/sec"
            )
            cursor.close()
        except MySQLError as e:
            logger.error(f"MySQL insert failed: {e}")
            self.db.rollback()
        finally:
            self.batch = []
            self.last_flush = time.time()

    def run(self):
        print("\n" + "#" * 70)
        print("#  RAILWAY SAFETY - KAFKA -> MYSQL CONSUMER")
        print(f"#  Topic:    {self.topic}")
        print(f"#  MySQL:    {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        print(f"#  Batch:    {self.batch_size} records / {self.flush_interval}s flush")
        print("#" * 70 + "\n")

        self.start_time = time.time()

        try:
            for message in self.consumer:
                if not self.running:
                    break

                try:
                    row = flatten_record(message.value)
                    self.batch.append(row)
                    self.messages_consumed += 1

                    if len(self.batch) >= self.batch_size:
                        self.flush()
                    elif time.time() - self.last_flush >= self.flush_interval:
                        self.flush()

                    if self.messages_consumed % 100 == 0:
                        elapsed = time.time() - self.start_time
                        rate = self.messages_consumed / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"[STATS] Consumed: {self.messages_consumed} | "
                            f"Inserted: {self.messages_inserted} | "
                            f"Rate: {rate:.1f} msg/sec"
                        )

                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"Skipping malformed message: {e}")

        except KeyboardInterrupt:
            print("\n\nConsumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.shutdown()

    def flush_remaining(self):
        self.flush()

    def shutdown(self):
        if self.batch:
            self.flush()

        elapsed = time.time() - self.start_time if self.start_time else 0
        print("\n" + "=" * 70)
        print("  CONSUMER SHUTDOWN")
        print("=" * 70)
        print(f"  Total Consumed     : {self.messages_consumed}")
        print(f"  Total Inserted     : {self.messages_inserted}")
        print(f"  Total Duration     : {elapsed:.1f} seconds")
        if elapsed > 0:
            print(f"  Average Rate       : {self.messages_consumed / elapsed:.1f} msg/sec")
        print("=" * 70)

        self.running = False
        try:
            self.consumer.close()
            print("  Kafka connection closed.")
        except Exception as e:
            logger.error(f"Error closing Kafka consumer: {e}")
        try:
            self.db.close()
            print("  MySQL connection closed.")
        except Exception as e:
            logger.error(f"Error closing MySQL connection: {e}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Kafka Consumer -> MySQL for Railway Safety System"
    )
    parser.add_argument(
        "--bootstrap-servers", type=str, default="localhost:9092",
        help="Kafka broker addresses (default: localhost:9092)"
    )
    parser.add_argument(
        "--topic", type=str, default="trackside.vibration.raw",
        help="Kafka topic (default: trackside.vibration.raw)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=50,
        help="Batch size for MySQL inserts (default: 50)"
    )
    parser.add_argument(
        "--flush-interval", type=float, default=5.0,
        help="Max seconds between flushes (default: 5.0)"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    consumer = VibrationConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        batch_size=args.batch_size,
        flush_interval=args.flush_interval,
    )

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        consumer.running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer.run()


if __name__ == "__main__":
    main()
