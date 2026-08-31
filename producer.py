"""
edge_capture_gateway.py
=======================
Trackside Vibration Data Producer for Kafka

This producer reads trackside vibration data from CSV file
and publishes each row as a JSON message to the 'trackside.vibration.raw'
Kafka topic, streaming continuously by looping through the file.

Usage:
    python producer.py [--bootstrap-servers SERVERS] [--topic TOPIC] [--rate RATE] [--file FILE]

Requirements:
    pip install kafka-python
"""

import argparse
import csv
import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Dict, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TracksideVibrationProducer:

    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topic: str = 'trackside.vibration.raw',
        data_file: str = 'trackside_vibration_raw.csv',
        rate: float = 10.0
    ):
        self.topic = topic
        self.data_file = data_file
        self.rate = rate
        self.running = True
        self.messages_sent = 0
        self.start_time = None

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1,
                linger_ms=10,
                batch_size=16384,
                compression_type='gzip'
            )
            logger.info(f"Kafka producer initialized. Broker: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def display_record(self, record: Dict[str, Any], msg_num: int):
        """Display all fields of a record in a readable format."""
        defect = record.get('defect', {})
        accel = record.get('acceleration', {})
        vib = record.get('vibration_features', {})
        env = record.get('environment', {})

        defect_status = "DEFECT DETECTED" if defect.get('flag') else "No Defect"

        print("\n" + "=" * 70)
        print(f"  RECORD #{msg_num} | {record.get('timestamp', 'N/A')}")
        print("=" * 70)

        print(f"  Sensor ID       : {record.get('sensor_id', 'N/A')}")
        print(f"  Sensor Location : {record.get('sensor_location', 'N/A')}")
        print(f"  Track Section   : {record.get('track_section_id', 'N/A')}")

        print("-" * 70)
        print("  TRAIN INFO")
        print("-" * 70)
        print(f"  Train ID        : {record.get('train_id', 'N/A')}")
        print(f"  Train Type      : {record.get('train_type', 'N/A')}")
        print(f"  Direction       : {record.get('direction', 'N/A')}")
        print(f"  Speed           : {record.get('speed_kmh', 0)} km/h")

        print("-" * 70)
        print("  VIBRATION - ACCELERATION (m/s²)")
        print("-" * 70)
        print(f"  X-Axis  Mean: {accel.get('x_mean', 0):>8.4f}  |  Std: {accel.get('x_std', 0):>8.4f}  |  Max: {accel.get('x_max', 0):>8.4f}  |  RMS: {accel.get('x_rms', 0):>8.4f}")
        print(f"  Y-Axis  Mean: {accel.get('y_mean', 0):>8.4f}  |  Std: {accel.get('y_std', 0):>8.4f}  |  Max: {accel.get('y_max', 0):>8.4f}  |  RMS: {accel.get('y_rms', 0):>8.4f}")
        print(f"  Z-Axis  Mean: {accel.get('z_mean', 0):>8.4f}  |  Std: {accel.get('z_std', 0):>8.4f}  |  Max: {accel.get('z_max', 0):>8.4f}  |  RMS: {accel.get('z_rms', 0):>8.4f}")

        print("-" * 70)
        print("  VIBRATION FEATURES")
        print("-" * 70)
        print(f"  Kurtosis        : {vib.get('kurtosis', 0):>8.4f}")
        print(f"  Skewness        : {vib.get('skewness', 0):>8.4f}")
        print(f"  Peak-to-RMS     : {vib.get('peak_to_rms_ratio', 0):>8.4f}")
        print(f"  Sampling Rate   : {record.get('sampling_rate_hz', 0)} Hz")

        print("-" * 70)
        print("  ENVIRONMENT")
        print("-" * 70)
        print(f"  Temperature     : {env.get('temperature_c', 0)} °C")
        print(f"  Humidity        : {env.get('humidity_pct', 0)} %")
        print(f"  Weather         : {env.get('weather_condition', 'N/A')}")
        print(f"  Signal Quality  : {record.get('signal_quality', 0)}")

        print("-" * 70)
        print(f"  DEFECT STATUS   : {defect_status}")
        if defect.get('flag'):
            print(f"  Defect Type     : {defect.get('type', 'N/A')}")
            print(f"  Defect Severity : {defect.get('severity', 0)}")
        print("=" * 70)

    def delivery_callback(self, record_metadata, record_id):
        if record_metadata:
            self.messages_sent += 1
            elapsed = time.time() - self.start_time
            rate_actual = self.messages_sent / elapsed if elapsed > 0 else 0
            logger.info(
                f"[MSG #{self.messages_sent}] Sent to "
                f"{record_metadata.topic}[{record_metadata.partition}] "
                f"| Offset: {record_metadata.offset} "
                f"| Rate: {rate_actual:.1f} msg/sec"
            )
        else:
            logger.warning(f"Message {record_id} delivery failed")

    def error_callback(self, exc):
        logger.error(f"Message delivery failed: {exc}")

    def serialize_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        try:
            return {
                'record_id': row.get('record_id', ''),
                'timestamp': row.get('timestamp', ''),
                'sensor_id': row.get('sensor_id', ''),
                'sensor_location': row.get('sensor_location', ''),
                'train_id': row.get('train_id', ''),
                'train_type': row.get('train_type', ''),
                'direction': row.get('direction', ''),
                'speed_kmh': float(row.get('speed_kmh', 0)),
                'acceleration': {
                    'x_mean': float(row.get('acceleration_x_mean', 0)),
                    'y_mean': float(row.get('acceleration_y_mean', 0)),
                    'z_mean': float(row.get('acceleration_z_mean', 0)),
                    'x_std': float(row.get('acceleration_x_std', 0)),
                    'y_std': float(row.get('acceleration_y_std', 0)),
                    'z_std': float(row.get('acceleration_z_std', 0)),
                    'x_max': float(row.get('acceleration_x_max', 0)),
                    'y_max': float(row.get('acceleration_y_max', 0)),
                    'z_max': float(row.get('acceleration_z_max', 0)),
                    'x_rms': float(row.get('acceleration_x_rms', 0)),
                    'y_rms': float(row.get('acceleration_y_rms', 0)),
                    'z_rms': float(row.get('acceleration_z_rms', 0)),
                },
                'vibration_features': {
                    'kurtosis': float(row.get('vibration_kurtosis', 0)),
                    'skewness': float(row.get('vibration_skewness', 0)),
                    'peak_to_rms_ratio': float(row.get('peak_to_rms_ratio', 0)),
                },
                'sampling_rate_hz': int(row.get('sampling_rate_hz', 10000)),
                'environment': {
                    'temperature_c': float(row.get('temperature_c', 25)),
                    'humidity_pct': float(row.get('humidity_pct', 50)),
                    'weather_condition': row.get('weather_condition', 'Clear'),
                },
                'defect': {
                    'flag': row.get('wheel_defect_flag', 'False').lower() == 'true',
                    'type': row.get('defect_type', 'None') if row.get('defect_type') else 'None',
                    'severity': float(row.get('defect_severity', 0)),
                },
                'track_section_id': row.get('track_section_id', ''),
                'signal_quality': float(row.get('signal_quality', 0.8)),
                'metadata': {
                    'producer_version': '2.0.0',
                    'schema_version': '1.0',
                    'produced_at': datetime.now(timezone.utc).isoformat(),
                }
            }
        except (ValueError, TypeError) as e:
            logger.error(f"Error serializing row {row.get('record_id')}: {e}")
            return None

    def send_record(self, record: Dict[str, Any]) -> bool:
        try:
            record_id = record.get('record_id', 'unknown')
            future = self.producer.send(
                topic=self.topic,
                key=record_id,
                value=record
            )
            future.add_callback(lambda metadata: self.delivery_callback(metadata, record_id))
            future.add_errback(self.error_callback)
            return True
        except KafkaTimeoutError:
            logger.warning(f"Kafka timeout sending record {record_id}")
            return False
        except Exception as e:
            logger.error(f"Error sending record: {e}")
            return False

    def run(self):
        print("\n" + "#" * 70)
        print("#  TRACKSIDE VIBRATION DATA PRODUCER")
        print("#  Topic: " + self.topic)
        print("#  File:  " + self.data_file)
        print("#  Rate:  " + str(self.rate) + " records/sec")
        print("#" * 70)

        self.start_time = time.time()
        interval = 1.0 / self.rate if self.rate > 0 else 0
        pass_num = 0

        try:
            while self.running:
                pass_num += 1
                print(f"\n{'*' * 70}")
                print(f"  PASS {pass_num} - Reading {self.data_file}")
                print(f"{'*' * 70}")
                row_count = 0

                with open(self.data_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if not self.running:
                            break

                        serialized = self.serialize_row(row)
                        if serialized:
                            row_count += 1
                            self.display_record(serialized, row_count)
                            self.send_record(serialized)

                        if interval > 0:
                            time.sleep(interval)

                print(f"\n{'*' * 70}")
                print(f"  PASS {pass_num} COMPLETE: {row_count} records streamed")
                print(f"{'*' * 70}")

            self.producer.flush(timeout=10)

        except KeyboardInterrupt:
            print("\n\nProducer interrupted by user")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            self.shutdown()

    def shutdown(self):
        elapsed = time.time() - self.start_time if self.start_time else 0
        print("\n" + "=" * 70)
        print("  PRODUCER SHUTDOWN")
        print("=" * 70)
        print(f"  Total Messages Sent : {self.messages_sent}")
        print(f"  Total Duration      : {elapsed:.1f} seconds")
        if elapsed > 0:
            print(f"  Average Rate        : {self.messages_sent / elapsed:.1f} msg/sec")
        print("=" * 70)
        self.running = False
        try:
            self.producer.close(timeout=10)
            print("  Kafka connection closed.")
        except Exception as e:
            logger.error(f"Error closing producer: {e}")


def parse_args():
    parser = argparse.ArgumentParser(
        description='Trackside Vibration Data Kafka Streaming Producer'
    )
    parser.add_argument(
        '--bootstrap-servers', type=str, default='localhost:9092',
        help='Kafka broker addresses (default: localhost:9092)'
    )
    parser.add_argument(
        '--topic', type=str, default='trackside.vibration.raw',
        help='Kafka topic (default: trackside.vibration.raw)'
    )
    parser.add_argument(
        '--file', type=str, default='trackside_vibration_raw.csv',
        help='CSV data file path (default: trackside_vibration_raw.csv)'
    )
    parser.add_argument(
        '--rate', type=float, default=10.0,
        help='Records per second (default: 10.0)'
    )
    return parser.parse_args()


def main():
    args = parse_args()

    producer = TracksideVibrationProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        data_file=args.file,
        rate=args.rate,
    )

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        producer.running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    producer.run()


if __name__ == '__main__':
    main()
