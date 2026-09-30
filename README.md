# Railway Safety System

A streaming data pipeline for real-time railway wheel defect detection using trackside vibration monitoring with Apache Kafka, MySQL, and Grafana.

## Overview

This project simulates a trackside vibration monitoring system that generates synthetic accelerometer data from railway sensors and streams it through Kafka for downstream anomaly detection. A Kafka consumer writes records to MySQL, and a Grafana dashboard visualizes the live stream with 8 charts.

The goal is to enable real-time safety interventions on high-density rail networks by detecting wheel defects (flat spots, spalls, cracks, corrosion, shelling) as trains pass monitoring checkpoints.

## Architecture

```
generate_vibration_data.py → trackside_vibration_raw.csv
              ↓
producer.py → Kafka (localhost:9092, topic: trackside.vibration.raw)
              ↓
consumer.py → MySQL (localhost:3306, railway_safety.vibration_records)
              ↓
Grafana (localhost:3000) → 8-chart dashboard
```

> **Environment:** this project runs on the standard SDA course Docker stack
> (`sda-kafka-1` on :9092, `sda-mysql-1` on :3306 with user `root` / password `root`,
> `sda-grafana-1` on :3001 with login `admin` / `admin`).
> No extra infrastructure is needed. `docker-compose.yml` (MySQL + Kafka + Grafana)
> is provided as a self-contained fallback — only use it if the course stack is stopped.

## Project Structure

```
.
├── generate_vibration_data.py       # Generates 5000 synthetic vibration records (CSV + JSON)
├── producer.py                      # Kafka producer that streams CSV data to a topic
├── consumer.py                      # Kafka consumer that writes to MySQL
├── validate_data.py                 # Validates the generated dataset
├── verify_data.py                   # Validates MySQL rows + panel query shapes
├── setup_grafana.py                 # One-time Grafana setup (datasource + dashboard import via API)
├── init.sql                         # MySQL database + table creation
├── docker-compose.yml               # Fallback stack (MySQL + Kafka + Grafana) if course stack is down
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/mysql.yml    # Auto-connects Grafana → MySQL
│   │   └── dashboards/provider.yml  # Auto-loads dashboard JSON
│   └── dashboards/
│       └── railway-safety.json      # 8-panel dashboard definition
├── requirements.txt                 # Python dependencies
├── SDA.txt                          # Pipeline design and assignment documentation
└── README.md
```

## Prerequisites

- Python 3.10+
- SDA course Docker stack running: Kafka (`localhost:9092`), MySQL (`localhost:3306`,
  user `root`, password `root`), Grafana (`localhost:3000` → mapped, course instance on `:3001`)
- Docker Desktop (only needed for the `docker-compose.yml` fallback)

## Setup

```bash
# Clone the repository
git clone <repo-url>
cd Railway-Safety-System

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate   # Linux/Mac
venv\Scripts\activate      # Windows

# Install dependencies
pip install -r requirements.txt
```

## Usage

### 1. Generate Synthetic Data

```bash
python generate_vibration_data.py
```

This creates `trackside_vibration_raw.csv` and `trackside_vibration_raw.json` with 5000 records.

### 2. Initialize MySQL

```bash
mysql -u root -proot < init.sql
```

(`consumer.py` also creates the table with `IF NOT EXISTS` as a safety net.)

### 3. Register Grafana Datasource + Dashboard (one-time)

```bash
python setup_grafana.py
```

This creates the `MySQL` datasource (uid `mysql-railway`) and imports the
`Railway Safety - Trackside Vibration Monitor` dashboard via the Grafana API.
Re-running it safely overwrites the previous version.

> If you use the `docker-compose.yml` fallback stack instead of the course stack,
> start it first with `docker compose up -d` (Grafana on `:3000`) and point
> `setup_grafana.py` at the right port.

### 4. Start Kafka Consumer (Terminal 1)

```bash
python consumer.py
```

Reads from Kafka and inserts into MySQL in batches of 50.

### 5. Start Kafka Producer (Terminal 2)

```bash
python producer.py
```

Streams CSV data to Kafka at 10 records/sec.

### 6. Open Dashboard

Navigate to `http://localhost:3001/d/railway-safety` (login `admin` / `admin`).

> **Note on time range:** the synthetic data uses January 2024 timestamps, so the
> dashboard defaults to `2024-01-01 00:00 – 01:00`. Do not switch it to "Last 30 minutes"
> or the time-filtered panels will appear empty.

## Dashboard Charts

| # | Chart | Type | What It Shows |
|---|-------|------|---------------|
| 1 | Total Records | Stat | Total vibration records in MySQL |
| 2 | Total Defects Detected | Stat | Count of defective records |
| 3 | Max Defect Severity | Stat | Highest severity score (0–1) |
| 4 | Defect Rate Over Time | Time series | % of defective records per minute |
| 5 | Defects by Type | Bar chart | Flat, Spall, Crack, Corrosion, Shelling counts |
| 6 | Vibration RMS vs Speed | Scatter (XY) | RMS acceleration vs speed, colored by defect |
| 7 | Defects by Train Type | Bar gauge | Defect count per fleet segment |
| 8 | Defect Severity Distribution Over Time | Heatmap | Density of severity scores over time (X = time, Y = severity buckets), filterable by defect type dropdown |

## CLI Options

### Producer

| Flag | Default | Description |
|------|---------|-------------|
| `--bootstrap-servers` | `localhost:9092` | Kafka broker address |
| `--topic` | `trackside.vibration.raw` | Kafka topic name |
| `--file` | `trackside_vibration_raw.csv` | Input CSV file |
| `--rate` | `10.0` | Records per second |

### Consumer

| Flag | Default | Description |
|------|---------|-------------|
| `--bootstrap-servers` | `localhost:9092` | Kafka broker address |
| `--topic` | `trackside.vibration.raw` | Kafka topic name |
| `--batch-size` | `50` | Records per MySQL insert batch |
| `--flush-interval` | `5.0` | Max seconds between flushes |

## Data Schema

| Field | Type | Description |
|-------|------|-------------|
| `record_id` | string (UUID) | Unique record identifier |
| `timestamp` | ISO 8601 | Observation timestamp |
| `sensor_id` | string | Sensor identifier (e.g. `SENS_7912`) |
| `sensor_location` | string | Track location |
| `train_id` | string | Train identifier |
| `train_type` | string | Passenger, Freight, Metro, EMU, DMU, HighSpeed |
| `direction` | string | Up, Down, Bidirectional |
| `speed_kmh` | float | Train speed in km/h |
| `acceleration_{x,y,z}_{mean,std,max,rms}` | float | Vibration statistics per axis (m/s^2) |
| `vibration_kurtosis` | float | Signal kurtosis |
| `vibration_skewness` | float | Signal skewness |
| `peak_to_rms_ratio` | float | Peak-to-RMS (crest factor) |
| `sampling_rate_hz` | int | Sensor sampling rate (5000-20000 Hz) |
| `temperature_c` | float | Ambient temperature |
| `humidity_pct` | float | Relative humidity |
| `wheel_defect_flag` | bool | Whether a defect is present |
| `defect_type` | string | None, Flat, Spall, Crack, Corrosion, Shelling |
| `defect_severity` | float | Severity score (0.0 - 1.0) |
| `track_section_id` | string | Track section identifier |
| `weather_condition` | string | Clear, Cloudy, Rain, Fog, Extreme_Heat, Windy |
| `signal_quality` | float | Sensor signal quality (0.0 - 1.0) |

## Troubleshooting

**Grafana can't reach MySQL:**
- The datasource uses host `mysql:3306` (Docker-network alias for `sda-mysql-1`).
  If you run Grafana outside Docker, change the datasource URL to `localhost:3306`.
- Verify credentials: user `root`, password `root`, database `railway_safety`.
- Re-run `python setup_grafana.py` to re-register the datasource.

**Consumer can't connect to Kafka:**
- Ensure the course stack is running: `docker ps` should show `sda-kafka-1` on `:9092`.
- If the port is held by another container, stop the conflicting stack first.

**Dashboard shows "No data":**
- The data timestamps are January 2024 — keep the dashboard time range on
  `2024-01-01 00:00 – 01:00`, not "Last 30 minutes".
- Ensure producer and consumer are both running.
- Verify rows exist: run `python verify_data.py`.
- `UnicodeEncodeError` on Windows: fixed by using ASCII-only log banners in `consumer.py`.

## License

Academic use only.
