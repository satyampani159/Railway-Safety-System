# Railway Safety System

A streaming data pipeline for real-time railway wheel defect detection using trackside vibration monitoring with Apache Kafka.

## Overview

This project simulates a trackside vibration monitoring system that generates synthetic accelerometer data from railway sensors and streams it through Kafka for downstream anomaly detection. The pipeline captures vibration signals across multiple sensor locations, encodes train metadata (speed, type, direction), environmental conditions, and wheel defect information.

The goal is to enable real-time safety interventions on high-density rail networks by detecting wheel defects (flat spots, spalls, cracks, corrosion, shelling) as trains pass monitoring checkpoints.

## Project Structure

```
.
├── generate_vibration_data.py   # Generates 5000 synthetic vibration records (CSV + JSON)
├── producer.py                  # Kafka producer that streams CSV data to a topic
├── validate_data.py             # Validates the generated dataset
├── requirements.txt             # Python dependencies
├── SDA.txt                      # Pipeline design and assignment documentation
└── README.md
```

## Prerequisites

- Python 3.10+
- Apache Kafka (running on `localhost:9092`)

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

### 2. Validate Data

```bash
python validate_data.py
```

### 3. Stream to Kafka

```bash
python producer.py
```

#### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--bootstrap-servers` | `localhost:9092` | Kafka broker address |
| `--topic` | `trackside.vibration.raw` | Kafka topic name |
| `--file` | `trackside_vibration_raw.csv` | Input CSV file |
| `--rate` | `10.0` | Records per second |

Example:

```bash
python producer.py --bootstrap-servers broker1:9092,broker2:9092 --rate 50
```

## Data Schema

Each record contains:

| Field | Type | Description |
|-------|------|-------------|
| `record_id` | string (UUID) | Unique record identifier |
| `timestamp` | ISO 8601 | Observation timestamp |
| `sensor_id` | string | Sensor identifier (e.g. `SENS_7912`) |
| `sensor_location` | string | Track location (e.g. `KM_151200_Between_Dejvicka_Borislavka`) |
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

## License

Academic use only.
