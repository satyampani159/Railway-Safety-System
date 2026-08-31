import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import random
import os

np.random.seed(42)
random.seed(42)

SENSOR_LOCATIONS = [
    "KM_151200_Between_Dejvicka_Borislavka",
    "KM_152300_Praha_Vychod",
    "KM_153100_Praha_Hlavni",
    "KM_154000_Praha_Vysehrad",
    "KM_155200_Praha_Radotin",
    "KM_156100_Praha_Liben",
    "KM_157300_Praha_Karlín",
    "KM_158400_Praha_Vrsovice",
    "KM_159200_Praha_Žižkov",
    "KM_160100_Praha_Holešovice"
]

TRAIN_TYPES = ["Passenger", "Freight", "Metro", "EMU", "DMU", "HighSpeed"]
DEFECT_TYPES = ["None", "Flat", "Spall", "Crack", "Corrosion", "Shelling"]
WEATHER_CONDITIONS = ["Clear", "Cloudy", "Rain", "Fog", "Extreme_Heat", "Windy"]
DIRECTIONS = ["Up", "Down", "Bidirectional"]

BASE_SPEED_RANGES = {
    "Passenger": (80, 160),
    "Freight": (20, 80),
    "Metro": (40, 100),
    "EMU": (60, 140),
    "DMU": (50, 120),
    "HighSpeed": (120, 180)
}

DEFECT_PROBABILITY = 0.15
HIGH_SEVERITY_PROBABILITY = 0.03

def generate_vibration_stats(n_samples, speed_kmh, defect_type, defect_severity, temperature):
    base_freq = speed_kmh / 3.6 / 2.8 / 2 * np.pi
    
    t = np.linspace(0, 2.0, n_samples)
    
    base_signal = (
        0.5 * np.sin(2 * np.pi * 50 * t) +
        0.3 * np.sin(2 * np.pi * 100 * t) +
        0.2 * np.sin(2 * np.pi * 150 * t)
    )
    
    speed_factor = speed_kmh / 100
    base_signal *= speed_factor
    
    if defect_type == "Flat":
        flat_period = 2.8 / (speed_kmh / 3.6)
        n_impacts = int(2.0 / flat_period)
        for i in range(min(n_impacts, 20)):
            impact_time = i * flat_period
            impact_idx = int(impact_time * n_samples / 2.0)
            if impact_idx < n_samples:
                impact_width = max(1, int(0.002 * n_samples / 2.0))
                impact = defect_severity * 15 * np.exp(-np.linspace(0, 10, impact_width))
                start_idx = max(0, impact_idx - impact_width // 2)
                end_idx = min(n_samples, start_idx + len(impact))
                base_signal[start_idx:end_idx] += impact[:end_idx - start_idx]
    
    elif defect_type == "Spall":
        spall_period = 2.8 / (speed_kmh / 3.6) * 1.5
        n_impacts = int(2.0 / spall_period)
        for i in range(min(n_impacts, 15)):
            impact_time = i * spall_period + 0.3
            impact_idx = int(impact_time * n_samples / 2.0)
            if impact_idx < n_samples:
                impact_width = max(1, int(0.001 * n_samples / 2.0))
                impact = defect_severity * 8 * np.random.randn(impact_width)
                start_idx = max(0, impact_idx - impact_width // 2)
                end_idx = min(n_samples, start_idx + len(impact))
                base_signal[start_idx:end_idx] += impact[:end_idx - start_idx]
    
    elif defect_type == "Crack":
        crack_modulation = 1 + defect_severity * 0.5 * np.sin(2 * np.pi * 30 * t)
        base_signal *= crack_modulation
    
    elif defect_type == "Corrosion":
        corrosion_noise = defect_severity * 0.3 * np.random.randn(n_samples)
        base_signal += corrosion_noise
    
    elif defect_type == "Shelling":
        shelling_period = 2.8 / (speed_kmh / 3.6) * 0.8
        n_impacts = int(2.0 / shelling_period)
        for i in range(min(n_impacts, 25)):
            impact_time = i * shelling_period + 0.1
            impact_idx = int(impact_time * n_samples / 2.0)
            if impact_idx < n_samples:
                impact_width = max(1, int(0.0015 * n_samples / 2.0))
                impact = defect_severity * 12 * np.sin(np.linspace(0, np.pi, impact_width))
                start_idx = max(0, impact_idx - impact_width // 2)
                end_idx = min(n_samples, start_idx + len(impact))
                base_signal[start_idx:end_idx] += impact[:end_idx - start_idx]
    
    temp_factor = 1 + 0.001 * (temperature - 25)
    base_signal *= temp_factor
    
    noise = 0.1 * np.random.randn(n_samples)
    base_signal += noise
    
    mean_x = float(np.mean(np.abs(base_signal)))
    mean_y = float(np.mean(np.abs(base_signal * 1.2)))
    mean_z = float(np.mean(np.abs(base_signal * 0.8)))
    
    std_x = float(np.std(base_signal))
    std_y = float(np.std(base_signal * 1.2))
    std_z = float(np.std(base_signal * 0.8))
    
    max_x = float(np.max(np.abs(base_signal)))
    max_y = float(np.max(np.abs(base_signal * 1.2)))
    max_z = float(np.max(np.abs(base_signal * 0.8)))
    
    rms_x = float(np.sqrt(np.mean(base_signal**2)))
    rms_y = float(np.sqrt(np.mean((base_signal * 1.2)**2)))
    rms_z = float(np.sqrt(np.mean((base_signal * 0.8)**2)))
    
    kurtosis = float(pd.Series(base_signal).kurtosis())
    skewness = float(pd.Series(base_signal).skew())
    
    return {
        "acceleration_x_mean": round(mean_x, 4),
        "acceleration_y_mean": round(mean_y, 4),
        "acceleration_z_mean": round(mean_z, 4),
        "acceleration_x_std": round(std_x, 4),
        "acceleration_y_std": round(std_y, 4),
        "acceleration_z_std": round(std_z, 4),
        "acceleration_x_max": round(max_x, 4),
        "acceleration_y_max": round(max_y, 4),
        "acceleration_z_max": round(max_z, 4),
        "acceleration_x_rms": round(rms_x, 4),
        "acceleration_y_rms": round(rms_y, 4),
        "acceleration_z_rms": round(rms_z, 4),
        "vibration_kurtosis": round(kurtosis, 4),
        "vibration_skewness": round(skewness, 4),
        "peak_to_rms_ratio": round(max_x / max(rms_x, 0.001), 4)
    }

def generate_record(record_num, start_time):
    train_type = random.choice(TRAIN_TYPES)
    speed_min, speed_max = BASE_SPEED_RANGES[train_type]
    speed_kmh = round(random.uniform(speed_min, speed_max), 1)
    
    weather = random.choice(WEATHER_CONDITIONS)
    if weather == "Rain":
        humidity = random.uniform(70, 100)
        temperature = random.uniform(15, 35)
        signal_quality = random.uniform(0.6, 0.85)
    elif weather == "Fog":
        humidity = random.uniform(80, 100)
        temperature = random.uniform(5, 20)
        signal_quality = random.uniform(0.65, 0.9)
    elif weather == "Extreme_Heat":
        humidity = random.uniform(20, 50)
        temperature = random.uniform(40, 55)
        signal_quality = random.uniform(0.7, 0.95)
    elif weather == "Clear":
        humidity = random.uniform(30, 70)
        temperature = random.uniform(15, 40)
        signal_quality = random.uniform(0.85, 1.0)
    elif weather == "Cloudy":
        humidity = random.uniform(40, 80)
        temperature = random.uniform(10, 35)
        signal_quality = random.uniform(0.8, 0.95)
    else:
        humidity = random.uniform(25, 65)
        temperature = random.uniform(10, 35)
        signal_quality = random.uniform(0.8, 0.95)
    
    has_defect = random.random() < DEFECT_PROBABILITY
    if has_defect:
        defect_type = random.choice([d for d in DEFECT_TYPES if d != "None"])
        if random.random() < HIGH_SEVERITY_PROBABILITY:
            defect_severity = round(random.uniform(0.7, 1.0), 3)
        else:
            defect_severity = round(random.uniform(0.1, 0.6), 3)
    else:
        defect_type = "None"
        defect_severity = 0.0
    
    sampling_rate = random.choice([5000, 10000, 15000, 20000])
    n_samples = 1000
    
    vibration_stats = generate_vibration_stats(n_samples, speed_kmh, defect_type, defect_severity, temperature)
    
    record = {
        "record_id": str(uuid.uuid4()),
        "timestamp": (start_time + timedelta(seconds=record_num * 0.5)).isoformat() + "Z",
        "sensor_id": f"SENS_{random.randint(1000, 9999)}",
        "sensor_location": random.choice(SENSOR_LOCATIONS),
        "train_id": f"TR_{random.randint(100, 999)}",
        "train_type": train_type,
        "direction": random.choice(DIRECTIONS),
        "speed_kmh": speed_kmh,
        "acceleration_x_mean": vibration_stats["acceleration_x_mean"],
        "acceleration_y_mean": vibration_stats["acceleration_y_mean"],
        "acceleration_z_mean": vibration_stats["acceleration_z_mean"],
        "acceleration_x_std": vibration_stats["acceleration_x_std"],
        "acceleration_y_std": vibration_stats["acceleration_y_std"],
        "acceleration_z_std": vibration_stats["acceleration_z_std"],
        "acceleration_x_max": vibration_stats["acceleration_x_max"],
        "acceleration_y_max": vibration_stats["acceleration_y_max"],
        "acceleration_z_max": vibration_stats["acceleration_z_max"],
        "acceleration_x_rms": vibration_stats["acceleration_x_rms"],
        "acceleration_y_rms": vibration_stats["acceleration_y_rms"],
        "acceleration_z_rms": vibration_stats["acceleration_z_rms"],
        "vibration_kurtosis": vibration_stats["vibration_kurtosis"],
        "vibration_skewness": vibration_stats["vibration_skewness"],
        "peak_to_rms_ratio": vibration_stats["peak_to_rms_ratio"],
        "sampling_rate_hz": sampling_rate,
        "temperature_c": round(temperature, 1),
        "humidity_pct": round(humidity, 1),
        "wheel_defect_flag": has_defect,
        "defect_type": defect_type,
        "defect_severity": defect_severity,
        "track_section_id": f"SEC_{random.randint(1, 50):03d}",
        "weather_condition": weather,
        "signal_quality": round(signal_quality, 3)
    }
    
    return record

def main():
    n_records = 5000
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    
    print(f"Generating {n_records} synthetic railway vibration records...")
    
    records = []
    for i in range(n_records):
        record = generate_record(i, start_time)
        records.append(record)
        
        if (i + 1) % 1000 == 0:
            print(f"  Generated {i + 1}/{n_records} records...")
    
    df = pd.DataFrame(records)
    
    output_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(output_dir, "trackside_vibration_raw.csv")
    json_path = os.path.join(output_dir, "trackside_vibration_raw.json")
    
    df.to_csv(csv_path, index=False)
    print(f"CSV saved to: {csv_path}")
    
    df.to_json(json_path, orient="records", indent=2)
    print(f"JSON saved to: {json_path}")
    
    print("\n=== Data Summary ===")
    print(f"Total records: {len(df)}")
    print(f"\nTrain type distribution:")
    print(df["train_type"].value_counts())
    print(f"\nDefect type distribution:")
    print(df["defect_type"].value_counts())
    print(f"\nWeather distribution:")
    print(df["weather_condition"].value_counts())
    print(f"\nSpeed statistics:")
    print(df["speed_kmh"].describe())
    print(f"\nDefect severity statistics (defective only):")
    defective_df = df[df["wheel_defect_flag"] == True]
    print(defective_df["defect_severity"].describe())
    
    return df

if __name__ == "__main__":
    main()
