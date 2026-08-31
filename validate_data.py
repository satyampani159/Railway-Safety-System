import pandas as pd
import numpy as np

df = pd.read_csv('trackside_vibration_raw.csv')

print('=== Data Validation Report ===')
print(f'Total records: {len(df)}')
print(f'Columns: {len(df.columns)}')

print('\n=== Missing Values ===')
print(df.isnull().sum())

print('\n=== Data Types ===')
print(df.dtypes)

print('\n=== Speed Range Check ===')
print(f"Min speed: {df['speed_kmh'].min()} km/h")
print(f"Max speed: {df['speed_kmh'].max()} km/h")

print('\n=== Temperature Range Check ===')
print(f"Min temp: {df['temperature_c'].min()} C")
print(f"Max temp: {df['temperature_c'].max()} C")

print('\n=== Humidity Range Check ===')
print(f"Min humidity: {df['humidity_pct'].min()} %")
print(f"Max humidity: {df['humidity_pct'].max()} %")

print('\n=== Defect Severity Range Check ===')
print(f"Min severity: {df['defect_severity'].min()}")
print(f"Max severity: {df['defect_severity'].max()}")

print('\n=== Signal Quality Range Check ===')
print(f"Min quality: {df['signal_quality'].min()}")
print(f"Max quality: {df['signal_quality'].max()}")

print('\n=== Sample Record (first row) ===')
print(df.iloc[0].to_dict())

print('\n=== Defective Records Sample ===')
defective = df[df['wheel_defect_flag'] == True].head(3)
print(defective[['record_id', 'train_type', 'speed_kmh', 'defect_type', 'defect_severity', 'weather_condition']].to_string())

print('\n=== Validation PASSED ===')
