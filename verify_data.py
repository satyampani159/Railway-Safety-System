"""Verify MySQL data shape matches Grafana panel queries."""
import mysql.connector

db = mysql.connector.connect(
    host="localhost", port=3306, user="root",
    password="root", database="railway_safety",
)
cur = db.cursor()
cur.execute("SELECT COUNT(*), SUM(wheel_defect_flag), MAX(defect_severity) FROM vibration_records")
print("total, defects, max_sev:", cur.fetchone())
cur.execute("SELECT defect_type, COUNT(*) FROM vibration_records WHERE wheel_defect_flag=1 GROUP BY defect_type")
print("by_type:", cur.fetchall())
cur.execute("SELECT train_type, COUNT(*) FROM vibration_records WHERE wheel_defect_flag=1 GROUP BY train_type")
print("by_train:", cur.fetchall())
cur.execute("SELECT weather_condition, ROUND(AVG(defect_severity),3) FROM vibration_records WHERE wheel_defect_flag=1 GROUP BY weather_condition")
print("by_weather:", cur.fetchall())
cur.execute("SELECT MIN(`timestamp`), MAX(`timestamp`) FROM vibration_records")
print("time_range:", cur.fetchone())
cur.execute("SELECT FLOOR(UNIX_TIMESTAMP(`timestamp`) / 60) * 60000 AS t, AVG(wheel_defect_flag) * 100 FROM vibration_records WHERE `timestamp` BETWEEN '2024-01-01' AND '2024-01-01 01:00' GROUP BY 1 ORDER BY 1 LIMIT 5")
print("rate_sample:", cur.fetchall())
cur.execute("SELECT weather_condition, defect_type, ROUND(AVG(defect_severity),3), COUNT(*) FROM vibration_records WHERE wheel_defect_flag=1 GROUP BY weather_condition, defect_type ORDER BY weather_condition, defect_type")
cells = cur.fetchall()
print("heatmap cells:", len(cells), "of max 30")
for row in cells:
    print("  ", row)
cur.execute("SELECT CASE WHEN speed_kmh < 40 THEN '0-40' WHEN speed_kmh < 80 THEN '40-80' WHEN speed_kmh < 120 THEN '80-120' WHEN speed_kmh < 160 THEN '120-160' ELSE '160+' END AS b, defect_type, ROUND(AVG(defect_severity),3), COUNT(*) FROM vibration_records WHERE wheel_defect_flag = 1 GROUP BY b, defect_type ORDER BY MIN(speed_kmh), defect_type")
cells2 = cur.fetchall()
print("speed-bin cells:", len(cells2), "of max 25")
for row in cells2:
    print("  ", row)
cur.execute("SELECT weather_condition, CASE WHEN speed_kmh < 40 THEN '0-40' WHEN speed_kmh < 80 THEN '40-80' WHEN speed_kmh < 120 THEN '80-120' WHEN speed_kmh < 160 THEN '120-160' ELSE '160+' END AS b, COUNT(*) FROM vibration_records WHERE wheel_defect_flag = 1 GROUP BY weather_condition, b ORDER BY weather_condition, MIN(speed_kmh)")
cells3 = cur.fetchall()
print("weather x speed cells:", len(cells3), "of max 30")
for row in cells3:
    print("  ", row)
db.close()
print("VERIFY OK")
