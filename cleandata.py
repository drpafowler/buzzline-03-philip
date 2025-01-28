
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import csv
import matplotlib
matplotlib.use('Qt5Agg')  # Use the Qt5Agg backend

# Loaded variable 'df' from URI: /home/philip/nwmissouri/buzzline-03-philip/data/hourly_data.csv
import pandas as pd
df = pd.read_csv(r'/home/philip/nwmissouri/buzzline-03-philip/data/hourly_data.csv')

# Drop columns: 'STATION', 'REPORT_TYPE' and 16 other columns
df = df.drop(columns=['STATION', 'REPORT_TYPE', 'SOURCE', 'BackupElements', 'BackupElevation', 'BackupEquipment', 'BackupLatitude', 'BackupLongitude', 'BackupName', 'HourlyAltimeterSetting', 'WindEquipmentChangeDate', 'HourlyWindSpeed', 'HourlyWindDirection', 'HourlyVisibility', 'HourlyStationPressure', 'HourlySeaLevelPressure', 'HourlyRelativeHumidity', 'HourlyPrecipitation'])

# Drop column: 'HourlyDewPointTemperature'
df = df.drop(columns=['HourlyDewPointTemperature'])

# Change the hourly dry bulb temperature column to numeric
df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce')

# Drop rows with missing values
df = df.dropna()

#save the data to a new csv file
df.to_csv(r'/home/philip/nwmissouri/buzzline-03-philip/data/hourly_data_clean.csv', index=False)