import socket
import base64
import sys
import serial
import mariadb
import time
from datetime import datetime

# Mapping of fix types
fix_type = {
    '0': "Invalid",
    '1': "GPS fix (SPS)",
    '2': "DGPS fix",
    '3': "PPS fix",
    '4': "RTK fix",
    '5': "Float RTK",
    '6': "Estimated (dead reckoning) (2.3 feature)",
    '7': "Manual input mode",
    '8': "Simulation mode"
}

# Serial port configuration
ser = serial.Serial('/dev/topblue', 38400, timeout=0)

# NTRIP server configuration
server, port, username, password, mountpoint = 'czeposr.cuzk.gov.cz', 2101, "cvutvyuka", "k155dremejakokone", 'CPRG3-MSM'

# Encode username and password for Basic Authentication
credentials = base64.b64encode("{}:{}".format(username, password).encode())

# Construct HTTP header for NTRIP request
header = (
    "GET /{} HTTP/1.1\r\n".format(mountpoint) +
    "Host: {}\r\n".format(server) +
    "Ntrip-Version: Ntrip/2.0\r\n" +
    "User-Agent: NTRIP pyUblox/0.0\r\n" +
    "Connection: close\r\n" +
    "Authorization: Basic {}\r\n\r\n".format(credentials.decode())
)

# Create socket and connect to NTRIP server
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((server, port))
s.send(header.encode())

# Variables to store
fix_start_time = None
fix_end_time = None
last_fix_type_value = None
endloop = 0

latitude_MDB = 0.0  
longitude_MDB = 0.0 
altitude_MDB = 0.0  
time_d_float = 0.0

# Receive response from NTRIP server
resp = s.recv(1024)


try:
    conn = mariadb.connect(
        user="otto",
        password="password1",
        host="localhost",
        database="Mereni_GNGGA"
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

databaseName = "Mereni_GNGGA"
# Get Cursor
cur = conn.cursor()


try:
    while True:
        # Read data from GPS receiver
        rover_message = ser.readline().decode('utf-8').strip()

        # Parse GGA sentence
        if 'GNGGA' in rover_message:
            data = rover_message.split(",")
            fix_type_value = data[6]
            
            # Convert latitude and longitude to degrees
            latitude = float(data[2]) // 100 + (float(data[2]) % 100) / 60
            longitude = float(data[4]) // 100 + (float(data[4]) % 100) / 60
            
            # Get MSL altitude
            altitude = float(data[9])
            
            # Get timestamp with date and seconds
            timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            
            if fix_type_value == '4':
                
                # Reassign variables for MariaDB input
                fix_type_value_MDB =  fix_type_value
                latitude_MDB =        latitude
                longitude_MDB =       longitude
                altitude_MDB =        altitude             
                
                endloop = endloop + 1
                
            # Calculate time difference for fix transition
            if fix_type_value == '4':
                fix_end_time = datetime.utcnow()
                last_fix_type_value = fix_type_value
                if fix_start_time:
                    transition_time = fix_end_time - fix_start_time
                    
                    time_d_float = transition_time.total_seconds()
                    
                fix_start_time = None
            elif not fix_start_time:
                fix_start_time = datetime.utcnow()
            
            if fix_type_value == '4' and transition_time:
                latitude_MDB = latitude_MDB     
                longitude_MDB = longitude_MDB   
                altitude_MDB = altitude_MDB     
                timestamp_dt_MDB = timestamp_dt
                time_d_float_MDB = time_d_float

                query = "INSERT INTO Mereni_GNGGA (LAT, LON, MSL_Altitude, Pos_Fix, tTime, tfix) VALUES (?, ?, ?, ?, ?, ?)"
                
                
                try:
                    cur.execute(query, (latitude_MDB, longitude_MDB, altitude_MDB, fix_type_value_MDB, timestamp_dt_MDB, time_d_float_MDB))
                    conn.commit()  # Commit the transaction
                    break
                except mariadb.Error as e:
                    print(f"Error inserting data: {e}")
            
        # Receive data from NTRIP server and forward to GPS receiver
        data = s.recv(1024)
        ser.write(data)
finally:
    # Close socket connection
    s.close()
    conn.close()  # Close the database connection
