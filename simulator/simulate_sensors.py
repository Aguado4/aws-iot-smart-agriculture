import os
from dotenv import load_dotenv
import time
import json
import random
from datetime import datetime
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import logging
logging.basicConfig(level=logging.DEBUG)

# ===== Load environment variables =====
load_dotenv()

ENDPOINT = os.getenv("AWS_IOT_ENDPOINT")  # e.g. "abcd1234-ats.iot.us-east-1.amazonaws.com"
CLIENT_ID = os.getenv("AWS_IOT_CLIENT_ID")  # e.g. "sensor-simulator-client"
ROOT_CA = os.getenv("AWS_IOT_ROOT_CA")      # e.g. "./certs/AmazonRootCA1.pem"
PRIVATE_KEY = os.getenv("AWS_IOT_PRIVATE_KEY")  # e.g. "./certs/private.pem.key"
CERTIFICATE = os.getenv("AWS_IOT_CERTIFICATE")  # e.g. "./certs/certificate.pem.crt"
FARM_ID = os.getenv("FARM_ID")
ZONE_ID = os.getenv("ZONE_ID")

# ===== Sensor data generators =====

def generate_soil_moisture():
    return round(random.uniform(10.0, 50.0), 2)

def generate_soil_npk():
    return {
        "N": round(random.uniform(1.0, 5.0), 2),
        "P": round(random.uniform(1.0, 5.0), 2),
        "K": round(random.uniform(1.0, 5.0), 2)
    }

def generate_weather():
    return {
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2)
    }

sensors = [
    {"id": "SM01", "type": "soil_moisture", "unit": "%", "generate": generate_soil_moisture},
    {"id": "NPK01", "type": "soil_npk", "unit": "NPK", "generate": generate_soil_npk},
    {"id": "WS01", "type": "weather_station", "unit": "env", "generate": generate_weather},
]

# ===== MQTT Client Setup =====
client = AWSIoTMQTTClient(CLIENT_ID)
client.configureEndpoint(ENDPOINT, 8883)
client.configureCredentials(ROOT_CA, PRIVATE_KEY, CERTIFICATE)
client.configureOfflinePublishQueueing(-1)  # infinite offline queue
client.configureDrainingFrequency(2)  # Draining: 2 Hz
client.configureConnectDisconnectTimeout(10)  # 10 sec
client.configureMQTTOperationTimeout(5)  # 5 sec
client.configureConnectDisconnectTimeout(30)  # extiende timeout

# Connect to AWS IoT
client.connect()
print("Connected to AWS IoT Core")

# ===== Publish Loop =====
try:
    while True:
        for sensor in sensors:
            topic = f"smartagri/{FARM_ID}/{ZONE_ID}/sensor/{sensor['type']}/{sensor['id']}/telemetry"
            value = sensor['generate']()
            payload = {
                "sensor": sensor['id'],
                "type": sensor['type'],
                "value": value,
                "unit": sensor['unit'],
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
            message = json.dumps(payload)
            client.publish(topic, message, 1)  # QoS 1 = AT_LEAST_ONCE
            print(f"Published to {topic}: {message}")
        time.sleep(5)

except KeyboardInterrupt:
    print("Simulation stopped by user")
    client.disconnect()
