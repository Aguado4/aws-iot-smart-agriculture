import os
from pathlib import Path
from dotenv import load_dotenv
import time
import json
import random
from datetime import datetime
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import logging

# ===== Logging setup =====
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# ===== Base directory & environment =====
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

# ===== Configuration =====
ENDPOINT = os.getenv("AWS_IOT_ENDPOINT")
CLIENT_ID = os.getenv("AWS_IOT_CLIENT_ID")
ROOT_CA = BASE_DIR / os.getenv("AWS_IOT_ROOT_CA")
PRIVATE_KEY = BASE_DIR / os.getenv("AWS_IOT_PRIVATE_KEY")
CERTIFICATE = BASE_DIR / os.getenv("AWS_IOT_CERTIFICATE")
FARM_ID = os.getenv("FARM_ID")
ZONE_ID = os.getenv("ZONE_ID")

# ===== Validate configuration =====
assert ENDPOINT, "AWS_IOT_ENDPOINT not set in .env"
assert CLIENT_ID, "AWS_IOT_CLIENT_ID not set in .env"
assert ROOT_CA.exists(), f"Root CA certificate not found: {ROOT_CA}"
assert PRIVATE_KEY.exists(), f"Private key not found: {PRIVATE_KEY}"
assert CERTIFICATE.exists(), f"Certificate not found: {CERTIFICATE}"
assert FARM_ID, "FARM_ID not set in .env"
assert ZONE_ID, "ZONE_ID not set in .env"

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
    {"id": "SM01",  "type": "soil_moisture",    "unit": "%",   "generate": generate_soil_moisture},
    {"id": "NPK01","type": "soil_npk",         "unit": "NPK","generate": generate_soil_npk},
    {"id": "WS01",  "type": "weather_station",  "unit": "env","generate": generate_weather},
]

# ===== MQTT Client Setup =====
logging.getLogger().setLevel(logging.DEBUG)
client = AWSIoTMQTTClient(CLIENT_ID)
client.configureEndpoint(ENDPOINT, 8883)
client.configureCredentials(str(ROOT_CA), str(PRIVATE_KEY), str(CERTIFICATE))
client.configureOfflinePublishQueueing(-1)  # infinite offline queue
client.configureDrainingFrequency(2)          # 0.5 Hz draining
client.configureConnectDisconnectTimeout(30)  # 30 sec
client.configureMQTTOperationTimeout(5)       # 5 sec

# ===== Connect =====
try:
    logging.info("Connecting to AWS IoT Core at %s...", ENDPOINT)
    client.connect()
    logging.info("Connected to AWS IoT Core")
except Exception as e:
    logging.error("Failed to connect: %s", e)
    exit(1)

# ===== Publish Loop =====
try:
    while True:
        for sensor in sensors:
            topic = f"smartagri/{FARM_ID}/{ZONE_ID}/sensor/{sensor['type']}/{sensor['id']}/telemetry"
            value = sensor['generate']()
            payload = {
                "sensor":    sensor['id'],
                "type":      sensor['type'],
                "value":     value,
                "unit":      sensor['unit'],
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
            message = json.dumps(payload)
            client.publish(topic, message, 1)
            logging.info("Published to %s: %s", topic, message)
        time.sleep(5)
except KeyboardInterrupt:
    logging.info("Simulation stopped by user")
    client.disconnect()