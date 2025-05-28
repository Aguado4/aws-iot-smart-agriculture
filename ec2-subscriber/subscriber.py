import os
import json
import logging
import time
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

# — Logging setup —
logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s')

# — Load environment variables —
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / '.env')

# — AWS IoT configuration —
endpoint    = os.getenv('AWS_IOT_ENDPOINT')
client_id   = os.getenv('AWS_IOT_CLIENT_ID')
root_ca     = BASE_DIR / os.getenv('AWS_IOT_ROOT_CA')
private_key = BASE_DIR / os.getenv('AWS_IOT_PRIVATE_KEY')
certificate = BASE_DIR / os.getenv('AWS_IOT_CERTIFICATE')

# Validate certificate files
for f in (root_ca, private_key, certificate):
    if not f.exists():
        logging.error("Required file missing: %s", f)
        exit(1)

mqtt_client = AWSIoTMQTTClient(client_id)
mqtt_client.configureEndpoint(endpoint, 8883)
mqtt_client.configureCredentials(
    str(root_ca), str(private_key), str(certificate)
)
mqtt_client.configureConnectDisconnectTimeout(10)  # seconds
mqtt_client.configureMQTTOperationTimeout(5)       # seconds

# — PostgreSQL configuration —
db_conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS')
)
db_conn.autocommit = True
cursor = db_conn.cursor()

# — MQTT message callback —
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload)
        logging.info("Received on %s: %s", msg.topic, payload)
        cursor.execute(
            "INSERT INTO sensor_events (sensor_id, type, value, unit, timestamp) VALUES (%s, %s, %s, %s, %s)",
            (
                payload.get('sensor'),
                payload.get('type'),
                json.dumps(payload.get('value')),
                payload.get('unit'),
                payload.get('timestamp')
            )
        )
    except Exception as e:
        logging.error("Error processing message: %s", e)

# — Connect and subscribe —
logging.info("Connecting to AWS IoT Core at %s...", endpoint)
try:
    mqtt_client.connect()
    logging.info("Connected to AWS IoT Core")
except Exception as e:
    logging.error("Connection failed: %s", e)
    exit(1)

topic_filter = 'smartagri/+/+/sensor/+/+/telemetry'
mqtt_client.subscribe(topic_filter, 1, on_message)
logging.info("Subscribed to %s", topic_filter)

# — Keep script running —
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    logging.info("Stopping subscriber...")
    mqtt_client.disconnect()
    cursor.close()
    db_conn.close()
    logging.info("Subscriber stopped")