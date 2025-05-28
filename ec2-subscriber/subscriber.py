import os
import json
import logging
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Load env
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / '.env')

# AWS IoT config
endpoint    = os.getenv('AWS_IOT_ENDPOINT')
client_id   = os.getenv('AWS_IOT_CLIENT_ID')
root_ca     = BASE_DIR / os.getenv('AWS_IOT_ROOT_CA')
private_key = BASE_DIR / os.getenv('AWS_IOT_PRIVATE_KEY')
certificate = BASE_DIR / os.getenv('AWS_IOT_CERTIFICATE')

assert root_ca.exists(), f"Root CA not found: {root_ca}"
assert private_key.exists(), f"Private key not found: {private_key}"
assert certificate.exists(), f"Cert not found: {certificate}"

mqtt_client = AWSIoTMQTTClient(client_id)
mqtt_client.configureEndpoint(endpoint, 8883)
mqtt_client.configureCredentials(str(root_ca), str(private_key), str(certificate))
mqtt_client.configureConnectDisconnectTimeout(10)
mqtt_client.configureMQTTOperationTimeout(5)

# DB config
db_conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS')
)
db_conn.autocommit = True
cursor = db_conn.cursor()

def on_message(client, userdata, msg):
    payload = json.loads(msg.payload)
    logging.info("Received on %s: %s", msg.topic, payload)
    cursor.execute(
        "INSERT INTO sensor_events (sensor_id, type, value, unit, timestamp) VALUES (%s, %s, %s, %s, %s)",
        (payload['sensor'], payload['type'], json.dumps(payload['value']), payload['unit'], payload['timestamp'])
    )

logging.info("Connecting to AWS IoT Core...")
mqtt_client.connect()
mqtt_client.subscribe('smartagri/+/+/sensor/+/+/telemetry', 1, on_message)
logging.info("Subscribed; awaiting messages...")
mqtt_client.loop_forever()
