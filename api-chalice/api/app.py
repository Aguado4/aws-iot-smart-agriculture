import os
import json
import logging
import psycopg2
from chalice import Chalice, BadRequestError
from pathlib import Path
from dotenv import load_dotenv

# — Logging —
logging.basicConfig(level=logging.INFO)

# — Load env —
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR.parent / '.env')   # sube un nivel si tu .env está en api-chalice

# — DB connection factory —
def get_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS')
    )

app = Chalice(app_name='smartagri-api')

@app.route('/sensors', methods=['GET'])
def list_sensors():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id, type, farm_id, zone_id, created_at FROM sensors")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{'id':r[0],'type':r[1],'farm_id':r[2],'zone_id':r[3],'created_at':r[4].isoformat()} for r in rows]

@app.route('/sensors', methods=['POST'])
def create_sensor():
    body = app.current_request.json_body
    for k in ('id','type','farm_id','zone_id'): 
        if k not in body: raise BadRequestError(f"Missing {k}")
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO sensors (id,type,farm_id,zone_id) VALUES (%s,%s,%s,%s)",
        (body['id'],body['type'],body['farm_id'],body['zone_id'])
    )
    conn.commit(); cur.close(); conn.close()
    return {'status':'created','sensor':body}

@app.route('/sensors/{sensor_id}/events', methods=['GET'])
def get_events(sensor_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "SELECT value, unit, timestamp FROM sensor_events WHERE sensor_id=%s ORDER BY timestamp DESC",
        (sensor_id,)
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{'value':json.loads(r[0]) if isinstance(r[0],str) else r[0],
             'unit':r[1],'timestamp':r[2].isoformat()} for r in rows]

@app.route('/actuators', methods=['GET'])
def list_actuators():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id, type, farm_id, zone_id, created_at FROM actuators")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{'id':r[0],'type':r[1],'farm_id':r[2],'zone_id':r[3],'created_at':r[4].isoformat()} for r in rows]

@app.route('/actuators', methods=['POST'])
def create_actuator():
    body = app.current_request.json_body
    for k in ('id','type','farm_id','zone_id'): 
        if k not in body: raise BadRequestError(f"Missing {k}")
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO actuators (id,type,farm_id,zone_id) VALUES (%s,%s,%s,%s)",
        (body['id'],body['type'],body['farm_id'],body['zone_id'])
    )
    conn.commit(); cur.close(); conn.close()
    return {'status':'created','actuator':body}
