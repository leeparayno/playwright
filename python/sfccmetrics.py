import os
import wmill

import time
from datetime import datetime, timedelta, UTC
import requests
from datetime import datetime, timedelta
from pyotp import TOTP
import asyncio
from influxdb import InfluxDBClient
from u.admin.drtlibraryv3 import get_secret
from playwright.sync_api import sync_playwright
import boto3
import json
from botocore.exceptions import ClientError
from contextlib import contextmanager

sfcc_user_key = f"u/admin/sfcc/sfcc_user"
sfcc_pw_key = f"u/admin/sfcc/sfcc_pw"

print("sfcc_user_key: " + sfcc_user_key)
print("sfcc_pw_key: " + sfcc_pw_key)

# Environment variables
SFCC_USER = get_secret(sfcc_user_key)
SFCC_PW = get_secret(sfcc_pw_key)

sfcc_totp_token_key = f"u/admin/sfcc/sfcc_totp_token"
sfcc_tenant_key = f"u/admin/sfcc/sfcc_us_tenant"
sfcc_storename_key = f"u/admin/sfcc/sfcc_us_storename"
sfcc_pollmetrics = f"u/admin/sfcc/poll_metrics"
sfcc_pollinterval = f"u/admin/sfcc/poll_interval"

influxdb_addr_key = f"u/admin/influxdb/influxdb_addr"
influxdb_db = f"u/admin/influxdb/influxdb_db"

TOTP_TOKEN = get_secret(sfcc_totp_token_key)
SFCC_TENANT = get_secret(sfcc_tenant_key)
SFCC_STORENAME = get_secret(sfcc_storename_key)
POLL_METRICS = get_secret(sfcc_pollmetrics).split(',')
INFLUXDB_URL = get_secret(influxdb_addr_key)
INFLUXDB_PORT = 8086
INFLUXDB_DB = get_secret(influxdb_db)

# Add these to your existing environment variables or secrets
S3_BUCKET_NAME = get_secret("u/admin/aws/s3/bucket_name")
AWS_ACCESS_KEY_ID = "" #get_secret("u/admin/aws/access_key_id")
AWS_SECRET_ACCESS_KEY = "" #get_secret("u/admin/aws/secret_access_key")

# Backup directory
BACKUP_DIR = "/app/windmill_shared_data/backup"

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

POLL_INTERVAL = int(get_secret(sfcc_pollinterval), 30)

# Global variables
sfcc_token = None
xsrf_token = None
token_expiry = 0
influxdb_client = None

@contextmanager
def get_influxdb_client():
    global influxdb_client
    if influxdb_client is None:
        influxdb_client = InfluxDBClient(host=INFLUXDB_URL, port=INFLUXDB_PORT, database=INFLUXDB_DB)
    try:
        yield influxdb_client
    except Exception as e:
        print(f"Error with InfluxDB client: {e}")
        influxdb_client = None
        raise
    finally:
        if influxdb_client is not None:
            influxdb_client.close()
            influxdb_client = None

def save_auth_state(sfcc_token, xsrf_token, expiry):
    state = {
        "sfcc_token": sfcc_token,
        "xsrf_token": xsrf_token,
        "expiry": expiry.isoformat()
    }
    wmill.set_state(state)

def get_auth_state():
    state = wmill.get_state()
    if state:
        state['expiry'] = datetime.fromisoformat(state['expiry'])
    return state

def get_sfcc_auth():
    auth_state = get_auth_state()
    current_time = datetime.now(UTC)

    if auth_state and auth_state['expiry'] > current_time:
        print("Using cached SFCC authentication")
        return auth_state['sfcc_token'], auth_state['xsrf_token']

    print("Performing new SFCC authentication")
    sfcc_token, xsrf_token = sfcc_analytics_auth(SFCC_TENANT, SFCC_USER, SFCC_PW, TOTP_TOKEN)
    
    # Set expiry to 1 hour from now
    expiry = current_time + timedelta(hours=1)
    save_auth_state(sfcc_token, xsrf_token, expiry)

    return sfcc_token, xsrf_token

def sfcc_analytics_auth(sfcc_tenant, username, password, totp_token):
    print(f"info: starting analytics auth with user {username}")

    with sync_playwright() as p:
        print(f"Launching browser")
        # browser = p.chromium.launch(headless=True)
        browser = p.chromium.launch(headless=False,
            args=[
            "--headless=new",
            '--disable-gpu',
            '--no-sandbox',
            '--headless',
        ])
        print(f"Opening a new page")
        page = browser.new_page()

        print(f"Navigating to ccac.analytics.commercecloud.salesforce.com")
        page.goto("https://ccac.analytics.commercecloud.salesforce.com/login", wait_until="networkidle")
        time.sleep(4)
        page.fill('input[placeholder="User Name"]', username)
        page.click('#loginButton_0')
        time.sleep(4)
        page.fill('input[placeholder="Password"]', password)
        page.click('#loginButton_0')
        time.sleep(8)

        # Take a full-page screenshot
        page.screenshot(path="screenshot.png", full_page=True)
        
        print(f"Generating TOTP token")
        totp = TOTP(totp_token)
        token = totp.now()
        print(f"info: TOTP token generation complete: {token}")

        print(f"Inputting TOTP in form and pressing enter")
        page.fill('#input-9', token)
        page.press('#input-9', 'Enter')
        time.sleep(8)

        print(f"Retrieving cookies")
        cookies = page.context.cookies()
        sfcc_token = next((cookie['value'] for cookie in cookies if cookie['name'] == 'connect.sid'), "")
        xsrf_token = next((cookie['value'] for cookie in cookies if cookie['name'] == 'XSRF-TOKEN'), "")

        print(f"info: cookie received connect.sid:{sfcc_token}")
        print(f"debug: cookie received xsrf:{xsrf_token}")

        tenant_select(page, f"https://ccac.analytics.commercecloud.salesforce.com/api/v0/users/self", sfcc_tenant, sfcc_token, xsrf_token)
        browser.close()

    return sfcc_token, xsrf_token

def tenant_select(page, url, tenant, sfcc_token, xsrf_token):
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Cookie": f"connect.sid={sfcc_token}",
        "X-XSRF-TOKEN": xsrf_token
    }
    payload = {"currentTenant": tenant}

    response = page.request.put(url, headers=headers, data=payload)
    if response.ok:
        print("info: tenant select requested")
    else:
        print(f"error: tenant select failed with status {response.status}")

def query_sfcc_metric(metric, sfcc_token, xsrf_token, sfcc_tenant, sfcc_storename):
    timestamp = (datetime.utcnow() - timedelta(minutes=10)).strftime("%Y-%m-%d%%20%H:%M:%S")
    url = f"https://ccac.analytics.commercecloud.salesforce.com/api/v0/monitoring/tenants/{sfcc_tenant}/data?metrics={metric}&timezone=UTC&start={timestamp}"
    
    headers = {
        "Cookie": f"connect.sid={sfcc_token}",
        "X-XSRF-TOKEN": xsrf_token
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if data and len(data) > 0:
            latest_data = max(data, key=lambda x: x['create_date'])
            return {
                "timestamp": int(datetime.strptime(latest_data['create_date'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()),
                "type": metric,
                "store": sfcc_storename,
                "value": latest_data['metric_value']
            }
        else:
            print(f"error: no data returned for metric {metric}")   
    else:
        print(f"error: failed to query metric {metric} with status {response.status_code}")
        
    return None

def write_to_s3(data, file_name):
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(data)
        )
        print(f"Successfully wrote {file_name} to S3")
    except ClientError as e:
        print(f"Error writing to S3: {e}")

def read_from_s3(file_name):
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_name)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except ClientError as e:
        print(f"Error reading from S3: {e}")
        return None

def write_to_backup(points):
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"backup_{timestamp}.json"
    filepath = os.path.join(BACKUP_DIR, filename)

    print(f"debug: writing {len(points)} points to backup file {filepath}")

    # Convert datetime objects to strings before writing to JSON
    def convert_datetime(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    with open(filepath, 'w') as f:
        json.dump(points, f, default=convert_datetime)
    
    print(f"Backup written to {filepath}")

def read_from_backup():
    if not os.path.exists(BACKUP_DIR):
        print("Backup directory does not exist")
        return []
    
    backup_files = [f for f in os.listdir(BACKUP_DIR) if f.startswith("backup_") and f.endswith(".json")]
    
    all_points = []
    for file in backup_files:
        filepath = os.path.join(BACKUP_DIR, file)
        with open(filepath, 'r') as f:
            # Check if file has any contents
            print(f"debug: reading from backup file {filepath}")

            # Read the file contents
            file_contents = f.read()
            print(f"debug: file contents: {file_contents}")
            if not file_contents:
                print(f"debug: file {filepath} has no contents")
                continue

            points = json.loads(file_contents)

            print(f"debug: read {len(points)} points from backup file {filepath}")
            # Points loaded from backup
            print(f"debug: points from file {filepath}: {points}")

            all_points.extend(points)
        
        # Optionally, remove the file after reading
        # os.remove(filepath)
    
    return all_points

def retry_failed_points():
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix='failed_points_')
        
        if 'Contents' in response:
            for obj in response['Contents']:
                file_name = obj['Key']
                points = read_from_s3(file_name)
                
                if points:
                    print(f"Retrying to write points from {file_name}")
                    result = write_to_influxdb(points)
                    
                    if result['success']:
                        print(f"Successfully wrote points from {file_name}")
                        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=file_name)
                    else:
                        print(f"Failed to write points from {file_name}")
    except Exception as e:
        print(f"Error in retry_failed_points: {e}")

def write_to_influxdb(points):
    failed_points = []
    
    try:
        with get_influxdb_client() as client:
            if points:
                # Write points individually to track which ones fail
                for point in points:
                    try:
                        print(f"debug: writing point to influxdb: {point}")
                        # Get timing of the write process
                        start_time = time.time()
                        client.write_points([point], time_precision='s')
                        end_time = time.time()
                        print(f"debug: write time: {end_time - start_time} seconds")
                    except Exception as e:
                        print(f"Failed to write point: {point}. Error: {str(e)}")
                        failed_points.append(point)
        
        return {"success": len(failed_points) == 0, "written": len(points) - len(failed_points), "failed": failed_points}
    except Exception as e:
        error_message = f"Unexpected error in write_to_influxdb: {str(e)}"
        print(error_message)
        return {"success": False, "error": error_message, "failed": points}

def main():
    sfcc_user_key = "u/admin/sfcc/sfcc_user"
    sfcc_pw_key = "u/admin/sfcc/sfcc_pw"
    sfcc_totp_token_key = "u/admin/sfcc/sfcc_totp_token"
    sfcc_tenant_key = "u/admin/sfcc/sfcc_us_tenant"
    sfcc_storename_key = "u/admin/sfcc/sfcc_us_storename"
    sfcc_pollmetrics = "u/admin/sfcc/poll_metrics"
    sfcc_pollinterval = "u/admin/sfcc/poll_interval"
    sfcc_clear_backup = "u/admin/sfcc/clear_backup"

    SFCC_USER = get_secret(sfcc_user_key)
    SFCC_PW = get_secret(sfcc_pw_key)
    TOTP_TOKEN = get_secret(sfcc_totp_token_key)
    SFCC_TENANT = get_secret(sfcc_tenant_key)
    SFCC_STORENAME = get_secret(sfcc_storename_key)
    POLL_METRICS = get_secret(sfcc_pollmetrics).split(',')
    POLL_INTERVAL = int(get_secret(sfcc_pollinterval))
    CLEAR_BACKUP = get_secret(sfcc_clear_backup)
    # Add this at the beginning of the main function
    # Disabled until storage issue resolved
    # retry_failed_points()

    # Check if backup directory exists, if not create it
    if not os.path.exists(BACKUP_DIR):
        print("Creating backup directory")
        os.makedirs(BACKUP_DIR)
        # listing the contents of the directory
        print(os.listdir(BACKUP_DIR))
    else:
        print("Backup directory already exists")
        # listing the contents of the directory
        print(os.listdir(BACKUP_DIR))

    if CLEAR_BACKUP == "true":      
        # Clear the backup directory
        print(f"debug: clearing backup directory")
        for file in os.listdir(BACKUP_DIR):
            os.remove(os.path.join(BACKUP_DIR, file))

    # Try to process any backed up points
    print("debug: Checking backup directory")
    backed_up_points = read_from_backup()
    if backed_up_points:
        print(f"Processing {len(backed_up_points)} backed up points")
        write_result = write_to_influxdb(backed_up_points)
        if not write_result['success']:
            print(f"Failed to write backed up points to InfluxDB: {write_result['error']}")
        else:
            print(f"Successfully wrote {len(backed_up_points)} backed up points to InfluxDB")
            # Clear the backup directory
            for file in os.listdir(BACKUP_DIR):
                os.remove(os.path.join(BACKUP_DIR, file))

    while True:
        sfcc_token, xsrf_token = get_sfcc_auth()

        current_failed_points = []
        
        # Batch write all points for this iteration
        all_points = []
        for metric in POLL_METRICS:
            print(f"Querying metric: {metric}")
            result = query_sfcc_metric(metric, sfcc_token, xsrf_token, SFCC_TENANT, SFCC_STORENAME)
            if result:
                print(f"Metric result: {result}")
                influx_point = {
                    "measurement": "sfccmetrics",
                    "tags": {
                        "store": result['store'],
                        "type": result['type']
                    },
                    "time": result['timestamp'],
                    "fields": {
                        "value": float(result['value'])
                    }
                }
                all_points.append(influx_point)

        if all_points:
            print(f"Writing {len(all_points)} points to InfluxDB")
            write_result = write_to_influxdb(all_points)
            if not write_result['success']:
                print(f"Failed to write some points to InfluxDB: {write_result.get('error', 'Unknown error')}")
                current_failed_points.extend(write_result['failed'])

        # Backup only the points that failed in this iteration
        if current_failed_points:
            print(f"Backing up {len(current_failed_points)} failed points")
            write_to_backup(current_failed_points)

        # Try to process any backed up points
        backed_up_points = read_from_backup()
        if backed_up_points:
            print(f"Processing {len(backed_up_points)} backed up points")
            write_result = write_to_influxdb(backed_up_points)
            if not write_result['success']:
                print(f"Failed to write some backed up points to InfluxDB")
                # Write only the failed points back to backup
                write_to_backup(write_result['failed'])
            else:
                print(f"Successfully wrote all backed up points to InfluxDB")
                # Clear the backup files since all points were written successfully
                for file in os.listdir(BACKUP_DIR):
                    os.remove(os.path.join(BACKUP_DIR, file))

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()