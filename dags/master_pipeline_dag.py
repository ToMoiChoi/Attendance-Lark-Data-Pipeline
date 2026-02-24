import os
import sys
import pandas as pd
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from dotenv import load_dotenv

import requests

# Add the dags folder to sys.path so we can import our modules
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
if DAGS_FOLDER not in sys.path:
    sys.path.append(DAGS_FOLDER)

from databricks_extract import DatabricksExtractor
from bigquery_upload import BigQueryUploader

# Load environment variables
dotenv_path = os.path.join(DAGS_FOLDER, '..', '.env')
load_dotenv(dotenv_path)

LARK_APP_ID = os.getenv('LARK_APP_ID')
LARK_APP_SECRET = os.getenv('LARK_APP_SECRET')
LARK_DOMAIN = os.getenv('LARK_DOMAIN', 'open.larksuite.com')
ATTENDANCE_DAYS = int(os.getenv('ATTENDANCE_DAYS', '30'))

DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
DATABRICKS_HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH')
DATABRICKS_CATALOG = os.getenv('DATABRICKS_CATALOG', 'main')
DATABRICKS_SCHEMA = os.getenv('DATABRICKS_SCHEMA', 'default')

GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
BIGQUERY_PROJECT_ID = os.getenv('BIGQUERY_PROJECT_ID')
BIGQUERY_CREDENTIALS_PATH = os.getenv('BIGQUERY_CREDENTIALS_PATH')

logger = logging.getLogger(__name__)

# =============================================
# Helper Functions (Lark to Local CSV)
# =============================================
def get_tenant_access_token():
    domains_to_try = [LARK_DOMAIN, 'open.feishu.cn' if LARK_DOMAIN == 'open.larksuite.com' else 'open.larksuite.com']
    for domain in domains_to_try:
        url = f'https://{domain}/open-apis/auth/v3/tenant_access_token/internal'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        payload = {'app_id': LARK_APP_ID, 'app_secret': LARK_APP_SECRET}
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get('code') == 0:
                logger.info(f'✅ Connected to {domain} and got tenant_access_token!')
                return data['tenant_access_token'], domain
        except Exception as e:
            logger.warning(f'Failed for {domain}: {e}')
    raise Exception('Could not connect to Lark API.')

def get_all_departments(token, domain):
    all_dept_ids = ['0']
    queue = ['0']
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json; charset=utf-8'}
    while queue:
        parent_id = queue.pop(0)
        page_token = None
        while True:
            url = f'https://{domain}/open-apis/contact/v3/departments/{parent_id}/children'
            params = {'page_size': 50, 'department_id_type': 'open_department_id', 'fetch_child': 'false'}
            if page_token: params['page_token'] = page_token
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            if resp.status_code != 200: break
            data = resp.json()
            if data.get('code') != 0: break
            items = data.get('data', {}).get('items', [])
            for dept in items:
                dept_id = dept.get('open_department_id', '')
                if dept_id and dept_id not in all_dept_ids:
                    all_dept_ids.append(dept_id)
                    queue.append(dept_id)
            if data.get('data', {}).get('has_more', False):
                page_token = data.get('data', {}).get('page_token')
            else: break
    return all_dept_ids

def get_users_from_department(token, domain, department_id):
    users = []
    page_token = None
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json; charset=utf-8'}
    while True:
        url = f'https://{domain}/open-apis/contact/v3/users/find_by_department'
        params = {'department_id': department_id, 'page_size': 50, 'user_id_type': 'user_id'}
        if page_token: params['page_token'] = page_token
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        if resp.status_code != 200 or resp.json().get('code') != 0: return None
        data = resp.json()
        items = data.get('data', {}).get('items', [])
        for user in items:
            users.append({'user_id': user.get('user_id'), 'name': user.get('name', ''), 'employee_no': user.get('employee_no', '')})
        if data.get('data', {}).get('has_more', False):
            page_token = data.get('data', {}).get('page_token')
        else: break
    return users

def get_employee_list(token, domain):
    test_users = get_users_from_department(token, domain, '0')
    if test_users is None:
        raise Exception("Contact API no permission.")
    dept_ids = get_all_departments(token, domain)
    all_users = []
    user_ids_set = set()
    for dept_id in dept_ids:
        users = get_users_from_department(token, domain, dept_id)
        if users is None: continue
        for user in users:
            uid = user.get('user_id')
            if uid and uid not in user_ids_set:
                user_ids_set.add(uid)
                all_users.append(user)
    return all_users

def get_attendance_data(token, domain, user_ids):
    all_records = []
    today = datetime.now()
    check_date_from = int((today - timedelta(days=ATTENDANCE_DAYS)).strftime('%Y%m%d'))
    check_date_to = int(today.strftime('%Y%m%d'))
    batch_size = 50
    for i in range(0, len(user_ids), batch_size):
        batch_users = user_ids[i:i + batch_size]
        url = f'https://{domain}/open-apis/attendance/v1/user_tasks/query'
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json; charset=utf-8'}
        params = {'employee_type': 'employee_id', 'ignore_invalid_users': 'true'}
        payload = {'user_ids': batch_users, 'check_date_from': check_date_from, 'check_date_to': check_date_to}
        response = requests.post(url, headers=headers, params=params, json=payload, timeout=30)
        data = response.json()
        if data.get('code') != 0: continue
        items = data.get('data', {}).get('user_task_results', [])
        all_records.extend(items)
    return all_records

def get_user_approvals(token, domain, user_ids):
    all_approvals = []
    today = datetime.now()
    check_date_from = int((today - timedelta(days=ATTENDANCE_DAYS)).strftime('%Y%m%d'))
    check_date_to = int(today.strftime('%Y%m%d'))
    batch_size = 50
    for i in range(0, len(user_ids), batch_size):
        batch_users = user_ids[i:i + batch_size]
        url = f'https://{domain}/open-apis/attendance/v1/user_approvals/query'
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json; charset=utf-8'}
        params = {'employee_type': 'employee_id'}
        payload = {'user_ids': batch_users, 'check_date_from': check_date_from, 'check_date_to': check_date_to}
        try:
            response = requests.post(url, headers=headers, params=params, json=payload, timeout=30)
            data = response.json()
            if data.get('code') != 0: continue
            items = data.get('data', {}).get('user_approvals', [])
            all_approvals.extend(items)
        except: continue
    return all_approvals

def transform_employee_data(employees):
    return pd.DataFrame([{'user_id': e.get('user_id', ''), 'name': e.get('name', ''), 'employee_no': e.get('employee_no', '')} for e in employees])

def transform_attendance_data(records):
    data = []
    for record in records:
        user_id = record.get('user_id')
        employee_name = record.get('employee_name')
        day_str = str(record.get('day')) if record.get('day') else None
        results = record.get('records', []) or []
        if not results:
            data.append({
                'user_id': user_id, 'employee_name': employee_name, 'day': day_str,
                'group_id': record.get('group_id'), 'group_name': record.get('group_name'),
                'shift_id': record.get('shift_id'), 'shift_name': record.get('shift_name'),
                'check_in_record_id': None, 'check_in_time': record.get('check_in_record', {}).get('check_time') if record.get('check_in_record') else None,
                'check_in_result': record.get('check_in_result'),
                'check_out_record_id': None, 'check_out_time': record.get('check_out_record', {}).get('check_time') if record.get('check_out_record') else None,
                'check_out_result': record.get('check_out_result'),
            })
        else:
            for sub in results:
                data.append({
                    'user_id': user_id, 'employee_name': employee_name, 'day': day_str,
                    'group_id': record.get('group_id'), 'group_name': record.get('group_name'),
                    'shift_id': sub.get('shift_id'), 'shift_name': sub.get('shift_name'),
                    'check_in_record_id': sub.get('check_in_record', {}).get('record_id'),
                    'check_in_time': sub.get('check_in_record', {}).get('check_time'),
                    'check_in_result': sub.get('check_in_result'),
                    'check_out_record_id': sub.get('check_out_record', {}).get('record_id'),
                    'check_out_time': sub.get('check_out_record', {}).get('check_time'),
                    'check_out_result': sub.get('check_out_result'),
                })
    return pd.DataFrame(data)

def transform_approval_data(approvals):
    data = []
    for app in approvals:
        user_id = app.get('user_id', '')
        for cat, name in [('leaves', 'Nghỉ phép'), ('overtime_works', 'Tăng ca'), ('trips', 'Công tác'), ('outs', 'Làm ngoài')]:
            for item in (app.get(cat, []) or []):
                unit_str = 'ngày' if str(item.get('unit', '')) == '1' else 'giờ'
                data.append({
                    'user_id': user_id, 'approval_type': name,
                    'start_time': str(item.get('start_time', '')) or None, 'end_time': str(item.get('end_time', '')) or None,
                    'unit': unit_str, 'duration': str(item.get('duration', '')) or None, 'reason': item.get('reason', '') or None
                })
    return pd.DataFrame(data)

# =============================================
# Airflow Tasks (Medallion Architecture)
# =============================================
def task_fetch_and_transform_to_csv(**kwargs):
    """Fetch from Lark API and save to local CSV files"""
    logger.info("Task 1: Extract from Lark and save to Local CSV")
    token, domain = get_tenant_access_token()
    
    employees = get_employee_list(token, domain)
    if not employees: raise ValueError("No employees found.")
    
    user_ids = [e['user_id'] for e in employees if e['user_id']]
    records = get_attendance_data(token, domain, user_ids)
    approvals = get_user_approvals(token, domain, user_ids)
    
    df_emp = transform_employee_data(employees)
    df_att = transform_attendance_data(records)
    df_app = transform_approval_data(approvals)

    date_str = datetime.now().strftime('%Y%m%d')
    # Save to local CSV
    os.makedirs('/tmp/lark_data', exist_ok=True)
    df_emp.to_csv(f'/tmp/lark_data/lark_employees_{date_str}.csv', index=False)
    df_att.to_csv(f'/tmp/lark_data/lark_attendance_{date_str}.csv', index=False)
    df_app.to_csv(f'/tmp/lark_data/lark_approvals_{date_str}.csv', index=False)
    
    logger.info("Local CSVs generated in /tmp/lark_data/")


def task_upload_to_gcs_bronze(**kwargs):
    """Upload Local CSVs to GCS Bucket (Bronze Layer)"""
    logger.info("Task 2: Upload CSVs to GCS (Bronze Layer)")
    if not GCS_BUCKET_NAME:
        raise ValueError("GCS_BUCKET_NAME is not set in environment variables.")

    # We assume 'google_cloud_default' connection is set up in Airflow 
    # OR it uses BIGQUERY_CREDENTIALS_PATH / GOOGLE_APPLICATION_CREDENTIALS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BIGQUERY_CREDENTIALS_PATH
    gcs_hook = GCSHook()

    date_str = datetime.now().strftime('%Y%m%d')
    files = [f'lark_employees_{date_str}.csv', f'lark_attendance_{date_str}.csv', f'lark_approvals_{date_str}.csv']
    
    for filename in files:
        local_path = f'/tmp/lark_data/{filename}'
        # e.g., raw/20231027/lark_employees.csv
        gcs_path = f"raw/{date_str}/{filename}"
        
        logger.info(f"Uploading {local_path} to gs://{GCS_BUCKET_NAME}/{gcs_path}")
        gcs_hook.upload(
            bucket_name=GCS_BUCKET_NAME,
            object_name=gcs_path,
            filename=local_path
        )


def task_databricks_load_silver(**kwargs):
    """Load CSV from GCS into Databricks Delta Tables (Silver Layer)"""
    logger.info("Task 3: Load Data to Databricks (Silver Layer)")
    from databricks import sql as databricks_sql
    
    schema = f'{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}'
    host = DATABRICKS_HOST.replace('https://', '').replace('http://', '')
    date_str = datetime.now().strftime('%Y%m%d')
    
    with databricks_sql.connect(
        server_hostname=host,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            
            tables = [
                ('silver_lark_employees', f'lark_employees_{date_str}.csv', "user_id STRING, name STRING, employee_no STRING"),
                ('silver_lark_attendance', f'lark_attendance_{date_str}.csv', "user_id STRING, employee_name STRING, day STRING, group_id STRING, group_name STRING, shift_id STRING, shift_name STRING, check_in_record_id STRING, check_in_time STRING, check_in_result STRING, check_out_record_id STRING, check_out_time STRING, check_out_result STRING"),
                ('silver_lark_approvals', f'lark_approvals_{date_str}.csv', "user_id STRING, approval_type STRING, start_time STRING, end_time STRING, unit STRING, duration STRING, reason STRING")
            ]
            
            # Using COPY INTO to load data from GCS into Delta tables
            for table_name, filename, table_schema in tables:
                full_table_name = f"{schema}.{table_name}"
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {full_table_name} ({table_schema}) USING DELTA")
                
                # Requires Databricks to have access to GCS (e.g., IAM role or Storage Credential)
                gcs_uri = f"gs://{GCS_BUCKET_NAME}/raw/{date_str}/{filename}"
                logger.info(f"Running COPY INTO {full_table_name} from {gcs_uri}")
                
                # The credentials can be passed directly or assumed to be configured on the Databricks cluster/SQL warehouse
                # Note: If no Storage Credential is set up in Unity Catalog, this might require explicit credentials.
                # Assuming Unity Catalog Storage Credential is used as per Enterprise standards.
                try:
                    cursor.execute(f"""
                        COPY INTO {full_table_name}
                        FROM '{gcs_uri}'
                        FILEFORMAT = CSV
                        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
                        COPY_OPTIONS ('mergeSchema' = 'true')
                    """)
                except Exception as e:
                    logger.error(f"Failed to copy into {full_table_name}. Did you set up Databricks GCS access? Error: {e}")
                    raise


def task_databricks_transform_gold(**kwargs):
    """Aggregate Data in Databricks (Gold Layer)"""
    logger.info("Task 4: Aggregations in Databricks (Gold Layer)")
    from databricks import sql as databricks_sql
    
    schema = f'{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}'
    host = DATABRICKS_HOST.replace('https://', '').replace('http://', '')
    
    with databricks_sql.connect(
        server_hostname=host,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as connection:
        with connection.cursor() as cursor:
            # Create a Gold table summarizing attendance per user
            gold_table_name = f"{schema}.gold_lark_attendance_summary"
            
            cursor.execute(f"DROP TABLE IF EXISTS {gold_table_name}")
            cursor.execute(f"""
                CREATE TABLE {gold_table_name} USING DELTA AS
                SELECT 
                    e.user_id,
                    e.name,
                    e.employee_no,
                    COUNT(a.day) as total_days_worked
                FROM {schema}.silver_lark_employees e
                LEFT JOIN {schema}.silver_lark_attendance a ON e.user_id = a.user_id
                GROUP BY e.user_id, e.name, e.employee_no
            """)
            logger.info(f"Created Gold Table: {gold_table_name}")


def task_databricks_to_bigquery(**kwargs):
    """Extract from Databricks Gold/Silver and upload to BigQuery with YYYYMMDD suffix"""
    logger.info("Task 5: Extract from Databricks Gold and Load to BigQuery")
    extractor = DatabricksExtractor()
    uploader = BigQueryUploader()
    
    date_str = datetime.now().strftime('%Y%m%d')
    tables_to_sync = ['silver_lark_employees', 'silver_lark_attendance', 'silver_lark_approvals', 'gold_lark_attendance_summary']
    
    for table in tables_to_sync:
        bq_table_name = f"{table}_{date_str}"
        logger.info(f"Syncing Databricks table {table} to BigQuery table {bq_table_name}...")
        try:
            df = extractor.extract_table(table)
            if df is not None and not df.empty:
                uploader.upload_dataframe(df, bq_table_name, mode="WRITE_TRUNCATE")
            else:
                logger.info(f"Databricks table {table} is empty. Skipping upload to BigQuery.")
        except Exception as e:
            logger.error(f"Error syncing table {table} to {bq_table_name}: {e}")
            raise

# =============================================
# DAG Definition
# =============================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'master_lark_databricks_bigquery_pipeline',
    default_args=default_args,
    description='Medallion Architecture: Lark -> GCS(Bronze) -> Databricks(Silver/Gold) -> BigQuery',
    schedule_interval='30 22 * * *', # 22h30 UTC = 05:30 UTC+7 daily
    catchup=False,
)

t1 = PythonOperator(
    task_id='fetch_and_transform_to_csv',
    python_callable=task_fetch_and_transform_to_csv,
    dag=dag,
)

t2 = PythonOperator(
    task_id='upload_to_gcs_bronze',
    python_callable=task_upload_to_gcs_bronze,
    dag=dag,
)

t3 = PythonOperator(
    task_id='databricks_load_silver',
    python_callable=task_databricks_load_silver,
    dag=dag,
)

t4 = PythonOperator(
    task_id='databricks_transform_gold',
    python_callable=task_databricks_transform_gold,
    dag=dag,
)

t5 = PythonOperator(
    task_id='databricks_to_bigquery',
    python_callable=task_databricks_to_bigquery,
    dag=dag,
)

# Pipeline Definition
t1 >> t2 >> t3 >> t4 >> t5
