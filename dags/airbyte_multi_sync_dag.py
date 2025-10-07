# dags/airbyte_multi_sync_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import requests
import time

# Lista dos connection IDs do Airbyte para sincronizar
CONNECTION_IDS = [
    "3f6fca1d-a1bc-409c-b8d1-dc51744badbf",
    "6e590292-58b6-4311-acfd-6d881d9f475c",
    "fb6a211e-d8da-478c-8109-98c4a92ffd9e",
    "c1a44681-fbf1-4bd9-919f-c11a618cebc3",
    "f698c6a8-6407-432c-8860-44a7c69424ef",
    "62a3c07f-fc8b-4ec5-9c51-94ccff81cc4f",
    "2bc1dda9-8d65-43b3-a127-e3c9e5560fbd",
    "14ed4596-e329-43e4-8f72-f43f82bcaaf6",
    "a04cd22e-7fdd-49c6-be34-6eda9d11042c",
    "8335aee6-c676-4aa5-92bd-efd6cbb02d91",
    "1832918d-91b1-46b8-9dd8-b49661c2ba8c",
    "69f4c6ad-ec7f-4051-9487-c262c640aa68",
]

# Preferir ENV; se não houver, cair para Airflow Variable (mantém compatibilidade)
AIRBYTE_URL = os.environ.get("AIRBYTE_URL") or Variable.get("AIRBYTE_IP", default_var="http://host.docker.internal:8000")
AIRBYTE_API_KEY = os.environ.get("AIRBYTE_API_KEY") or Variable.get("AIRBYTE_API_KEY", default_var=None)

def _headers():
    h = {"Content-Type": "application/json"}
    if AIRBYTE_API_KEY:  # se usar Airbyte Cloud ou auth via API key
        h["Authorization"] = f"Bearer {AIRBYTE_API_KEY}"
    return h

def trigger_airbyte_sync(connection_id: str):
    sync_url = f"{AIRBYTE_URL}/api/v1/connections/sync"
    job_url  = f"{AIRBYTE_URL}/api/v1/jobs/get"

    # Inicia a sync
    resp = requests.post(sync_url, json={"connectionId": connection_id}, headers=_headers(), timeout=60)
    resp.raise_for_status()
    job_id = resp.json()["job"]["id"]

    # Polling do job
    while True:
        status_resp = requests.post(job_url, json={"id": job_id}, headers=_headers(), timeout=30)
        status_resp.raise_for_status()
        status = status_resp.json()["job"]["status"]

        if status == "succeeded":
            print(f"[OK] Conexão {connection_id} sincronizada.")
            break
        if status == "failed":
            raise RuntimeError(f"[FAIL] Airbyte sync falhou para {connection_id}.")
        print(f"[WAIT] {connection_id} status: {status} ...")
        time.sleep(10)

def run_syncs():
    for cid in CONNECTION_IDS:
        trigger_airbyte_sync(cid)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="airbyte_multi_connections_sync",
    default_args=default_args,
    description="Sincroniza múltiplas conexões Airbyte; lê AIRBYTE_URL/AIRBYTE_API_KEY (env) ou AIRBYTE_IP (Variable).",
    # Para rodar apenas manualmente, use schedule=None
    schedule=None,
    catchup=False,
) as dag:
    sync_task = PythonOperator(
        task_id="run_airbyte_syncs",
        python_callable=run_syncs,
    )
