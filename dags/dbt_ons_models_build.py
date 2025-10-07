# infra/airflow/dags/dbt_ons_models_build.py

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import cross_downstream
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# === Configs de caminho (batem com seu docker-compose) ===
DBT_PROJECT_DIR = "/opt/airflow/project/dbt"
DBT_PROFILES_DIR = "/opt/airflow/project/dbt/profiles"

STAGING_DIR = Path(DBT_PROJECT_DIR) / "models" / "staging"
CORE_DIR = Path(DBT_PROJECT_DIR) / "models" / "core"

# Ambiente passado pros comandos dbt (dbt lê as credenciais do profiles.yml via envs)
DBT_ENV = {
    "DBT_PROJECT_DIR": DBT_PROJECT_DIR,
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "PYTHONUNBUFFERED": "1",
}

# Helpers -------------------------------------------------
def dbt_run_selector(selector: str) -> str:
    """
    Monta comando dbt run com um selector (tag, model, path etc).
    Ex.: selector='path:models/staging/stg_usina.sql'
    """
    return (
        'set -euo pipefail; '
        'cd "$DBT_PROJECT_DIR"; '
        f'dbt run --profiles-dir "$DBT_PROFILES_DIR" -s "{selector}" --no-write-json'
    )

def dbt_test_selector(selector: str) -> str:
    return (
        'set -euo pipefail; '
        'cd "$DBT_PROJECT_DIR"; '
        f'dbt test --profiles-dir "$DBT_PROFILES_DIR" -s "{selector}" --no-write-json'
    )

def file_selector(rel_path_from_project: Path) -> str:
    # dbt aceita seletores por path:  path:models/staging/arquivo.sql
    return f"path:{rel_path_from_project.as_posix()}"


def list_sql_files(folder: Path) -> list[Path]:
    if not folder.exists():
        return []
    return sorted(p.relative_to(Path(DBT_PROJECT_DIR)) for p in folder.glob("*.sql"))


# DAG -----------------------------------------------------
with DAG(
    dag_id="dbt_ons_models_build",
    description="Roda dbt deps + staging + core (Snowflake) do projeto ONS",
    start_date=datetime(2025, 10, 1),
    schedule=None,  # deixe None e rode manualmente enquanto ajusta
    catchup=False,
    default_args={"owner": "camilla", "retries": 0},
    tags=["dbt", "ons", "snowflake"],
) as dag:

    # 0) dbt deps (sempre antes)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            'set -euo pipefail; '
            'cd "$DBT_PROJECT_DIR"; '
            'dbt deps --profiles-dir "$DBT_PROFILES_DIR"'
        ),
        env=DBT_ENV,
    )

    # 1) STAGING (uma task por arquivo .sql)
    with TaskGroup(group_id="staging", tooltip="models/staging") as tg_staging:
        stg_tasks = []
        for rel_sql in list_sql_files(STAGING_DIR):
            # rel_sql é relativo ao projeto (ex.: models/staging/stg_usina.sql)
            task_id = rel_sql.stem  # stg_usina
            stg_tasks.append(
                BashOperator(
                    task_id=task_id,
                    bash_command=dbt_run_selector(file_selector(rel_sql)),
                    env=DBT_ENV,
                )
            )

    # 2) CORE (uma task por arquivo .sql)
    with TaskGroup(group_id="core", tooltip="models/core") as tg_core:
        core_run_tasks = []
        for rel_sql in list_sql_files(CORE_DIR):
            task_id = rel_sql.stem  # ex.: dim_usina, fato_disponibilidade
            core_run_tasks.append(
                BashOperator(
                    task_id=f"{task_id}_run",
                    bash_command=dbt_run_selector(file_selector(rel_sql)),
                    env=DBT_ENV,
                )
            )

        # Opcional: um teste por arquivo core (fica mais visual)
        core_test_tasks = []
        for rel_sql in list_sql_files(CORE_DIR):
            task_id = rel_sql.stem
            t = BashOperator(
                task_id=f"{task_id}_test",
                bash_command= dbt_test_selector(file_selector(rel_sql)),
                env=DBT_ENV,
            )
            core_test_tasks.append(t)

        # Encadear run -> test para cada modelo core com mesmo índice
        # (se listas com tamanhos diferentes, só encadeia o que existir em ambos)
        for run_task, test_task in zip(core_run_tasks, core_test_tasks):
            run_task >> test_task

    # Ligações (sem usar lista >> lista):
    # deps -> todos os staging
    dbt_deps >> stg_tasks
    # todos os staging -> todos os core_run
    cross_downstream(stg_tasks, core_run_tasks)

# Fim do arquivo
