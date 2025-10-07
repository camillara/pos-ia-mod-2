# infra/airflow/dags/dbt_ons_models_build.py
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import cross_downstream

# --- Paths (batem com seu docker-compose) ---
DBT_PROJECT_DIR = "/opt/airflow/project/dbt"
DBT_PROFILES_DIR = "/opt/airflow/project/dbt/profiles"

STAGING_DIR     = Path(DBT_PROJECT_DIR) / "models" / "staging"
CORE_DIR        = Path(DBT_PROJECT_DIR) / "models" / "core"

# --- Env que o dbt usa dentro do container ---
DBT_ENV = {
    "DBT_PROJECT_DIR":   DBT_PROJECT_DIR,
    "DBT_PROFILES_DIR":  DBT_PROFILES_DIR,
    "PYTHONUNBUFFERED":  "1",
}

# ----------------- helpers -----------------
def file_selector(rel_path_from_project: Path) -> str:
    # dbt aceita path selectors: path:models/staging/arquivo.sql
    return f"path:{rel_path_from_project.as_posix()}"

def dbt_run_selector(selector: str) -> str:
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

def list_sql_files(folder: Path, pattern: str = "*.sql") -> list[Path]:
    if not folder.exists():
        return []
    # retornamos caminhos RELATIVOS ao projeto dbt (p/ montar o path: selector)
    return sorted(p.relative_to(Path(DBT_PROJECT_DIR)) for p in folder.glob(pattern))

def build_model_group(
    parent: TaskGroup,
    group_id: str,
    files: list[Path],
    *,
    include_tests: bool = True,
):
    """Cria um TaskGroup com start/end e, para cada arquivo .sql,
    tasks <model>_run e (opcional) <model>_test. Liga start -> run -> test -> end.
    Retorna (start, end, run_tasks)."""
    with TaskGroup(group_id=group_id, tooltip=group_id, parent_group=parent) as tg:
        start = EmptyOperator(task_id="start")
        end   = EmptyOperator(task_id="end")

        run_tasks: list[BashOperator] = []
        for rel_sql in files:
            model_id = rel_sql.stem  # ex.: dim_usina, fact_disponibilidade, stg_...
            selector = file_selector(rel_sql)

            t_run = BashOperator(
                task_id=f"{model_id}_run",
                bash_command=dbt_run_selector(selector),
                env=DBT_ENV,
            )
            start >> t_run  # começo do subgrupo

            if include_tests:
                t_test = BashOperator(
                    task_id=f"{model_id}_test",
                    bash_command=dbt_test_selector(selector),
                    env=DBT_ENV,
                )
                t_run >> t_test >> end
            else:
                t_run >> end

            run_tasks.append(t_run)

        return start, end, run_tasks

# ----------------- DAG -----------------
with DAG(
    dag_id="dbt_ons_models_build",
    description="Roda dbt deps + staging + core (Snowflake) do projeto ONS",
    start_date=datetime(2025, 10, 1),
    schedule=None,            # rode manualmente
    catchup=False,
    default_args={"owner": "camilla", "retries": 0},
    tags=["dbt", "ons", "snowflake"],
) as dag:

    # 0) dbt deps antes de tudo
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command='set -euo pipefail; cd "$DBT_PROJECT_DIR"; dbt deps --profiles-dir "$DBT_PROFILES_DIR"',
        env=DBT_ENV,
    )

    # 1) STAGING: um task por SQL em models/staging/*.sql
    stg_files = list_sql_files(STAGING_DIR, "*.sql")
    with TaskGroup(group_id="staging", tooltip="models/staging") as tg_staging:
        # aqui só run (normalmente não testamos staging)
        stg_start, stg_end, stg_runs = build_model_group(
            parent=tg_staging, group_id="tasks", files=stg_files, include_tests=False
        )

    # 2) CORE com subgrupos: dimensions e facts
    with TaskGroup(group_id="core", tooltip="models/core") as tg_core:
        # a) dimensions: models/core/dim_*.sql
        dim_files  = list_sql_files(CORE_DIR, "dim_*.sql")
        dim_start, dim_end, dim_runs = build_model_group(
            parent=tg_core, group_id="dimensions", files=dim_files, include_tests=True
        )

        # b) facts: models/core/fact_*.sql  (ajuste prefixo se usar 'fato_')
        fact_files = list_sql_files(CORE_DIR, "fact_*.sql") or list_sql_files(CORE_DIR, "fato_*.sql")
        fact_start, fact_end, fact_runs = build_model_group(
            parent=tg_core, group_id="facts", files=fact_files, include_tests=True
        )

        # encadear: todas as dimensions antes dos facts (visual no Graph)
        dim_end >> fact_start

    # ---- Dependências de alto nível (com setinhas visíveis) ----
    dbt_deps >> tg_staging                 # deps -> staging
    stg_end   >> dim_start                 # staging -> dimensions
    # (dentro do core já ligamos dimensions -> facts)

    # Além disso, garanta que qualquer model de staging termine antes de QUALQUER run de core:
    cross_downstream(stg_runs, dim_runs + fact_runs)
