"""DAG do Airflow: mesma ordem do runner local, agora agendada.

Instalar com `docker compose -f ops/docker-compose.airflow.yml up`, que monta
este diretorio em /opt/airflow/dags e o projeto em /opt/pipeline.

As tasks chamam o CLI - nao reimplementam logica. Assim o que roda no
Airflow e' exatamente o que roda no CI e na mao.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

CAMINHO_PROJETO = "/opt/pipeline"
COMUM = {
    "owner": "matheus",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="indicadores_bcb",
    description="Extrai series do BCB, carrega no banco e valida a qualidade",
    schedule="0 6 * * *",          # todo dia 06:00 (America/Sao_Paulo no compose)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=COMUM,
    tags=["dados", "bcb", "etl"],
) as dag:

    extrair = BashOperator(
        task_id="extrair_e_carregar",
        bash_command=(
            "cd %s && python -m pipeline run "
            "--inicio 01/01/2015 --fim 31/12/2026" % CAMINHO_PROJETO
        ),
    )

    validar = BashOperator(
        task_id="validar_qualidade",
        bash_command="cd %s && python -m pipeline status" % CAMINHO_PROJETO,
    )

    publicar = BashOperator(
        task_id="publicar_relatorio",
        bash_command="cd %s && python -m pipeline relatorio --json > /tmp/relatorio.json && cat /tmp/relatorio.json"
        % CAMINHO_PROJETO,
    )

    extrair >> validar >> publicar
