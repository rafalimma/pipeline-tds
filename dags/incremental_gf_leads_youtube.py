import pendulum
from airflow.sdk import dag, task

from pipelines.bronze.gf_leads_youtube import (
    incremental_gf_leads_youtube
)

@dag(
    dag_id="incremental_gf_leads_youtube",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz="America/Sao_Paulo",
    ),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "incremental", "youtube"],
)
def incremental_youtube_dag():
    @task(retries=2)
    def carregar_incremental():
        return incremental_gf_leads_youtube()
    carregar_incremental()

incremental_youtube_dag()