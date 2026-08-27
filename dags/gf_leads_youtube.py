from datetime import timedelta
import pendulum

from airflow.sdk import dag, task
from pipelines.bronze.gf_leads_youtube import (
    bootstrap_gf_leads_youtube
)

@dag(
    dag_id="gf_leads_youtube",
    description="Carga inicial completa de gf.leads_youtube para a Bronze",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="America/Sao_Paulo",
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bronze", "gf", "youtube", "bootstrap"],
)
def bootstrap_youtube():
    @task
    def full_load():
        return bootstrap_gf_leads_youtube(batch_size=5000)
    full_load()

bootstrap_youtube()