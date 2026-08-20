import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="test_postgres_connections",
    description="Testa as conexões com a origem e o warehouse",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="America/Sao_Paulo",
    ),
    catchup=False,
    tags=["teste", "postgres"],
)
def test_postgres_connections():

    @task
    def test_connection(connection_id: str) -> dict:
        hook = PostgresHook(postgres_conn_id=connection_id)

        result = hook.get_first(
            """
            SELECT
                current_database(),
                current_user,
                current_timestamp;
            """
        )

        information = {
            "connection_id": connection_id,
            "database": result[0],
            "user": result[1],
            "timestamp": str(result[2]),
        }

        print(information)
        return information

    test_connection.override(
        task_id="test_source_pg"
    )("source_pg")

    test_connection.override(
        task_id="test_warehouse_pg"
    )("warehouse_pg")


test_postgres_connections()