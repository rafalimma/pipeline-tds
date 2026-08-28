import logging
from datetime import datetime, timezone
from uuid import uuid4

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

SOURCE_CONN_ID = "source_pg"
WEREHOUSE_CONN_ID = "warehouse_pg"
BATCH_SIZE = 5000

PIPELINE_NAME = "azure__gf__leads_youtube"
SOURCE_SELECT = """
    SELECT
        id,
        data_criacao_bd,
        data_submit,
        nome_completo,
        telefone,
        email,
        motivo_inscricao,
        form_id,
        form_name,
        utm_source,
        utm_medium,
        utm_content,
        utm_campaign,
        utm_id
    FROM gf.leads_youtube
    WHERE id > %s
      AND id <= %s
    ORDER BY id
    LIMIT %s
"""
TARGET_INSERT = """
    INSERT INTO bronze.gf_leads_youtube (
        id,
        data_criacao_bd,
        data_submit,
        nome_completo,
        telefone,
        email,
        motivo_inscricao,
        form_id,
        form_name,
        utm_source,
        utm_medium,
        utm_content,
        utm_campaign,
        utm_id
    )
    VALUES %s
    ON CONFLICT (id) DO NOTHING
"""

def incremental_gf_leads_youtube() -> int:
    source_hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
    warehouse_hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)

    source_conn = source_hook.get_conn()
    warehouse_conn = warehouse_hook.get_conn()

    total_inseridos = 0

    try:
        # Descobre até qual ID a origem possuía no início da execução.
        with source_conn.cursor() as source_cursor:
            source_cursor.execute(
                "SELECT COALESCE(MAX(id), 0) FROM gf.leads_youtube"
            )
            cutoff_id = source_cursor.fetchone()[0]

        # Descobre o último ID já carregado na Bronze.
        with warehouse_conn.cursor() as warehouse_cursor:
            warehouse_cursor.execute(
                """
                SELECT COALESCE(MAX(id), 0)
                FROM bronze.gf_leads_youtube
                """
            )
            last_id = warehouse_cursor.fetchone()[0]

        logger.info(
            "Carga incremental iniciada: last_id=%s, cutoff_id=%s",
            last_id,
            cutoff_id,
        )

        if last_id >= cutoff_id:
            logger.info("Nenhum registro novo encontrado.")
            return 0

        while last_id < cutoff_id:
            with source_conn.cursor() as source_cursor:
                source_cursor.execute(
                    SOURCE_SELECT,
                    (last_id, cutoff_id, BATCH_SIZE),
                )
                registros = source_cursor.fetchall()

            if not registros:
                break

            with warehouse_conn.cursor() as warehouse_cursor:
                execute_values(
                    warehouse_cursor,
                    TARGET_INSERT,
                    registros,
                    page_size=BATCH_SIZE,
                )
                inseridos_lote = warehouse_cursor.rowcount

            warehouse_conn.commit()

            last_id = registros[-1][0]
            total_inseridos += inseridos_lote

            logger.info(
                "Lote concluído: ultimo_id=%s, recebidos=%s, inseridos=%s",
                last_id,
                len(registros),
                inseridos_lote,
            )

        logger.info(
            "Carga incremental concluída. Total inserido: %s",
            total_inseridos,
        )

        return total_inseridos

    except Exception:
        warehouse_conn.rollback()
        logger.exception("Erro durante a carga incremental.")
        raise

    finally:
        source_conn.close()
        warehouse_conn.close()


def bootstrap_gf_leads_youtube(
    batch_size: int = 5000,
) -> dict:
    source_hook = PostgresHook(
        postgres_conn_id="source_pg"
    )
    warehouse_hook = PostgresHook(
        postgres_conn_id="warehouse_pg"
    )

    source_conn = source_hook.get_conn()
    warehouse_conn = warehouse_hook.get_conn()

    source_stream = None

    batch_id = str(uuid4())
    loaded_at = datetime.now(timezone.utc)

    source_count = 0
    extracted_rows = 0
    staging_count = 0
    bronze_count = 0
    cutoff_id = 0

    try:
        source_conn.autocommit = False
        warehouse_conn.autocommit = False

        # Cria uma visão consistente da origem.
        with source_conn.cursor() as source_cursor:
            # Deve ser a primeira instrução da transação.
            source_cursor.execute(
                """
                SET TRANSACTION
                    ISOLATION LEVEL REPEATABLE READ
                    READ ONLY
                """
            )

            source_cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COALESCE(MAX(id), 0) AS max_id
                FROM gf.leads_youtube
                """
            )

            source_result = source_cursor.fetchone()

            if source_result is None:
                raise RuntimeError(
                    "Não foi possível consultar a origem"
                )

            source_count, cutoff_id = source_result

            source_count = int(source_count)
            cutoff_id = int(cutoff_id)

        logger.info(
            "Bootstrap iniciado: pipeline=%s, "
            "batch_id=%s, total=%s, cutoff_id=%s",
            PIPELINE_NAME,
            batch_id,
            source_count,
            cutoff_id,
        )

        # Limpa somente a tabela temporária.
        with warehouse_conn.cursor() as destination_cursor:
            destination_cursor.execute(
                """
                TRUNCATE TABLE
                    bronze._stg__azure__gf__leads_youtube
                """
            )

        # Cursor de servidor: não carrega toda a origem na memória.
        source_stream = source_conn.cursor(
            name=(
                "bootstrap_youtube_"
                + batch_id.replace("-", "")
            )
        )

        source_stream.execute(
            SOURCE_SELECT,
            (cutoff_id,),
        )

        while True:
            rows = source_stream.fetchmany(batch_size)

            if not rows:
                break

            with warehouse_conn.cursor() as destination_cursor:
                destination_cursor.executemany(
                    STAGING_INSERT,
                    rows,
                )

            extracted_rows += len(rows)

            logger.info(
                "Progresso da extração: %s/%s registros",
                extracted_rows,
                source_count,
            )

        # Valida staging e substitui a Bronze atomicamente.
        with warehouse_conn.cursor() as destination_cursor:
            destination_cursor.execute(
                """
                SELECT COUNT(*)
                FROM bronze._stg__azure__gf__leads_youtube
                """
            )

            staging_count = int(
                destination_cursor.fetchone()[0]
            )

            if staging_count != source_count:
                raise RuntimeError(
                    "Quantidade divergente na staging: "
                    f"origem={source_count}, "
                    f"staging={staging_count}"
                )

            destination_cursor.execute(
                """
                TRUNCATE TABLE
                    bronze.azure__gf__leads_youtube
                RESTART IDENTITY
                """
            )

            destination_cursor.execute(
                """
                INSERT INTO bronze.azure__gf__leads_youtube (
                    id,
                    data_criacao_bd,
                    data_submit,
                    nome_completo,
                    telefone,
                    email,
                    motivo_inscricao,
                    form_id,
                    form_name,
                    utm_source,
                    utm_medium,
                    utm_content,
                    utm_campaign,
                    utm_id,
                    _loaded_at,
                    _batch_id
                )
                SELECT
                    id,
                    data_criacao_bd,
                    data_submit,
                    nome_completo,
                    telefone,
                    email,
                    motivo_inscricao,
                    form_id,
                    form_name,
                    utm_source,
                    utm_medium,
                    utm_content,
                    utm_campaign,
                    utm_id,
                    %s,
                    %s::UUID
                FROM bronze._stg__azure__gf__leads_youtube
                """,
                (
                    loaded_at,
                    batch_id,
                ),
            )

            destination_cursor.execute(
                """
                SELECT COUNT(*)
                FROM bronze.azure__gf__leads_youtube
                """
            )

            bronze_count = int(
                destination_cursor.fetchone()[0]
            )

            if bronze_count != source_count:
                raise RuntimeError(
                    "Quantidade divergente na Bronze: "
                    f"origem={source_count}, "
                    f"bronze={bronze_count}"
                )

            # Só atualiza o watermark após as validações.
            destination_cursor.execute(
                """
                INSERT INTO control.pipeline_watermark (
                    pipeline_name,
                    last_id,
                    last_success_at,
                    rows_last_load,
                    updated_at
                )
                VALUES (
                    %s,
                    %s,
                    CURRENT_TIMESTAMP,
                    %s,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (pipeline_name)
                DO UPDATE SET
                    last_id = EXCLUDED.last_id,
                    last_success_at = EXCLUDED.last_success_at,
                    rows_last_load = EXCLUDED.rows_last_load,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    PIPELINE_NAME,
                    cutoff_id,
                    bronze_count,
                ),
            )

        # Bronze e watermark são confirmados juntos.
        warehouse_conn.commit()

        result = {
            "pipeline": PIPELINE_NAME,
            "batch_id": batch_id,
            "source_count": source_count,
            "extracted_rows": extracted_rows,
            "staging_count": staging_count,
            "bronze_count": bronze_count,
            "last_id": cutoff_id,
        }

        logger.info(
            "Bootstrap concluído com sucesso: %s",
            result,
        )

        return result

    except Exception:
        warehouse_conn.rollback()

        logger.exception(
            "Falha no bootstrap da pipeline %s",
            PIPELINE_NAME,
        )

        raise

    finally:
        if source_stream is not None:
            source_stream.close()

        source_conn.close()
        warehouse_conn.close()