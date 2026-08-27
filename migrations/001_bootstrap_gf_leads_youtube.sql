CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS bronze.azure__gf__leads_youtube (
    _bronze_row_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    id                  BIGINT NOT NULL,
    data_criacao_bd     TIMESTAMPTZ NOT NULL,
    data_submit         TIMESTAMPTZ NOT NULL,
    nome_completo       VARCHAR NOT NULL,
    telefone            TEXT NOT NULL,
    email               TEXT NOT NULL,
    motivo_inscricao    TEXT NOT NULL,
    form_id             TEXT,
    form_name           TEXT,
    utm_source          TEXT,
    utm_medium          TEXT,
    utm_content         TEXT,
    utm_campaign        TEXT,
    utm_id              TEXT,

    _loaded_at          TIMESTAMPTZ NOT NULL,
    _batch_id           UUID NOT NULL
);

-- undice nao unico, pois a origem possui IDs repetidos.
CREATE INDEX IF NOT EXISTS idx_azure_gf_leads_youtube_id
    ON bronze.azure__gf__leads_youtube (id);

-- Tabela temporária de trabalho, não exposta aos analistas.
CREATE UNLOGGED TABLE IF NOT EXISTS bronze._stg__azure__gf__leads_youtube (
    id                  BIGINT NOT NULL,
    data_criacao_bd     TIMESTAMPTZ NOT NULL,
    data_submit         TIMESTAMPTZ NOT NULL,
    nome_completo       VARCHAR NOT NULL,
    telefone            TEXT NOT NULL,
    email               TEXT NOT NULL,
    motivo_inscricao    TEXT NOT NULL,
    form_id             TEXT,
    form_name           TEXT,
    utm_source          TEXT,
    utm_medium          TEXT,
    utm_content         TEXT,
    utm_campaign        TEXT,
    utm_id              TEXT
);

CREATE TABLE IF NOT EXISTS control.pipeline_watermark (
    pipeline_name       TEXT PRIMARY KEY,
    last_id             BIGINT NOT NULL DEFAULT 0,
    last_success_at     TIMESTAMPTZ,
    rows_last_load      BIGINT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);