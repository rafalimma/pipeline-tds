BEGIN;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS audit;

COMMENT ON SCHEMA bronze IS
    'Dados preservados das fontes locais e entidades selecionadas';

COMMENT ON SCHEMA silver IS
    'Dados normalizados, deduplicados e integrados';

COMMENT ON SCHEMA gold IS
    'Fatos, dimensões e datasets publicados para o BI';

COMMENT ON SCHEMA control IS
    'Watermarks, configurações e estado dos pipelines';

COMMENT ON SCHEMA audit IS
    'Execuções, rejeições, reconciliação e qualidade';

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

COMMIT;