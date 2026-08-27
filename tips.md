## Tips para lidar com os containers docker

### Comandos útes

`Rotina de desenvolvimento usando docker:`

```
cd ~/projects/tds-data-pipeline
docker compose --env-file .env.dev config --quiet
docker compose --env-file .env.dev up -d --force-recreate
docker compose --env-file .env.dev ps
```

Recriar containers:
```
cd ~/projects/tds-data-pipeline

docker compose --env-file .env.dev down

docker compose --env-file .env.dev up -d --force-recreate
```

Recriar somente containers do Airflow
```
docker compose --env-file .env.dev up -d \
  --no-deps \
  --force-recreate \
  airflow-api-server \
  airflow-scheduler \
  airflow-dag-processor \
  airflow-triggerer
```

Verificar se os arquivos foram montados corretamente
```
docker compose --env-file .env.dev exec \
  airflow-dag-processor \
  find /opt/airflow/dags /opt/airflow/pipelines \
  -maxdepth 4 -type f -print
```

Você deverá enxergar tanto a DAG quanto o pipeline, por exemplo:
```
/opt/airflow/dags/bootstrap_leads_youtube.py
/opt/airflow/pipelines/__init__.py
/opt/airflow/pipelines/bronze/__init__.py
/opt/airflow/pipelines/bronze/bootstrap_gf_leads_youtube.py
```
Cheacar os pacotes de pipeline
```
docker compose --env-file .env.dev exec \
  airflow-dag-processor \
  python -c "import pipelines; print(pipelines.__file__)"
```