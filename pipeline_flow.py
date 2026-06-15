"""
pipeline_flow.py
----------------
Orquestração do pipeline ETL com Prefect 2.x — Arquitetura Medalhão.

Fluxo:
  extract_task (API IBGE)
      ↓
  bronze_task  (JSON bruto em disco)
      ↓
  silver_task  (PySpark: limpeza, tipagem, deduplicação)
      ↓
  gold_task    (MongoDB Atlas + SQLite)

Para rodar localmente:
    python pipeline_flow.py

Para agendar no Prefect Cloud:
    prefect deploy pipeline_flow.py:etl_pnad \
        --name "ETL PNAD Trimestral" \
        --cron "0 8 1 1,4,7,10 *"
"""

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta

from src.extract import Extract
from src.bronze import Bronze
from src.silver import Silver
from src.gold   import Gold

DB_NAME = "projeto_ocup_desocup"


# ── Tasks ──────────────────────────────────────────────────────────────────────

@task(
    name="Extract — API IBGE",
    description="Consome a API de Agregados v3 do IBGE (tabela 4093, últimos 6 trimestres).",
    retries=3,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=6),
)
def extract_task() -> dict:
    logger = get_run_logger()
    logger.info("Iniciando extração da API do IBGE...")

    dados = Extract().extract_desocupacao()

    for nome, resposta in dados.items():
        if resposta and isinstance(resposta, list) and resposta[0].get("resultados"):
            n = len(resposta[0]["resultados"])
            logger.info(f"  ✔ '{nome}': {n} categorias extraídas.")
        else:
            logger.warning(f"  ⚠ '{nome}': nenhum dado retornado.")

    return dados


@task(
    name="Bronze — persistir dados brutos",
    description="Grava o JSON cru da API em disco (camada Bronze / Data Lake).",
    retries=2,
    retry_delay_seconds=10,
)
def bronze_task(raw_data: dict) -> dict:
    logger = get_run_logger()
    logger.info("Persistindo dados brutos na camada Bronze...")

    caminhos = Bronze().save(raw_data)

    for indicador, caminho in caminhos.items():
        logger.info(f"  ✔ '{indicador}' → {caminho}")

    # Retorna o raw_data original para a Silver não precisar reler do disco
    return raw_data


@task(
    name="Silver — PySpark: limpeza e conformação",
    description=(
        "Transforma os dados brutos com PySpark: normalização, tipagem, "
        "tratamento de ausências e deduplicação."
    ),
    retries=1,
    retry_delay_seconds=15,
)
def silver_task(raw_data: dict):
    logger = get_run_logger()
    logger.info("Iniciando transformação Silver com PySpark...")

    silver = Silver()
    try:
        df = silver.transform(raw_data)
        n  = df.count()
        logger.info(f"  ✔ {n} registros gerados na camada Silver.")
        # Coleta para passar entre tasks (Prefect serializa o resultado)
        rows = [r.asDict() for r in df.collect()]
    finally:
        silver.stop()

    return rows


@task(
    name="Gold — MongoDB Atlas + SQLite",
    description=(
        "Persiste os dados Silver nos dois destinos: "
        "MongoDB Atlas (NoSQL) e SQLite (relacional)."
    ),
    retries=2,
    retry_delay_seconds=15,
)
def gold_task(rows: list[dict]) -> None:
    logger = get_run_logger()
    logger.info(f"Persistindo {len(rows)} registros na camada Gold...")

    from pyspark.sql import SparkSession
    from datetime import datetime

    spark = (
        SparkSession.builder
        .appName("PNAD_Gold")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Reconstrói o DataFrame a partir dos rows serializados
    # (converte string de data_carga de volta para datetime)
    for r in rows:
        if isinstance(r.get("data_carga"), str):
            try:
                r["data_carga"] = datetime.fromisoformat(r["data_carga"])
            except Exception:
                r["data_carga"] = datetime.utcnow()

    df_silver = spark.createDataFrame(rows)

    try:
        Gold(db_name=DB_NAME).save(df_silver)
        logger.info("  ✔ Gold concluída: MongoDB Atlas + SQLite atualizados.")
    finally:
        spark.stop()


# ── Flow principal ─────────────────────────────────────────────────────────────

@flow(
    name="ETL PNAD — Arquitetura Medalhão",
    description=(
        "Pipeline completo com arquitetura medalhão (Bronze → Silver → Gold). "
        "Fonte: API IBGE v3 · Silver: PySpark · Gold: MongoDB Atlas + SQLite."
    ),
    log_prints=True,
)
def etl_pnad() -> None:
    logger = get_run_logger()
    logger.info("=" * 55)
    logger.info("Flow ETL PNAD — Bronze → Silver → Gold")
    logger.info("=" * 55)

    raw_data = extract_task()

    if not raw_data:
        raise ValueError("Extract retornou vazio. Flow encerrado.")

    raw_data = bronze_task(raw_data)       # Bronze
    rows     = silver_task(raw_data)       # Silver (PySpark)
    gold_task(rows)                        # Gold   (MongoDB + SQLite)

    logger.info("✅ Flow ETL PNAD concluído com sucesso!")


# ── Entry‑point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    etl_pnad()
