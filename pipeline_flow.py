"""
pipeline_flow.py
----------------
Orquestração do pipeline ETL com Dagster — Arquitetura Medalhão.

Fluxo:
  extract_op → bronze_op → silver_op → gold_op

Para rodar:
    python pipeline_flow.py

Para abrir a UI do Dagster:
    dagster dev -f pipeline_flow.py
"""

from dagster import (
    op,
    job,
    schedule,
    RetryPolicy,
    Backoff,
    get_dagster_logger,
    RunRequest,
    ScheduleEvaluationContext,
)

from src.extract import Extract
from src.bronze  import Bronze
from src.silver  import Silver
from src.gold    import Gold

DB_NAME = "projeto_ocup_desocup"


# ── Ops ────────────────────────────────────────────────────────────────────────

@op(
    name="extract_op",
    description="Consome a API de Agregados v3 do IBGE (tabela 4093, últimos 6 trimestres).",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=30,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def extract_op(context) -> dict:
    logger = get_dagster_logger()
    logger.info("Iniciando extração da API do IBGE...")

    dados = Extract().extract_indicadores()

    if not dados:
        raise ValueError("Extract retornou vazio.")

    for nome, resposta in dados.items():
        if resposta and isinstance(resposta, list) and resposta[0].get("resultados"):
            n = len(resposta[0]["resultados"])
            logger.info(f"  ✔ '{nome}': {n} categorias extraídas.")
        else:
            logger.warning(f"  ⚠ '{nome}': nenhum dado retornado.")

    return dados


@op(
    name="bronze_op",
    description="Persiste o JSON bruto da API em disco (camada Bronze).",
    retry_policy=RetryPolicy(max_retries=2, delay=10),
)
def bronze_op(context, raw_data: dict) -> dict:
    logger = get_dagster_logger()
    logger.info("Persistindo dados brutos na camada Bronze...")

    caminhos = Bronze().save(raw_data)
    for indicador, caminho in caminhos.items():
        logger.info(f"  ✔ '{indicador}' → {caminho}")

    return raw_data


@op(
    name="silver_op",
    description="Transforma os dados com pandas: limpeza, tipagem e deduplicação (camada Silver).",
    retry_policy=RetryPolicy(max_retries=1, delay=15),
)
def silver_op(context, raw_data: dict) -> list:
    logger = get_dagster_logger()
    logger.info("Iniciando transformação Silver com pandas...")

    rows = Silver().transform(raw_data)
    logger.info(f"  ✔ {len(rows)} registros gerados na camada Silver.")
    return rows


@op(
    name="gold_op",
    description="Persiste os dados Silver no MongoDB Atlas e no SQLite (camada Gold).",
    retry_policy=RetryPolicy(max_retries=2, delay=15),
)
def gold_op(context, rows: list) -> None:
    logger = get_dagster_logger()
    logger.info(f"Persistindo {len(rows)} registros na camada Gold...")

    Gold(db_name=DB_NAME).save(rows)
    logger.info("  ✔ Gold concluída: MongoDB Atlas + SQLite atualizados.")


# ── Job ────────────────────────────────────────────────────────────────────────

@job(
    name="etl_pnad",
    description=(
        "Pipeline ETL completo com arquitetura medalhão (Bronze → Silver → Gold). "
        "Fonte: API IBGE v3 · Silver: pandas · Gold: MongoDB Atlas + SQLite."
    ),
)
def etl_pnad():
    raw    = extract_op()
    bronze = bronze_op(raw)
    rows   = silver_op(bronze)
    gold_op(rows)


# ── Schedule ───────────────────────────────────────────────────────────────────

@schedule(
    job=etl_pnad,
    cron_schedule="0 8 1 1,4,7,10 *",
    name="etl_pnad_trimestral",
    description="Executa o pipeline ETL no início de cada trimestre.",
)
def etl_pnad_trimestral(context: ScheduleEvaluationContext):
    return RunRequest()


# ── Entry‑point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    result = etl_pnad.execute_in_process()
    if result.success:
        print("✅ Pipeline ETL concluído com sucesso!")
    else:
        print("❌ Pipeline ETL falhou. Verifique os logs.")


# ── Definitions (obrigatório para o Dagster Cloud reconhecer o schedule) ───────
from dagster import Definitions

defs = Definitions(
    jobs=[etl_pnad],
    schedules=[etl_pnad_trimestral],
)
