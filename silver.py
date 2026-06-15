"""
src/silver.py
-------------
Camada Silver — dados limpos e conformados.

Responsabilidade: ler os dados brutos da camada Bronze, aplicar
transformações com PySpark e produzir um DataFrame padronizado,
tipado e sem valores ausentes problemáticos.

Justificativa do PySpark nesta etapa:
  A PNAD Contínua cobre 27 UFs × 3 categorias de sexo × N trimestres
  × 3 indicadores. Com múltiplas execuções acumuladas na Bronze, o
  volume cresce rapidamente. O PySpark permite processar esse volume
  de forma distribuída e expressiva, com lazy evaluation e otimizações
  automáticas do Catalyst — vantagem real frente a loops Python puros.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, TimestampType,
)

# Schema esperado após a normalização
SILVER_SCHEMA = StructType([
    StructField("indicador",  StringType(),   nullable=False),
    StructField("localidade", StringType(),   nullable=False),
    StructField("nivel",      StringType(),   nullable=False),   # N1=Brasil, N3=UF
    StructField("categoria",  StringType(),   nullable=False),   # Homem/Mulher/Total
    StructField("periodo",    StringType(),   nullable=False),   # '202401'
    StructField("valor",      FloatType(),    nullable=True),
    StructField("unidade",    StringType(),   nullable=False),
    StructField("data_carga", TimestampType(),nullable=False),
])

# Unidade por indicador
_UNIDADE = {
    "forca_trabalho": "mil pessoas",
    "desocupacao":    "%",
    "informalidade":  "%",
}

# Mapa id_categoria_sexo → rótulo
_SEXO_MAP = {"2793": "Homem", "2794": "Mulher", "93070": "Total"}

# Valores ausentes da API
_AUSENTES = {"...", "-", "..", ""}


class Silver:
    """Transforma dados Bronze em Silver usando PySpark."""

    def __init__(self) -> None:
        self.spark = (
            SparkSession.builder
            .appName("PNAD_Silver")
            .master("local[*]")             # usa todos os cores locais
            .config("spark.sql.shuffle.partitions", "4")   # adequado ao volume PNAD
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")

    # ── Interface pública ──────────────────────────────────────────────────────

    def transform(self, raw_data: dict[str, Any]) -> DataFrame:
        """
        Recebe {indicador: resposta_api} e devolve um DataFrame Silver
        com todos os registros normalizados e tipados.
        """
        registros = []

        for indicador, resposta in raw_data.items():
            registros.extend(self._flatten(indicador, resposta))

        df_raw = self.spark.createDataFrame(registros)

        df_silver = (
            df_raw
            # Remove registros sem valor (IBGE não divulgou)
            .filter(F.col("valor").isNotNull())
            # Garante que o período tem 6 dígitos
            .filter(F.length(F.col("periodo")) == 6)
            # Padroniza texto
            .withColumn("localidade", F.trim(F.col("localidade")))
            .withColumn("categoria",  F.trim(F.col("categoria")))
            # Adiciona coluna de período legível: '202401' → '2024 T1'
            .withColumn(
                "periodo_label",
                F.concat(
                    F.col("periodo").substr(1, 4),
                    F.lit(" T"),
                    F.col("periodo").substr(5, 2).cast("int").cast("string"),
                )
            )
            .dropDuplicates(["indicador", "localidade", "categoria", "periodo"])
            .orderBy("indicador", "localidade", "categoria", "periodo")
        )

        n = df_silver.count()
        print(f"[Silver] {n} registros gerados após limpeza e deduplicação.")
        return df_silver

    def stop(self) -> None:
        self.spark.stop()

    # ── Helpers de normalização ────────────────────────────────────────────────

    def _flatten(self, indicador: str, resposta_api: list) -> list[dict]:
        """Achata o JSON aninhado da API em uma lista de dicionários planos."""
        from datetime import datetime, timezone

        registros = []
        data_carga = datetime.now(timezone.utc)
        unidade = _UNIDADE.get(indicador, "%")

        if not isinstance(resposta_api, list) or not resposta_api:
            return registros

        variavel_obj = resposta_api[0]

        for bloco in variavel_obj.get("resultados", []):
            categoria = self._extrair_sexo(bloco)

            for serie in bloco.get("series", []):
                loc  = serie.get("localidade", {})
                nome = loc.get("nome", "Brasil")
                nivel = loc.get("nivel", {}).get("id", "N1")

                for periodo, valor_str in serie.get("serie", {}).items():
                    valor = self._parse_valor(valor_str)
                    registros.append({
                        "indicador":  indicador,
                        "localidade": nome,
                        "nivel":      nivel,
                        "categoria":  categoria,
                        "periodo":    str(periodo).strip(),
                        "valor":      valor,
                        "unidade":    unidade,
                        "data_carga": data_carga,
                    })

        return registros

    @staticmethod
    def _extrair_sexo(bloco: dict) -> str:
        for clf in bloco.get("classificacoes", []):
            for id_cat, nome_cat in clf.get("categoria", {}).items():
                return _SEXO_MAP.get(id_cat, nome_cat)
        return "Total"

    @staticmethod
    def _parse_valor(valor_str: str) -> float | None:
        v = str(valor_str).strip()
        if v in _AUSENTES:
            return None
        try:
            return float(v.replace(",", "."))
        except (ValueError, TypeError):
            return None
