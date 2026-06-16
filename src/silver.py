"""
src/silver.py
-------------
Camada Silver — dados limpos e conformados.

Responsabilidade: ler os dados brutos da camada Bronze e produzir
uma lista de documentos normalizados, tipados e sem valores ausentes.

Substituição do PySpark por pandas:
  Para o volume da PNAD (27 UFs × 3 sexos × 6 trimestres × 3 indicadores),
  o pandas é suficiente e elimina a dependência do Java/PySpark.
  O PySpark seria justificado em cenários com histórico acumulado de
  múltiplas execuções ou integração com clusters distribuídos.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

# Marcadores de ausência na API do IBGE
_AUSENTES = {"...", "-", "..", ""}

# Mapeamento de id de categoria de sexo → rótulo
_SEXO_MAP = {"2793": "Homem", "2794": "Mulher", "93070": "Total"}

# Unidade por indicador
# Variável IBGE 4096 (taxa de participação) é percentual, não volume.
_UNIDADE = {
    "forca_trabalho": "%",
    "desocupacao":    "%",
    "informalidade":  "%",
}


class Silver:
    """Transforma dados Bronze em registros Silver usando pandas."""

    # ── Interface pública ──────────────────────────────────────────────────────

    def transform(self, raw_data: dict[str, Any]) -> list[dict]:
        """
        Recebe {indicador: resposta_api} e devolve lista de documentos
        normalizados, tipados e deduplicados — prontos para a camada Gold.
        """
        registros = []
        data_carga = datetime.now(timezone.utc).isoformat()

        for indicador, resposta in raw_data.items():
            registros.extend(self._flatten(indicador, resposta, data_carga))

        if not registros:
            print("[Silver] Nenhum registro gerado.")
            return []

        # Deduplicação e limpeza com pandas
        df = pd.DataFrame(registros)
        df = df[df["valor"].notna()]
        df = df[df["periodo"].str.len() == 6]
        df["localidade"] = df["localidade"].str.strip()
        df["categoria"]  = df["categoria"].str.strip()
        df["periodo_label"] = df["periodo"].apply(self._formatar_periodo)
        df = df.drop_duplicates(subset=["indicador", "localidade", "categoria", "periodo"])
        df = df.sort_values(["indicador", "localidade", "categoria", "periodo"])

        resultado = df.to_dict(orient="records")
        print(f"[Silver] {len(resultado)} registros gerados após limpeza e deduplicação.")
        return resultado

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _flatten(self, indicador: str, resposta_api: list, data_carga: str) -> list[dict]:
        """Achata o JSON aninhado da API em uma lista de dicionários planos."""
        registros = []
        unidade = _UNIDADE.get(indicador, "%")

        if not isinstance(resposta_api, list) or not resposta_api:
            return registros

        variavel_obj = resposta_api[0]

        for bloco in variavel_obj.get("resultados", []):
            categoria = self._extrair_sexo(bloco)

            for serie in bloco.get("series", []):
                loc   = serie.get("localidade", {})
                nome  = loc.get("nome", "Brasil")
                nivel = loc.get("nivel", {}).get("id", "N1")

                for periodo, valor_str in serie.get("serie", {}).items():
                    registros.append({
                        "indicador":  indicador,
                        "localidade": nome,
                        "nivel":      nivel,
                        "categoria":  categoria,
                        "periodo":    str(periodo).strip(),
                        "valor":      self._parse_valor(valor_str),
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
    def _formatar_periodo(periodo: str) -> str:
        """'202401' → '2024 T1'"""
        if len(periodo) == 6 and periodo.isdigit():
            trim = periodo[4:].lstrip("0") or "0"
            return f"{periodo[:4]} T{trim}"
        return periodo

    @staticmethod
    def _parse_valor(valor_str: str) -> float | None:
        v = str(valor_str).strip()
        if v in _AUSENTES:
            return None
        try:
            return float(v.replace(",", "."))
        except (ValueError, TypeError):
            return None
