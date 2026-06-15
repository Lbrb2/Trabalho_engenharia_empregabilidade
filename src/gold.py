"""
src/gold.py
-----------
Camada Gold — dados agregados prontos para consumo.

Persiste a lista de documentos Silver em dois destinos:
  1. MongoDB Atlas (NoSQL documental) — serve o MCP e o chatbot
  2. SQLite (relacional)             — consultas SQL e BI

Justificativa dos dois bancos:
  MongoDB é ideal para consultas flexíveis por documento (filtro por
  UF + sexo + período). SQLite é ideal para agregações analíticas
  (GROUP BY, window functions) e integração com ferramentas de BI.
"""

from __future__ import annotations

import os
import sqlite3
from typing import Any

from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

load_dotenv()

_DB_USER     = os.getenv("DB_USER")
_DB_PASSWORD = os.getenv("DB_PASSWORD")
_MONGO_URI   = (
    f"mongodb+srv://{_DB_USER}:{_DB_PASSWORD}"
    "@clusterbob.jilronv.mongodb.net/?appName=ClusterBob"
)

SQLITE_PATH = os.path.join(os.path.dirname(__file__), "..", "gold", "pnad.db")

_MONGO_COLECOES = {
    "desocupacao": {
        "campo":        "taxa_desocupacao",
        "col_nacional": "desocupacao_nacional",
        "col_estadual": "desocupacao_estadual",
    },
    "informalidade": {
        "campo":        "taxa_informalidade",
        "col_nacional": "ocupacao_informal_nacional",
        "col_estadual": "ocupacao_informal_estadual",
    },
    "forca_trabalho": {
        "campo":        "taxa_participacao",
        "col_nacional": "ocupacao_ativa_nacional",
        "col_estadual": "ocupacao_ativa_estadual",
    },
}


class Gold:
    """Persiste os documentos Silver no MongoDB Atlas e no SQLite."""

    def __init__(
        self,
        db_name: str = "projeto_ocup_desocup",
        sqlite_path: str = SQLITE_PATH,
    ) -> None:
        self.db_name     = db_name
        self.sqlite_path = sqlite_path
        os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)

    def save(self, rows: list[dict]) -> None:
        """Persiste a lista de documentos nos dois destinos."""
        print(f"[Gold] Persistindo {len(rows)} registros...")
        self._save_mongo(rows)
        self._save_sqlite(rows)
        print("[Gold] Persistência concluída nos dois bancos.")

    # ── MongoDB ────────────────────────────────────────────────────────────────

    def _save_mongo(self, rows: list[dict]) -> None:
        client = MongoClient(_MONGO_URI, server_api=ServerApi("1"))
        db     = client[self.db_name]

        try:
            grupos: dict[str, list[dict]] = {}
            for row in rows:
                key = f"{row['indicador']}|{row['nivel']}"
                grupos.setdefault(key, []).append(row)

            for key, registros in grupos.items():
                ind, nivel = key.split("|")
                cfg = _MONGO_COLECOES.get(ind)
                if not cfg:
                    continue

                nacional  = nivel == "N1"
                col_name  = cfg["col_nacional"] if nacional else cfg["col_estadual"]
                campo_val = cfg["campo"]
                col       = db[col_name]

                docs = [
                    {
                        "categoria":  r["categoria"],
                        "periodo":    r["periodo"],
                        "localidade": r["localidade"],
                        campo_val:    r["valor"],
                    }
                    for r in registros
                ]

                col.delete_many({})
                col.insert_many(docs)
                print(f"  [MongoDB] '{col_name}': {len(docs)} docs inseridos.")
        finally:
            client.close()

    # ── SQLite ─────────────────────────────────────────────────────────────────

    def _save_sqlite(self, rows: list[dict]) -> None:
        con = sqlite3.connect(self.sqlite_path)
        cur = con.cursor()

        cur.execute("DROP TABLE IF EXISTS indicadores_pnad")
        cur.execute("""
            CREATE TABLE indicadores_pnad (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                indicador     TEXT NOT NULL,
                localidade    TEXT NOT NULL,
                nivel         TEXT NOT NULL,
                categoria     TEXT NOT NULL,
                periodo       TEXT NOT NULL,
                periodo_label TEXT,
                valor         REAL,
                unidade       TEXT,
                data_carga    TEXT,
                UNIQUE (indicador, localidade, categoria, periodo)
            )
        """)

        cur.executemany(
            """
            INSERT OR REPLACE INTO indicadores_pnad
              (indicador, localidade, nivel, categoria, periodo,
               periodo_label, valor, unidade, data_carga)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r["indicador"], r["localidade"], r["nivel"],
                    r["categoria"], r["periodo"], r.get("periodo_label"),
                    r["valor"], r["unidade"], str(r.get("data_carga", "")),
                )
                for r in rows
            ],
        )

        con.commit()
        n = cur.execute("SELECT COUNT(*) FROM indicadores_pnad").fetchone()[0]
        con.close()
        print(f"  [SQLite] '{self.sqlite_path}': {n} registros gravados.")
