"""
src/bronze.py
-------------
Camada Bronze — dados brutos.

Responsabilidade: receber o JSON cru da API do IBGE e persistir
sem nenhuma transformação, preservando o histórico completo.

Armazenamento: arquivos JSON em disco (pasta bronze/) simulando
um Data Lake local. Em produção, poderia ser S3, Azure Data Lake, etc.
"""

import json
import os
from datetime import datetime, timezone
from typing import Any


BRONZE_DIR = os.path.join(os.path.dirname(__file__), "..", "bronze")


class Bronze:
    """Persiste os dados brutos da API do IBGE na camada Bronze."""

    def __init__(self, bronze_dir: str = BRONZE_DIR) -> None:
        self.bronze_dir = bronze_dir
        os.makedirs(self.bronze_dir, exist_ok=True)

    def save(self, raw_data: dict[str, Any]) -> dict[str, str]:
        """
        Grava cada indicador como um arquivo JSON separado.

        Retorna um dicionário {indicador: caminho_arquivo} para
        a camada Silver saber onde ler.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        caminhos: dict[str, str] = {}

        for indicador, dados in raw_data.items():
            nome_arquivo = f"{indicador}_{timestamp}.json"
            caminho = os.path.join(self.bronze_dir, nome_arquivo)

            with open(caminho, "w", encoding="utf-8") as f:
                json.dump(
                    {"indicador": indicador, "extraido_em": timestamp, "dados": dados},
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

            caminhos[indicador] = caminho
            print(f"[Bronze] '{indicador}' salvo em: {caminho}")

        return caminhos

    def load_latest(self) -> dict[str, Any]:
        """
        Lê o arquivo mais recente de cada indicador da pasta Bronze.
        Útil para reprocessar sem chamar a API novamente.
        """
        arquivos: dict[str, str] = {}

        for arquivo in os.listdir(self.bronze_dir):
            if not arquivo.endswith(".json"):
                continue
            indicador = arquivo.rsplit("_", 1)[0]   # remove timestamp
            caminho   = os.path.join(self.bronze_dir, arquivo)
            # mantém o mais recente por indicador
            if indicador not in arquivos or caminho > arquivos[indicador]:
                arquivos[indicador] = caminho

        resultado: dict[str, Any] = {}
        for indicador, caminho in arquivos.items():
            with open(caminho, encoding="utf-8") as f:
                conteudo = json.load(f)
            resultado[indicador] = conteudo["dados"]
            print(f"[Bronze] Carregado: {caminho}")

        return resultado
