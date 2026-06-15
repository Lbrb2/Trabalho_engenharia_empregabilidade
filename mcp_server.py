"""
mcp_server.py
-------------
Servidor MCP que expõe os indicadores da PNAD Contínua (IBGE)
armazenados no MongoDB Atlas como ferramentas consultáveis por um
cliente de IA (ex.: Claude, GPT-4o-mini).

Coleções no MongoDB (conforme load.py):
  desocupacao_nacional      / desocupacao_estadual
  ocupacao_informal_nacional / ocupacao_informal_estadual
  ocupacao_ativa_nacional   / ocupacao_ativa_estadual

Campos de cada documento:
  categoria   – sexo: 'Homem', 'Mulher', 'Total'
  periodo     – '202401', '202402', ...  (YYYYTT onde TT=01..04)
  localidade  – 'Brasil', 'Pernambuco', 'São Paulo', ...
  taxa_desocupacao | taxa_informalidade | taxa_participacao  (float | None)
"""

import os
import re
from contextlib import contextmanager

from fastmcp import FastMCP
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# ── Configuração ───────────────────────────────────────────────────────────────
_DB_USER     = os.getenv("DB_USER")
_DB_PASSWORD = os.getenv("DB_PASSWORD")
_DB_NAME     = os.getenv("MONGO_DB", "projeto_ocup_desocup")
_URI = (
    f"mongodb+srv://{_DB_USER}:{_DB_PASSWORD}"
    "@clusterbob.jilronv.mongodb.net/?appName=ClusterBob"
)

_INDICADORES = {
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


# ── Helpers ────────────────────────────────────────────────────────────────────

@contextmanager
def _get_db():
    """
    Context manager que abre e fecha a conexão MongoDB corretamente.

    CORREÇÃO BUG 1: a versão anterior criava MongoClient mas nunca
    chamava client.close(), esgotando o pool após algumas queries.
    """
    client = MongoClient(_URI, serverSelectionTimeoutMS=10000)
    try:
        yield client[_DB_NAME]
    finally:
        client.close()


def _normalizar_periodo(periodo: str) -> str | None:
    """
    Converte qualquer formato de período para o código interno do banco (YYYYTT).

    CORREÇÃO BUG 2 + BUG 3:
      - Bug 2: '2024 T1' → .replace(" T","") → '20241' (5 dígitos).
        O banco armazena '202401' (6 dígitos com zero). O regex nunca batia.
      - Bug 3: listar_periodos devolve labels '2024 T1', mas buscar_indicador
        não conseguia converter esse formato de volta para o código do banco.

    Formatos aceitos → saída:
      '202401'   → '202401'   (já correto)
      '2024 T1'  → '202401'   (label do listar_periodos)
      '2024T1'   → '202401'
      '2024-T1'  → '202401'
      '20241'    → '202401'   (5 dígitos: trimestre sem zero)
    """
    if not periodo:
        return None

    p = periodo.strip()

    # Já no formato correto: 6 dígitos exatos (YYYYTT)
    if re.fullmatch(r"\d{6}", p):
        return p

    # Formatos com separador: '2024 T1', '2024T1', '2024-T1', '2024_T1'
    m = re.fullmatch(r"(\d{4})\s*[-_ ]?\s*[Tt](\d{1,2})", p)
    if m:
        ano  = m.group(1)
        trim = m.group(2).zfill(2)   # '1' → '01'
        return f"{ano}{trim}"

    # 5 dígitos sem zero: '20241' → '202401'
    m = re.fullmatch(r"(\d{4})(\d)", p)
    if m:
        return f"{m.group(1)}0{m.group(2)}"

    return None  # formato desconhecido — não aplica filtro de período


def _periodo_para_label(p: str) -> str:
    """'202401' → '2024 T1'"""
    if len(p) == 6 and p.isdigit():
        trim = p[4:].lstrip("0") or "0"
        return f"{p[:4]} T{trim}"
    return p


def _clean(doc: dict) -> dict:
    doc.pop("_id", None)
    return doc


# ── Servidor MCP ───────────────────────────────────────────────────────────────
mcp = FastMCP("pnad-desocupacao")


@mcp.tool()
def buscar_indicador(
    indicador: str,
    localidade: str | None = None,
    sexo: str | None = None,
    periodo: str | None = None,
    limite: int = 20,
) -> list[dict]:
    """
    Consulta um indicador da PNAD Contínua no MongoDB Atlas.

    Parâmetros
    ----------
    indicador : str
        'desocupacao', 'informalidade' ou 'forca_trabalho'.
    localidade : str, opcional
        Nome da UF (ex.: 'Pernambuco', 'São Paulo') ou 'Brasil'.
        Busca parcial, sem distinção de maiúsculas/minúsculas.
    sexo : str, opcional
        'Homem', 'Mulher' ou 'Total'.
    periodo : str, opcional
        Aceita vários formatos — todos equivalentes para o 1º trim. de 2024:
          '202401'  (formato interno do banco)
          '2024 T1' (formato retornado por listar_periodos)
          '2024T1'
    limite : int, opcional
        Máximo de registros retornados (padrão: 20).

    Exemplos
    --------
    → indicador='desocupacao', localidade='Pernambuco', periodo='2024 T1'
    → indicador='informalidade', localidade='Brasil', periodo='202304'
    """
    indicador = indicador.lower().strip()
    if indicador not in _INDICADORES:
        return [{"erro": f"Indicador inválido. Use: {list(_INDICADORES.keys())}"}]

    cfg = _INDICADORES[indicador]

    # Normaliza o período para o formato exato do banco (YYYYTT)
    periodo_normalizado = _normalizar_periodo(periodo) if periodo else None

    # Decide quais coleções consultar
    if localidade is None:
        colecoes = [cfg["col_nacional"], cfg["col_estadual"]]
    elif localidade.strip().lower() == "brasil":
        colecoes = [cfg["col_nacional"]]
    else:
        colecoes = [cfg["col_estadual"]]

    resultados = []

    with _get_db() as db:
        for nome_col in colecoes:
            col    = db[nome_col]
            filtro: dict = {}

            if localidade and localidade.strip().lower() != "brasil":
                filtro["localidade"] = {"$regex": localidade.strip(), "$options": "i"}

            if sexo:
                filtro["categoria"] = {"$regex": sexo.strip(), "$options": "i"}

            if periodo_normalizado:
                # Match exato — mais rápido e sem ambiguidade
                filtro["periodo"] = periodo_normalizado
            elif periodo:
                # Período informado mas formato desconhecido → regex como fallback
                filtro["periodo"] = {"$regex": periodo.strip(), "$options": "i"}

            docs = [
                _clean(d)
                for d in col.find(filtro).sort("periodo", -1).limit(limite)
            ]
            for d in docs:
                d["periodo_label"] = _periodo_para_label(d.get("periodo", ""))
            resultados.extend(docs)

    if not resultados:
        # Informa ao modelo quais períodos existem para ele poder tentar novamente
        periodos_disponiveis = listar_periodos(
            indicador=indicador,
            nacional=(localidade is None or localidade.lower() == "brasil"),
        )
        return [{
            "mensagem": (
                "Nenhum dado encontrado para os filtros informados. "
                f"Períodos disponíveis no banco: {periodos_disponiveis}. "
                "Tente novamente usando um dos períodos listados."
            )
        }]

    return resultados[:limite]


@mcp.tool()
def listar_periodos(
    indicador: str = "desocupacao",
    nacional: bool = True,
) -> list[str]:
    """
    Lista os trimestres disponíveis no banco para o indicador informado.

    Retorna períodos no formato legível '2024 T1' (mais recente primeiro).
    Passe esses valores diretamente para o parâmetro `periodo` de
    buscar_indicador — o formato é aceito diretamente.

    Parâmetros
    ----------
    indicador : str
        'desocupacao', 'informalidade' ou 'forca_trabalho'.
    nacional : bool
        True para períodos do Brasil, False para períodos das UFs.
    """
    indicador = indicador.lower().strip()
    if indicador not in _INDICADORES:
        return [f"Indicador inválido. Use: {list(_INDICADORES.keys())}"]

    cfg     = _INDICADORES[indicador]
    col_key = "col_nacional" if nacional else "col_estadual"

    with _get_db() as db:
        periodos = db[cfg[col_key]].distinct("periodo")

    return sorted([_periodo_para_label(p) for p in periodos], reverse=True)


@mcp.tool()
def listar_localidades(indicador: str = "desocupacao") -> list[str]:
    """
    Lista todas as localidades (UFs + Brasil) disponíveis no banco.

    Parâmetros
    ----------
    indicador : str
        'desocupacao', 'informalidade' ou 'forca_trabalho'.
    """
    indicador = indicador.lower().strip()
    if indicador not in _INDICADORES:
        return [f"Indicador inválido. Use: {list(_INDICADORES.keys())}"]

    cfg         = _INDICADORES[indicador]
    localidades: set = set()

    with _get_db() as db:
        for col_key in ["col_nacional", "col_estadual"]:
            localidades.update(db[cfg[col_key]].distinct("localidade"))

    return sorted(localidades)


# ── Entry‑point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mcp.run()
