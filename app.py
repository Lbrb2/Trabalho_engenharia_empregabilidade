"""
app.py
------
Chatbot Streamlit integrado ao mcp_server.py.
Chama as ferramentas diretamente como funções Python
(sem dependência do Client do fastmcp).
"""

import json
import os
import streamlit as st
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# Importa as ferramentas diretamente do mcp_server
from mcp_server import buscar_indicador, listar_periodos, listar_localidades

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ── Definição das ferramentas para o OpenAI ────────────────────────────────────
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "buscar_indicador",
            "description": "Consulta um indicador da PNAD Contínua no MongoDB Atlas.",
            "parameters": {
                "type": "object",
                "properties": {
                    "indicador": {
                        "type": "string",
                        "description": "'desocupacao', 'informalidade' ou 'forca_trabalho'"
                    },
                    "localidade": {
                        "type": "string",
                        "description": "Nome da UF (ex: 'Pernambuco') ou 'Brasil'"
                    },
                    "sexo": {
                        "type": "string",
                        "description": "'Homem', 'Mulher' ou 'Total'"
                    },
                    "periodo": {
                        "type": "string",
                        "description": "Formato YYYYQQ (ex: '202401') ou 'YYYY TN' (ex: '2024 T1')"
                    },
                    "limite": {
                        "type": "integer",
                        "description": "Máximo de registros (padrão: 20)"
                    }
                },
                "required": ["indicador"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "listar_periodos",
            "description": "Lista os trimestres disponíveis no banco para o indicador informado.",
            "parameters": {
                "type": "object",
                "properties": {
                    "indicador": {
                        "type": "string",
                        "description": "'desocupacao', 'informalidade' ou 'forca_trabalho'"
                    },
                    "nacional": {
                        "type": "boolean",
                        "description": "True para Brasil, False para UFs"
                    }
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "listar_localidades",
            "description": "Lista todas as localidades (UFs + Brasil) disponíveis no banco.",
            "parameters": {
                "type": "object",
                "properties": {
                    "indicador": {
                        "type": "string",
                        "description": "'desocupacao', 'informalidade' ou 'forca_trabalho'"
                    }
                },
                "required": []
            }
        }
    }
]

# Mapa nome → função
TOOL_MAP = {
    "buscar_indicador":   buscar_indicador,
    "listar_periodos":    listar_periodos,
    "listar_localidades": listar_localidades,
}

# ── System Prompt ──────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """
Você é um analista especialista em mercado de trabalho brasileiro, com acesso
direto aos dados da PNAD Contínua (IBGE) armazenados no MongoDB Atlas.
Responda SEMPRE com base nos dados reais retornados pelas ferramentas.
Nunca invente ou estime valores — se não houver dado, diga claramente.

## Indicadores disponíveis
| Chave           | Sinônimos aceitos                                     | Unidade     |
|-----------------|-------------------------------------------------------|-------------|
| desocupacao     | desemprego, desocupados, sem trabalho                 | %           |
| informalidade   | informal, trabalho informal, sem carteira assinada    | %           |
| forca_trabalho  | força de trabalho, participação, PEA                  | %           |

## Localidades disponíveis
Brasil e todos os 26 estados + Distrito Federal.
Converta siglas: SP→São Paulo, RJ→Rio de Janeiro, PE→Pernambuco, etc.

## Regras
1. "Último trimestre" → chame listar_periodos primeiro e use o primeiro item.
2. Para comparações → chame buscar_indicador sem filtrar sexo ou localidade.
3. Nunca assuma que o banco não tem o dado — sempre tente a ferramenta primeiro.
4. SEMPRE converta períodos para o formato 'YYYY TN' antes de chamar buscar_indicador.
   Exemplos obrigatórios:
     'primeiro trimestre de 2024' → '2024 T1'
     '2º trimestre de 2023'       → '2023 T2'
     '4º tri de 2023'             → '2023 T4'
     'terceiro trimestre de 2024' → '2024 T3'

## Formato das respostas
- Cite sempre o período e a localidade dos dados apresentados.
- Taxas: 1 casa decimal + símbolo % (ex.: 8,3%).
- Se valor for null, informe que o IBGE não divulgou o dado naquele período.
""".strip()


# ── Chat ───────────────────────────────────────────────────────────────────────
def chat(pergunta: str) -> str:
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": pergunta},
    ]

    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        tools=TOOLS,
    )
    msg = response.choices[0].message
    messages.append(msg)

    while msg.tool_calls:
        for tc in msg.tool_calls:
            func = TOOL_MAP.get(tc.function.name)
            if func:
                args = json.loads(tc.function.arguments)
                resultado = func(**args)
            else:
                resultado = {"erro": f"Ferramenta '{tc.function.name}' não encontrada."}

            messages.append({
                "role":         "tool",
                "tool_call_id": tc.id,
                "content":      json.dumps(resultado, ensure_ascii=False),
            })

        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=TOOLS,
        )
        msg = response.choices[0].message
        messages.append(msg)

    return msg.content or "Não foi possível gerar uma resposta."


# ── Interface Streamlit ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PNAD Chatbot",
    page_icon="📊",
    layout="centered",
)

st.title("📊 PNAD Contínua — Mercado de Trabalho")
st.caption(
    "Chatbot integrado ao MongoDB via MCP  ·  "
    "Dados: IBGE / PNAD Contínua  ·  "
    "Indicadores: Desocupação · Informalidade · Força de Trabalho"
)

if "historico" not in st.session_state:
    st.session_state.historico = []

for entry in st.session_state.historico:
    role  = "user" if entry["role"] == "user" else "assistant"
    label = "Você" if role == "user" else "Analista PNAD"
    with st.chat_message(role):
        st.markdown(f"**{label}:** {entry['content']}")

pergunta = st.chat_input(
    "Pergunte sobre desocupação, informalidade ou força de trabalho no Brasil..."
)

if pergunta:
    with st.chat_message("user"):
        st.markdown(f"**Você:** {pergunta}")

    with st.chat_message("assistant"):
        with st.spinner("Consultando dados no MongoDB..."):
            try:
                resposta = chat(pergunta)
            except Exception as e:
                resposta = f"❌ Erro ao consultar os dados: {e}"
        st.markdown(f"**Analista PNAD:** {resposta}")

    st.session_state.historico.append({"role": "user",      "content": pergunta})
    st.session_state.historico.append({"role": "assistant", "content": resposta})

if st.session_state.historico:
    if st.button("🗑️ Limpar conversa"):
        st.session_state.historico = []
        st.rerun()
