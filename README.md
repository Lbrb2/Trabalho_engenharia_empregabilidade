# PNAD Contínua — Pipeline ETL de Mercado de Trabalho

Projeto da disciplina de Engenharia de Dados — CESAR School (2026).

**Integrantes:**
- Anna Clara
- Fabiana Lima
- Felipe Saraiva
- Lucas Barros
- Renan Vanbasten

---

## Objetivo do projeto

Construir um pipeline ETL orientado a objetos que extrai indicadores do
mercado de trabalho brasileiro da API pública do IBGE (PNAD Contínua),
transforma e armazena os dados no MongoDB Atlas, e os disponibiliza via
servidor MCP para consulta em linguagem natural através de um chatbot.

---

## Fonte dos dados

**API de Agregados IBGE v3 — Tabela 4093 (PNAD Contínua)**

| Indicador | Variável IBGE | Descrição |
|---|---|---|
| Desocupação | 4099 | Taxa de desocupação (desemprego) |
| Informalidade | 12466 | Taxa de informalidade (sem carteira) |
| Força de trabalho | 4096 | Taxa de participação na força de trabalho |

- Cobertura: Brasil + 27 UFs
- Recorte por sexo: Total, Homens, Mulheres
- Janela temporal: últimos 6 trimestres disponíveis

---

## Arquitetura da solução

```
API IBGE v3
    │
    ▼
┌─────────────────────────────────────────────────┐
│  BRONZE  — dados brutos                         │
│  • JSON cru sem nenhuma transformação            │
│  • Salvo em bronze/<indicador>_<timestamp>.json  │
│  • Preserva histórico para reprocessamento       │
└─────────────────────────────────────────────────┘
    │
    ▼  PySpark (Silver.transform)
┌─────────────────────────────────────────────────┐
│  SILVER  — dados limpos e conformados           │
│  • Normalização do JSON aninhado da API          │
│  • Tipagem correta (float, timestamp)            │
│  • Tratamento de ausências ("...", "-")          │
│  • Deduplicação por (indicador+localidade+sexo+periodo) │
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│  GOLD  — dados prontos para consumo             │
│                                                  │
│  ┌──────────────────┐  ┌───────────────────────┐│
│  │  MongoDB Atlas   │  │       SQLite           ││
│  │  (NoSQL)         │  │    (relacional)        ││
│  │  • 6 coleções    │  │  • tabela única        ││
│  │  • Serve o MCP   │  │  • consultas SQL / BI  ││
│  └──────────────────┘  └───────────────────────┘│
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│  MCP SERVER + CHATBOT STREAMLIT                 │
│  • Consultas em linguagem natural               │
│  • Dados reais do MongoDB                       │
└─────────────────────────────────────────────────┘
```

### Justificativa dos dois bancos na Gold

| Critério | MongoDB Atlas | SQLite |
|---|---|---|
| Consulta por documento | ✅ Ideal | ✗ |
| Filtros flexíveis (MCP/chatbot) | ✅ | ✗ |
| Agregações SQL | ✗ | ✅ GROUP BY, window fn |
| BI / dashboards | ✗ | ✅ conector nativo |

---

## Estrutura do projeto

```
.
├── src/
│   ├── __init__.py
│   ├── extract.py        # Consome API IBGE v3 com retries
│   ├── bronze.py         # Camada Bronze — JSON bruto em disco
│   ├── silver.py         # Camada Silver — PySpark: limpeza e conformação
│   ├── gold.py           # Camada Gold   — MongoDB Atlas + SQLite
│   └── load.py           # Load original (mantido para compatibilidade)
├── bronze/               # Arquivos JSON brutos gerados pelo pipeline
├── gold/
│   └── pnad.db           # Banco SQLite gerado pelo pipeline
├── pipeline_flow.py      # Orquestração Prefect (medalhão completo)
├── mcp_server.py         # Servidor MCP — consultas parametrizadas ao MongoDB
├── app.py                # Chatbot Streamlit integrado ao MCP
├── requirements.txt
├── .env.example
└── .env                  # NÃO versionar — contém credenciais
```

---

## Modelagem no MongoDB

As coleções seguem a convenção `<indicador>_<nivel>`:

| Coleção | Conteúdo |
|---|---|
| `desocupacao_nacional` | Taxa de desocupação — Brasil |
| `desocupacao_estadual` | Taxa de desocupação — UFs |
| `ocupacao_informal_nacional` | Taxa de informalidade — Brasil |
| `ocupacao_informal_estadual` | Taxa de informalidade — UFs |
| `ocupacao_ativa_nacional` | Força de trabalho — Brasil |
| `ocupacao_ativa_estadual` | Força de trabalho — UFs |

**Estrutura de cada documento:**
```json
{
  "categoria":         "Mulher",
  "periodo":           "202401",
  "taxa_desocupacao":  8.3,
  "localidade":        "Pernambuco",
  "data_carga":        "2026-06-15T00:00:00"
}
```

**Estratégia de carga:** upsert idempotente por chave composta
`(periodo + localidade + categoria)` — reexecuções não geram duplicatas.

---

## Instalação

```bash
# Clone o repositório
git clone https://github.com/Lbrb2/Trabalho_engenharia_empregabilidade.git
cd Trabalho_engenharia_empregabilidade

# Instale as dependências
pip install -r requirements.txt

# Configure as variáveis de ambiente
cp .env.example .env
# Edite o .env com suas credenciais
```

---

## Como executar o ETL

```bash
python pipeline_flow.py
```

Executa o pipeline completo: Extract → Bronze → Silver → Gold.
Os dados são salvos no MongoDB Atlas e no SQLite (`gold/pnad.db`).

---

## Como executar o Prefect

O Prefect orquestra o pipeline com retries automáticos, cache e logging.

```bash
# Executar uma vez
python pipeline_flow.py

# Agendar execução trimestral automática no Prefect Cloud
prefect deploy pipeline_flow.py:etl_pnad \
    --name "ETL PNAD Trimestral" \
    --cron "0 8 1 1,4,7,10 *"
```

---

## Como executar o servidor MCP

```bash
python mcp_server.py
```

O servidor expõe três ferramentas consultáveis pelo chatbot:

| Tool | Descrição |
|---|---|
| `buscar_indicador` | Consulta desocupação, informalidade ou força de trabalho por localidade, sexo e período |
| `listar_periodos` | Lista os trimestres disponíveis no banco |
| `listar_localidades` | Lista Brasil + todas as UFs disponíveis |

---

## Como executar o chatbot

```bash
streamlit run app.py
```

Acesse `http://localhost:8501` no navegador.

**Exemplos de consulta:**
- *"Qual a taxa de desocupação das mulheres em Pernambuco no último trimestre disponível?"*
- *"Compare a informalidade entre homens e mulheres no Brasil em 2023."*
- *"Qual o estado com maior taxa de desocupação no 1º trimestre de 2024?"*

---

## Configuração — `.env`

```env
DB_USER=seu_usuario_mongodb
DB_PASSWORD=sua_senha_mongodb
MONGO_DB=projeto_ocup_desocup
OPENAI_API_KEY=sk-...
```
