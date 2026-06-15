# Atividade 2 — Engenharia de Dados · PNAD Desocupação / Ocupação
# Equipe: Anna Clara, Fabiana Lima, Felipe Saraiva, Lucas Barros, Renan Vanbasten

Pipeline ETL com **Arquitetura Medalhão** (Bronze → Silver → Gold),
**pandas** na transformação e **múltiplos bancos** na camada Gold
(MongoDB Atlas + SQLite), orquestrado com **Dagster**.

---

## Requisitos

- **Python 3.11** — obrigatório. Baixe em: https://www.python.org/downloads/release/python-3119/ → **Windows installer (64-bit)**
- Conta no **MongoDB Atlas**
- Chave de API da **OpenAI**

---

## Estrutura do projeto

```
.
├── src/
│   ├── __init__.py
│   ├── extract.py        # Consome API IBGE v3 (tabela 4093)
│   ├── bronze.py         # Camada Bronze — JSON bruto em disco
│   ├── silver.py         # Camada Silver — pandas: limpeza e transformação
│   ├── gold.py           # Camada Gold   — MongoDB Atlas + SQLite
│   └── load.py           # Load original (mantido para compatibilidade)
├── bronze/               # Arquivos JSON brutos gerados pelo pipeline
├── gold/
│   └── pnad.db           # Banco SQLite gerado pelo pipeline
├── pipeline_flow.py      # Orquestração Dagster (ponto de entrada)
├── mcp_server.py         # Servidor MCP
├── app.py                # Chatbot Streamlit
├── requirements.txt      # Dependências
└── .env                  # Variáveis de ambiente
```

---

## Arquitetura Medalhão

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
    ▼  pandas (Silver.transform)
┌─────────────────────────────────────────────────┐
│  SILVER  — dados limpos e conformados           │
│  • Normalização do JSON aninhado                 │
│  • Tipagem correta (float, timestamp)            │
│  • Tratamento de ausências ("...", "-")          │
│  • Deduplicação por (indicador+localidade+sexo+periodo)
│  • Coluna periodo_label legível ('2024 T1')      │
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│  GOLD  — dados prontos para consumo             │
│                                                  │
│  ┌──────────────────┐  ┌───────────────────────┐│
│  │  MongoDB Atlas   │  │  SQLite (relacional)  ││
│  │  • 6 coleções    │  │  • tabela única        ││
│  │  • Serve o MCP   │  │  • Consultas SQL       ││
│  │  • Chatbot       │  │  • BI / dashboards     ││
│  └──────────────────┘  └───────────────────────┘│
└─────────────────────────────────────────────────┘
```

### Justificativa dos dois bancos na Gold
| Critério           | MongoDB Atlas           | SQLite / PostgreSQL       |
|--------------------|-------------------------|---------------------------|
| Consulta por doc   | ✅ Ideal                | ✗ Menos natural           |
| Filtros flexíveis  | ✅ (MCP/chatbot)        | ✗                         |
| Agregações SQL     | ✗ Verboso               | ✅ GROUP BY, window fn    |
| BI / dashboards    | ✗                       | ✅ Conector nativo         |
| Idempotência       | ✅ delete + insert      | ✅ UNIQUE constraint       |

---

## Configuração

### Arquivo `.env`
```env
DB_USER=seu_usuario_mongo
DB_PASSWORD=sua_senha_mongo
MONGO_DB=projeto_ocup_desocup
OPENAI_API_KEY=sk-...
```

> ⚠️ Salve o `.env` com encoding **ASCII** para evitar problemas de leitura no Windows:
> ```bash
> "DB_USER=...`nDB_PASSWORD=...`nMONGO_DB=...`nOPENAI_API_KEY=..." | Out-File -FilePath .env -Encoding ascii -NoNewline
> ```

### Instalação
```bash
# Criar ambiente virtual com Python 3.11
py -3.11 -m venv .lbvenv

# Ativar (Windows)
.lbvenv\Scripts\activate

# Instalar dependências
pip install -r requirements.txt
```

### ⚠️ Problemas conhecidos no Windows

**greenlet — DLL load failed**
```bash
pip install greenlet==3.0.3
```

**griffe — ModuleNotFoundError**
```bash
pip install griffe==0.48.0
```

---

## Executando

### Pipeline ETL (popula o banco)
```bash
python pipeline_flow.py
```

### UI do Dagster (histórico de execuções e agendamento)
```bash
dagster dev -f pipeline_flow.py
```

### Chatbot
```bash
streamlit run app.py
```

---

## Orquestração com Dagster

| Componente        | Equivalente Prefect | Descrição                              |
|-------------------|---------------------|----------------------------------------|
| `@op`             | `@task`             | Unidade de trabalho com retries        |
| `@job`            | `@flow`             | Pipeline com dependências explícitas   |
| `@schedule`       | `--cron`            | Agendamento trimestral automático      |
| `dagster dev`     | Prefect UI          | Painel web com logs e histórico        |

Agendamento configurado: todo 1º dia de janeiro, abril, julho e outubro às 8h
(`0 8 1 1,4,7,10 *`) — acompanhando o calendário de divulgação do IBGE.

---

## Ferramentas MCP disponíveis

| Ferramenta           | Parâmetros principais                        | Descrição                         |
|----------------------|----------------------------------------------|-----------------------------------|
| `buscar_indicador`   | `indicador`, `localidade`, `sexo`, `periodo` | Consulta parametrizada no MongoDB |
| `listar_periodos`    | `indicador`, `nacional`                      | Trimestres disponíveis            |
| `listar_localidades` | `indicador`                                  | UFs e Brasil disponíveis          |

### Exemplos de perguntas para o chatbot
- *"Qual a taxa de desocupação das mulheres em Pernambuco no último trimestre?"*
- *"Compare o desemprego entre homens e mulheres no Brasil."*
- *"Qual estado tem maior informalidade?"*
- *"Como evoluiu a força de trabalho em São Paulo?"*
