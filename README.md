# PNAD Contínua — Pipeline ETL de Mercado de Trabalho
**Engenharia de Dados · CESAR School 2026**
**Equipe:** Anna Clara · Fabiana Lima · Felipe Saraiva · Lucas Barros · Renan Vanbasten

Pipeline ETL com **Arquitetura Medalhão** (Bronze → Silver → Gold),
**pandas** na transformação e **múltiplos bancos** na camada Gold
(MongoDB Atlas + SQLite), orquestrado com **Dagster**.

---

## Objetivo do projeto

Construir um pipeline ETL orientado a objetos que extrai indicadores do
mercado de trabalho brasileiro da API pública do IBGE (PNAD Contínua),
transforma e armazena os dados no MongoDB Atlas, e os disponibiliza via
servidor MCP para consulta em linguagem natural através de um chatbot.

---

## Fonte dos dados

**API de Agregados IBGE v3 — Tabela 4093 (PNAD Contínua)**

| Indicador         | Variável IBGE | Descrição                                     |
| ----------------- | ------------- | --------------------------------------------- |
| Desocupação       | 4099          | Taxa de desocupação (%)                       |
| Informalidade     | 12466         | Taxa de informalidade (%)                     |
| Força de trabalho | 4096          | Taxa de participação na força de trabalho (%) |

- Cobertura: Brasil + 27 UFs
- Recorte por sexo: Total, Homens, Mulheres
- Janela temporal: últimos 6 trimestres disponíveis

---

## Estrutura do projeto

```
.
├── src/
│   ├── __init__.py
│   ├── extract.py        # Consome API IBGE v3 (tabela 4093)
│   ├── bronze.py         # Camada Bronze — JSON bruto em disco
│   ├── silver.py         # Camada Silver — pandas: limpeza e transformação
│   └── gold.py           # Camada Gold   — MongoDB Atlas + SQLite
├── bronze/               # Arquivos JSON brutos gerados pelo pipeline
├── gold/
│   └── pnad.db           # Banco SQLite gerado pelo pipeline
├── pipeline_flow.py      # Orquestração Dagster (ponto de entrada do ETL)
├── mcp_server.py         # Servidor MCP — consultas ao MongoDB
├── app.py                # Chatbot Streamlit integrado ao MCP
├── requirements.txt
├── .env.example
└── .env                  # NÃO versionar — contém credenciais
```

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
    ▼  pandas (Silver.transform)
┌─────────────────────────────────────────────────┐
│  SILVER  — dados limpos e conformados           │
│  • Normalização do JSON aninhado da API          │
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
    │
    ▼
┌─────────────────────────────────────────────────┐
│  MCP SERVER + CHATBOT STREAMLIT                 │
│  • Consultas em linguagem natural               │
│  • Dados reais do MongoDB                       │
└─────────────────────────────────────────────────┘
```

### Justificativa dos dois bancos na Gold

| Critério                        | MongoDB Atlas            | SQLite                |
| ------------------------------- | ------------------------ | --------------------- |
| Consulta por documento          | ✅ Ideal                  | ✗                     |
| Filtros flexíveis (MCP/chatbot) | ✅                        | ✗                     |
| Agregações SQL                  | ✗                        | ✅ GROUP BY, window fn |
| BI / dashboards                 | ✗                        | ✅ conector nativo     |
| Idempotência                    | ✅ upsert por ReplaceOne  | ✅ UNIQUE constraint   |

---

## Modelagem no MongoDB

As coleções seguem a convenção `<indicador>_<nivel>`:

| Coleção                      | Conteúdo                       |
| ---------------------------- | ------------------------------ |
| `desocupacao_nacional`       | Taxa de desocupação — Brasil   |
| `desocupacao_estadual`       | Taxa de desocupação — UFs      |
| `ocupacao_informal_nacional` | Taxa de informalidade — Brasil |
| `ocupacao_informal_estadual` | Taxa de informalidade — UFs    |
| `ocupacao_ativa_nacional`    | Força de trabalho — Brasil     |
| `ocupacao_ativa_estadual`    | Força de trabalho — UFs        |

**Estrutura de cada documento:**
```json
{
  "categoria":         "Mulher",
  "periodo":           "202401",
  "localidade":        "Pernambuco",
  "taxa_desocupacao":  8.3,
  "data_carga":        "2026-06-15T00:00:00+00:00"
}
```

**Chave de upsert:** `(periodo + localidade + categoria)` — reexecuções atualizam sem duplicar.
**Índice único** criado automaticamente nessa mesma chave em cada coleção.

---

## Requisitos

- **Python 3.11** — obrigatório. Baixe em: https://www.python.org/downloads/release/python-3119/
- Conta no **MongoDB Atlas** com cluster ativo
- Chave de API da **OpenAI**

---

## Instalação

```bash
# Clone o repositório
git clone https://github.com/Lbrb2/Trabalho_engenharia_empregabilidade.git
cd Trabalho_engenharia_empregabilidade

# Crie o ambiente virtual com Python 3.11
py -3.11 -m venv .lbvenv

# Ative o ambiente (Windows)
.lbvenv\Scripts\activate

# Instale as dependências
pip install -r requirements.txt

# Configure as variáveis de ambiente
cp .env.example .env
# Edite o .env com suas credenciais
```

### Arquivo `.env`
```env
DB_USER=seu_usuario_mongo
DB_PASSWORD=sua_senha_mongo
MONGO_DB=projeto_ocup_desocup
OPENAI_API_KEY=sk-...
```

> ⚠️ No Windows, salve o `.env` com encoding **ASCII**:
> ```powershell
> "DB_USER=...`nDB_PASSWORD=...`nMONGO_DB=...`nOPENAI_API_KEY=..." | Out-File -FilePath .env -Encoding ascii -NoNewline
> ```

### ⚠️ Problemas conhecidos no Windows

**`greenlet` — DLL load failed**
```bash
pip install greenlet==3.0.3
```

**`griffe` — ModuleNotFoundError**
```bash
pip install griffe==0.48.0
```

Ambos já estão fixados no `requirements.txt` e são instalados automaticamente.

---

## Como executar

Os três serviços são independentes e devem rodar em **terminais separados** com o mesmo venv ativo.

### 1. Pipeline ETL (popula o MongoDB e o SQLite)
```bash
python pipeline_flow.py
```
Executa o pipeline completo: Extract → Bronze → Silver → Gold.
Pode ser reexecutado a qualquer momento — a carga é idempotente.

### 2. UI do Dagster (histórico de execuções e agendamento)
```bash
dagster dev -f pipeline_flow.py
```
Acesse `http://localhost:3000`.

### 3. Servidor MCP (opcional — para uso direto pelo cliente MCP)
```bash
python mcp_server.py
```

### 4. Chatbot Streamlit
```bash
streamlit run app.py
```
Acesse `http://localhost:8501` no navegador.

---

## Orquestração com Dagster

| Componente  | Descrição                                      |
| ----------- | ---------------------------------------------- |
| `@op`       | Unidade de trabalho com retries e delay        |
| `@job`      | Pipeline com dependências explícitas entre ops |
| `@schedule` | Agendamento trimestral automático              |

**Agendamento configurado:** todo 1º dia de janeiro, abril, julho e outubro às 8h
(`0 8 1 1,4,7,10 *`) — acompanhando o calendário de divulgação do IBGE.

---

## Ferramentas MCP disponíveis

| Ferramenta           | Parâmetros principais                        | Descrição                         |
| -------------------- | -------------------------------------------- | --------------------------------- |
| `buscar_indicador`   | `indicador`, `localidade`, `sexo`, `periodo` | Consulta parametrizada no MongoDB |
| `listar_periodos`    | `indicador`, `nacional`                      | Trimestres disponíveis            |
| `listar_localidades` | `indicador`                                  | UFs e Brasil disponíveis          |

### Exemplos de perguntas para o chatbot
- *"Qual a taxa de desocupação das mulheres em Pernambuco no último trimestre disponível?"*
- *"Compare a informalidade entre homens e mulheres no Brasil em 2023."*
- *"Qual o estado com maior taxa de desocupação no 1º trimestre de 2024?"*
- *"Como evoluiu a força de trabalho em São Paulo?"*
