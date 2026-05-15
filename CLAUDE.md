# CLAUDE.md

Guia para o Claude Code ao trabalhar neste repositório.

---

## Visão Geral do Projeto

**lol-analytics-platform** é um projeto portfolio-grade de data engineering que ingere dados de partidas ranqueadas de League of Legends a partir da Riot Games API, transforma os dados através de uma medallion architecture (Bronze → Silver → Gold) sobre Databricks + Delta Lake, e expõe meta-game analytics para análise de balanceamento de campeões.

**Audiência deste código:** recrutadores técnicos e gestores de engenharia avaliando candidatos a vagas de data engineering mid-senior. Qualidade de código, decisões arquiteturais e documentação importam tanto quanto funcionalidade.

**Pergunta de negócio que guia o design:** *Quais campeões estão atualmente overpowered e como a meta evoluiu nos patches recentes?*

**Escopo (Fase 1 — MVP):** regiões BR1 + KR, tier Master+, últimos 3 patches.
**Escopo (Fase 2 — planejado):** 4 regiões (BR1 + KR + NA1 + EUW1), todos os tiers com amostragem estratificada. Não implementar trabalho da Fase 2 até a Fase 1 estar completamente entregue.

---

## Tech Stack (decisões travadas, não propor alternativas sem pedido explícito)

- **Linguagem:** Python 3.11+
- **Dependency manager:** `uv` (não Poetry, não pip-tools)
- **HTTP client:** `httpx` (async) — escolhido sobre `requests`
- **Retry:** `tenacity`
- **Logging:** `structlog` com renderer JSON
- **Config:** `pydantic-settings` lendo `.env`
- **Compute:** Databricks Free Edition (single-node) — para workloads Spark
- **Storage:** Delta Lake em Unity Catalog volumes
- **Transform:** PySpark 3.5+
- **Orchestration:** Databricks Workflows (não introduzir Airflow)
- **Data quality framework:** custom lightweight checks (não puxar Great Expectations)
- **Dashboard:** Databricks SQL native (não introduzir Streamlit/Metabase ainda)
- **Testing:** `pytest` + `pytest-asyncio` (auto mode) + `respx` (httpx mocking) + `chispa` (PySpark assertions)
- **Lint/format:** `ruff` (substitui black + flake8 + isort)
- **Type checking:** `mypy` (strict mode no `pyproject.toml`, **enforçado no CI sem `continue-on-error`**)

---

## Práticas Databricks/Delta modernas (2025+)

Adote estas práticas em todo DDL e código novo. Justificam-se pela documentação oficial atual da Databricks e demonstram conhecimento de plataforma além do básico.

- **Unity Catalog three-level namespacing:** sempre `catalog.schema.table` (e.g. `lol_analytics.bronze.raw_matches`). Nunca usar `hive_metastore` ou tabelas no default schema.
- **Liquid Clustering em vez de `PARTITIONED BY`:** chaves de cluster são flexíveis (podem mudar sem reescrita), compatíveis com padrões de acesso variados, e oficialmente recomendadas para novas tabelas. Não misturar `CLUSTER BY` com `PARTITIONED BY` ou `ZORDER` — são mutuamente exclusivos. **Escolha de chaves:** ao contrário de partitioning, Liquid Clustering foi desenhada para se beneficiar de colunas de alta cardinalidade quando são alvo frequente de filtros/MERGE (ver doc Databricks). Para tabelas novas, considerar `CLUSTER BY AUTO` (deixar a Databricks escolher com base em padrões de query) quando disponível.
- **Column Mapping em modo `name`:** habilita rename/drop de colunas sem reescrita de arquivos e suporta caracteres especiais. Requer `delta.minReaderVersion = 2` e `delta.minWriterVersion = 5`.
- **Deletion Vectors (`delta.enableDeletionVectors = 'true'`):** acelera MERGE/UPDATE/DELETE drasticamente. Como Bronze depende de MERGE INTO para idempotência, é essencial.
- **Change Data Feed (`delta.enableChangeDataFeed = 'true'`):** habilita consumo incremental — Silver vai ler apenas as linhas novas/alteradas do Bronze em vez de full scan.
- **Generated columns:** derivar `ingestion_date` (e similares) de `ingestion_timestamp` via `GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE))`. Evita populá-las manualmente e elimina classe inteira de bugs.
- **Predictive Optimization:** ativar no catálogo via `ALTER CATALOG lol_analytics ENABLE PREDICTIVE OPTIMIZATION`. Substitui ajuste manual de `OPTIMIZE` / `VACUUM` e dispensa as flags `autoOptimize.optimizeWrite` / `autoOptimize.autoCompact` em managed tables sob Unity Catalog.
- **Managed tables por padrão:** confiar no Unity Catalog para localização física quando possível. Evitar `LOCATION '...'` em DDL. **Caveat Free Edition:** validar antes do Sprint 2 que managed tables funcionam no workspace; caso contrário, documentar o fallback (external tables com path em DBFS) em ADR específico.
- **Cost-aware compute:** jobs agendados rodam em job clusters (não all-purpose). Single-node SKU onde possível. Documentar trade-off de custo em qualquer mudança que afete cluster size, runtime ou frequência de jobs.

---

## Comandos Comuns

Todos os comandos rodam a partir da raiz do repo. O virtual environment é gerenciado pelo `uv` — nunca chamar `python` diretamente; sempre `uv run`.

### Setup
```bash
uv sync --extra dev          # instala todas as deps incluindo dev tools
cp .env.example .env         # editar .env com RIOT_API_KEY
```

### Test
```bash
uv run pytest --no-cov                       # unit tests rápidos
uv run pytest -v                             # com coverage
uv run pytest -m "not spark"                 # exclui testes Spark
uv run pytest tests/unit/test_rate_limiter.py -v   # arquivo único
uv run pytest -k "test_name_substring" -v    # filtrar por nome
```

### Lint / format
```bash
uv run ruff check src tests                  # lint
uv run ruff check --fix src tests            # autofix
uv run ruff format src tests                 # format
```

### Type check
```bash
uv run mypy src                              # strict mode; passa zero erros e CI enforça
```

### Run modules
```bash
uv run python -m lol_analytics.ingestion.smoke_test    # valida API key + rate limiter
uv run lol-ingest smoke-test                            # mesmo, via CLI
```

### Git workflow
- Commits seguem Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`
- Branch a partir de `main`, PR de volta pra `main`
- Referenciar o número do sprint em mensagens de commit quando aplicável: `feat(sprint-2): incremental bronze ingestion`
- Mensagens de commit em **inglês** (convenção do repo, mesmo que a conversa com o agent seja em português)

---

## Arquitetura

```
Riot API (rate-limited)
    │
    ▼
Bronze (raw JSON em Delta, clustered por (ingestion_date, match_id),
        append-only com MERGE para idempotência)
    │ PySpark transforms (incremental via CDF)
    ▼
Silver (modelo dimensional: dim_champion SCD2, dim_patch, dim_summoner,
        fact_match_participant, fact_match_event)
    │ agregações de negócio
    ▼
Gold (agg_champion_patch_elo, agg_champion_synergy, agg_meta_evolution;
      Liquid Clustered)
    │
    ▼
Databricks SQL Dashboard + 10 queries analíticas
```

Decisões arquiteturais são documentadas como ADRs em `docs/adr/`. Sempre ler os ADRs existentes antes de sugerir mudanças estruturais. Ao tomar uma nova decisão estrutural, escrever um novo ADR seguindo o formato estabelecido (Status, Context, Decision, Alternatives, Consequences).

---

## Layout do Repositório

```
src/lol_analytics/
├── ingestion/      # Riot API client, rate limiter, CLI entrypoints
├── bronze/         # transforms de landing raw
├── silver/         # transforms do modelo dimensional
├── gold/           # agregações de negócio
└── utils/          # config, logging, helpers compartilhados

sql/
├── ddl/            # statements CREATE TABLE (um arquivo por camada)
└── analyses/       # as 10 queries de portfolio (Sprint 4)

tests/
└── unit/           # testes pytest, espelhando estrutura de src/

docs/
├── architecture.md      # diagrama de sistema + narrativa
├── data_dictionary.md   # toda tabela, toda coluna, todo grão
├── setup/               # runbooks de provisionamento
│   └── databricks_workspace.md   # passos para ativar UC, Predictive Opt, etc.
└── adr/                 # Architecture Decision Records numerados
```

---

## Convenções de Código

### Estilo Python
- **Comprimento de linha:** 100 chars (configurado no ruff).
- **Imports:** apenas absolutos (`from lol_analytics.utils.config import ...`), nunca relativos (`from ..utils import ...`).
- **Type hints:** obrigatórios em todas as funções e métodos públicos. Usar `from __future__ import annotations` no topo de cada módulo pra habilitar sintaxe PEP 604 (`str | None`) no 3.11.
- **Docstrings:** obrigatórias em funções, classes e módulos públicos. Seguir estilo Google — linha de resumo curta, linha em branco, depois seções `Args:`, `Returns:`, `Raises:` quando relevantes. Em **inglês** (convenção do repo).
- **Async:** a camada de ingestion é async (`httpx.AsyncClient`). Transforms PySpark são síncronos. Não misturar.
- **Error handling:** nunca `except Exception` sem re-raising ou logging com contexto completo. Distinguir exceções retryable de não-retryable explicitamente (ver padrão em `riot_client.py`).

### Naming
- Módulos: `snake_case.py`
- Classes: `PascalCase`
- Funções/variáveis: `snake_case`
- Constantes: `UPPER_SNAKE_CASE`
- Helpers privados: `_leading_underscore`
- Arquivos de teste: `test_<module_under_test>.py`, espelhando layout sob `tests/unit/`

### Logging
- Sempre usar `structlog` via `get_logger(__name__)`, nunca `print()` ou stdlib `logging` direto.
- Logar eventos como substantivos (`rate_limited`, `match_loaded`, `ingestion_started`), não verbos.
- Adicionar campos estruturados, nunca f-strings dentro da mensagem:
  - Bom: `log.info("match_loaded", match_id=mid, duration_s=dur)`
  - Ruim: `log.info(f"loaded match {mid} in {dur}s")`

### Tests
- Todo módulo novo vem com unit tests. PRs sem testes para lógica nova não passam.
- Testes devem ser determinísticos. Sem chamadas reais de rede — usar `respx` para httpx, mocks para Spark.
- Usar fixtures pytest (em `conftest.py`) para setup compartilhado. Não repetir lógica de construção.
- Testes async: graças a `asyncio_mode = "auto"`, não é necessário `@pytest.mark.asyncio` em todo teste.
- **Validação contra Databricks real é parte do "Definition of Done" de cada sprint que toca Delta.** Mocks são necessários mas não suficientes — MERGE, CDF, Liquid Clustering só se validam no workspace de verdade. Antes de marcar um sprint como concluído, rodar a ingestion/transformação contra um dataset pequeno (50-100 partidas) no Free Edition.

### SQL
- Arquivos DDL em `sql/ddl/` são numerados por camada: `01_bronze.sql`, `02_silver.sql`, `03_gold.sql`.
- Todo `CREATE TABLE` tem um bloco de comentário explicando grão, clustering e linhagem.
- Queries analíticas em `sql/analyses/` são numeradas (`01_top_champions_winrate.sql`) e têm comentário de header com a pergunta de negócio, o grão do resultado e quaisquer caveats.
- Usar CTEs liberalmente; evitar subqueries aninhadas mais profundas que dois níveis.
- DDL deve seguir as práticas Databricks modernas listadas na seção "Práticas Databricks/Delta modernas" acima.

---

## Riot API — Coisas Críticas para Saber

### Routing (uma idiossincrasia da Riot que pega todo mundo)
- **Platform endpoints** (por shard): `BR1`, `KR`, `NA1`, `EUW1`, etc. Usados em `summoner-v4`, `league-v4`, `champion-v3`.
- **Regional endpoints** (super-regiões): `americas`, `europe`, `asia`, `sea`. Usados em `match-v5`.
- O mapeamento vive em `riot_client.PLATFORM_TO_REGION`. Usar `RiotApiClient.region_for_platform()` — nunca hardcoded.

### Rate limits (development key)
- **20 requests / 1 segundo** E **100 requests / 2 minutos** (ambas janelas enforced simultaneamente).
- O rate limiter em `ingestion/rate_limiter.py` lida com as duas. Não adicionar chamadas `time.sleep()` em lugar nenhum do path de ingestion.

### API key
- Development keys **expiram a cada 24 horas**. Se aparecerem erros `401`/`403`, o usuário precisa atualizar `.env` com uma key nova de developer.riotgames.com.
- Nunca logar a API key completa. O client loga apenas os últimos 4 chars como hash de auditoria.

### Idempotência
- Match data é imutável depois que a partida termina, mas a Riot pode atualizar campos (e.g., quando um participante é banido depois, `participantId.banned` pode mudar).
- Ingestion do Bronze usa `MERGE INTO` em `(match_id, platform)` para handlear re-ingestion com segurança.
- Hashamos payloads (coluna `payload_hash`) para detectar casos "mesma partida, payload diferente".

### Paginação
- `match-v5/matches/by-puuid/{puuid}/ids` pagina com `start` e `count`. Máximo `count=100` por call.
- `league-v4/entries/{queue}/{tier}/{division}` pagina com `page` (1-indexed). Sem `total` retornado — paginar até lista vazia.

### Endpoints com shapes diferentes (gotcha)
- `challengerleagues`, `grandmasterleagues`, `masterleagues` retornam um **único objeto league** com array `entries`.
- `entries/{queue}/{tier}/{division}` (para Diamond e abaixo) retorna uma **lista flat de entries**. Não confundir.

---

## Desenvolvimento PySpark Local

Ao desenvolver transforms Silver/Gold localmente (sem Databricks):

- O extra `[spark]` em `pyproject.toml` instala PySpark + delta-spark.
- Instalar com: `uv sync --extra dev --extra spark`
- Testes que requerem SparkSession são marcados `@pytest.mark.spark` e excluídos por default em runs rápidos.
- Usar fixture session-scoped (será adicionada na Sprint 3) pra evitar spin-up de Spark por teste.
- Java 17 deve estar instalado no host. Windows: `winget install Microsoft.OpenJDK.17`. macOS: `brew install openjdk@17`. Ubuntu: `apt install openjdk-17-jdk`.
- `chispa` é a biblioteca de assertion PySpark. Usar `assert_df_equality(actual, expected)` em vez de checks linha a linha.
- **Limitação importante:** PySpark local **não** executa MERGE, CDF, Liquid Clustering ou Predictive Optimization de forma idêntica ao Databricks Runtime. Use local para lógica de transformação pura; validação dessas features requer Databricks Free Edition.

---

## Setup e Validação Databricks (uma vez por workspace)

Antes do primeiro sprint que toca Delta de verdade (Sprint 2), executar uma vez no workspace:

```sql
-- 1. Criar catálogo
CREATE CATALOG IF NOT EXISTS lol_analytics;
USE CATALOG lol_analytics;

-- 2. Criar schemas das três camadas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 3. Ativar Predictive Optimization (substitui OPTIMIZE/VACUUM manuais)
ALTER CATALOG lol_analytics ENABLE PREDICTIVE OPTIMIZATION;
```

Documentar o procedimento completo em `docs/setup/databricks_workspace.md`. Inclui validação de:
- Managed tables criáveis em UC (vs precisar de external com LOCATION)
- Liquid Clustering aceito no DDL atual do workspace
- Deletion Vectors habilitáveis
- Change Data Feed funcional (escrever, alterar, ler com `table_changes()`)

Se alguma feature não estiver disponível no Free Edition, abrir ADR documentando o fallback escolhido.

---

## O Que o Claude Code Deve Fazer Sem Perguntar

- Adicionar testes ao adicionar código
- Rodar `uv run ruff check --fix` e `uv run ruff format` antes de declarar uma mudança como pronta
- Rodar `uv run pytest --no-cov` antes de declarar uma mudança como pronta
- Rodar `uv run mypy src` antes de declarar uma mudança como pronta (strict mode passa zero erros — manter assim)
- Atualizar `docs/data_dictionary.md` ao adicionar/modificar tabelas
- Atualizar os checkboxes de Roadmap do README ao completar um entregável de sprint
- Usar utilidades existentes (`get_logger`, `get_settings`, `RiotRateLimiter`) em vez de reimplementar
- Escrever mensagens de commit em formato Conventional Commits (em inglês)
- Em DDL novo, seguir as práticas modernas Databricks: Liquid Clustering, column mapping, deletion vectors, change data feed, generated columns, Unity Catalog three-level namespacing
- Em qualquer mudança que afete compute (cluster, runtime, frequência), declarar explicitamente o impacto de custo estimado no PR/commit

## O Que o Claude Code Deve Perguntar Antes de Fazer

- Adicionar uma nova dependência top-level — discutir alternativas e tradeoffs primeiro
- Mudar a estrutura medallion ou schemas de tabelas — propor um ADR primeiro
- Pular ou enfraquecer um teste
- Mexer na lógica do rate limiter — frágil e bem testada, mudanças precisam de justificativa
- Implementar qualquer coisa no escopo da Fase 2 antes da Fase 1 estar entregue
- Introduzir um novo framework (e.g., dbt, Airflow, Streamlit) — esses são non-goals explícitos por enquanto
- Iniciar um sprint novo sem validar o sprint anterior contra Databricks real

## O Que o Claude Code Nunca Deve Fazer

- Commitar secrets. O arquivo `.env` está gitignored — manter assim.
- Logar respostas completas da API em nível INFO (podem conter summoner names / PUUIDs). Nível DEBUG está OK.
- Usar `print()` em código de produção. Sempre `structlog`.
- Usar `from module import *`.
- Bypassar o rate limiter "só pra testar" — usar respostas HTTP mockadas (`respx`) em vez disso.
- Gerar dados sintéticos/fake de partidas sem um comentário claro `# fixture: synthetic data`. Dados realistas mas falsos já confundiram análises reais.
- Fazer push direto para `main` sem um run de CI passando.
- Usar `PARTITIONED BY` em tabelas Delta novas — usar `CLUSTER BY` (Liquid Clustering). Mistura entre os dois é proibida pelo Delta.
- Marcar sprint como concluído sem validação contra Databricks real quando o sprint toca Delta/Spark.

---

## Definition of Done (por sprint)

Um sprint só é marcado como `[x]` no roadmap quando **todos** os critérios abaixo são atendidos:

1. Código implementado, testado e revisado
2. Testes unitários passando (`uv run pytest --no-cov`)
3. mypy strict sem erros (`uv run mypy src`)
4. Ruff lint e format limpos
5. CI verde no GitHub
6. Documentação atualizada (data_dictionary, ADRs novos se aplicável, README roadmap)
7. **Para sprints que tocam Delta/Spark:** validação contra Databricks Free Edition real com dataset pequeno (50-100 partidas), evidência registrada como screenshot ou log em `docs/sprint-N-validation.md`

O passo 7 é o que separa "código que parece funcionar" de "código que funciona".

---

## Roadmap de Sprints (rastrear progresso aqui)

- [x] **Sprint 1: Foundation** — scaffolding do repo, Riot API client com rate limiter, Bronze DDL (com práticas Databricks modernas), smoke test, CLI, CI, docs/architecture.md + docs/data_dictionary.md, mypy strict passando
- [ ] **Sprint 2: Robust Bronze** — ingestion incremental com MERGE, eventos de timeline, dead-letter queue, logs estruturados de ingestion em Delta, populate de `payload_hash`. **Status atual:** primitives (payload_hash, dead_letter, ingestion_log) implementadas e testadas; falta RiotApiClient refactor, BronzeWriter PySpark, 3 runners, CLI commands, testes PySpark, e validação contra Databricks real (ver Definition of Done item 7).
- [ ] **Sprint 3: Silver** — modelo dimensional, SCD2 em dim_champion, framework de data quality, ADR 002 + ADR 003. **Bloqueado por:** conclusão e validação do Sprint 2.
- [ ] **Sprint 4: Gold + Analyses** — agregações de negócio, Liquid Clustering, as 10 queries SQL com comentário de negócio
- [ ] **Sprint 5: Dashboard + Workflow** — Databricks SQL dashboard, scheduled job, monitoring/alerting básico
- [ ] **Sprint 6: Polish** — README com insights e charts, demo video, hardening de CI, post LinkedIn
- [ ] **Sprint 7 (opcional): Personal Analytics Layer** — feature paralela que reaproveita Bronze do meta-game para análise pessoal das partidas do autor (tilt detector, worst matchups, trend semanal). Separado em namespace próprio (`silver.personal_*`, `gold.personal_*`) pra não poluir o modelo dimensional global. Pré-requisito: Sprint 4 entregue.

Ao iniciar um novo sprint, ler a seção relevante deste arquivo, os ADRs mais recentes e os entregáveis do sprint no README. Não começar a codar antes de confirmar alinhamento de escopo.

---

## Issues Conhecidas / Tech Debt

(Atualizar esta seção conforme issues surgem.)

- A coluna `payload_hash` no Bronze está declarada mas o código de ingestion (Sprint 1) ainda não popula — adicionado na Sprint 2 (a validar contra dados reais).
- Sem testes de integration contra uma Riot API real (sandboxed) — confiando em mocks `respx`. Aceitável para Fase 1.
- `mypy --strict` passa zero erros e o CI agora enforça (sem `continue-on-error`).
- Validação Sprint 2 contra Databricks real pendente — bloqueia Sprint 3.

---

## Em Caso de Dúvida

- Ler o README primeiro.
- Ler os ADRs relevantes em `docs/adr/`.
- Procurar padrões existentes no codebase antes de introduzir novos.
- Preferir soluções simples e chatas a soluções clever. Este é um projeto de portfolio destinado a demonstrar engenharia sólida, não novidade.
- O usuário (Christian Kenzo Seki) prefere ver tradeoffs e estimativas de custo *antes* da implementação, especialmente para decisões arquiteturais. Não apenas escrever código — explicar a escolha primeiro quando há mais de um caminho razoável.
- O usuário prefere conversar em **português**; código, commits, docstrings e mensagens de log permanecem em **inglês** (convenção do repo).
