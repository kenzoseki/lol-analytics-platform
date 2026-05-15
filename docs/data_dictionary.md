# Dicionário de Dados

Referência autoritativa de cada tabela do lakehouse: colunas, tipos,
grão, clustering e linhagem.

> **Escopo deste documento:** a partir da Sprint 1, apenas a camada
> Bronze foi entregue. Silver e Gold serão documentadas aqui conforme
> entregues (Sprint 3 e Sprint 4 respectivamente). A fonte de verdade
> do DDL é [`sql/ddl/`](../sql/ddl/).

---

## Convenções gerais

- **Namespacing Unity Catalog (three-level):** `catalog.schema.table`.
  Catálogo: `lol_analytics`. Schemas: `bronze`, `silver`, `gold`.
- **Timestamps são UTC.** Sempre.
- **Nomes de colunas:** `snake_case`.
- **Strings:** quando a coluna se chama `payload`, contém JSON cru
  preservado verbatim. Todo o resto é tipado/parseado a partir do Silver.
- **Bronze nunca deleta linhas.** Correções entram como novas linhas
  com `ingestion_timestamp` posterior; o Silver pega a mais recente.

### Properties Delta padrão em toda tabela Bronze

| Property | Valor | Por que |
|---|---|---|
| `delta.columnMapping.mode` | `name` | Permite renomear/dropar colunas sem reescrever arquivos. Suporta caracteres especiais. |
| `delta.minReaderVersion` | `2` | Requerido pelo column mapping. |
| `delta.minWriterVersion` | `5` | Requerido pelo column mapping. |
| `delta.enableDeletionVectors` | `true` | Acelera MERGE/UPDATE/DELETE — fundamental para idempotência via MERGE INTO. |
| `delta.enableChangeDataFeed` | `true` | Habilita CDF para que Silver consuma apenas linhas novas/alteradas (exceto no dead-letter, onde CDF não traz valor). |

### Clustering em vez de partitioning

Todas as tabelas Bronze usam **Liquid Clustering** (`CLUSTER BY`) em vez
de `PARTITIONED BY`. Recomendação atual da Databricks:

- Mais flexível: chaves de cluster podem mudar sem reescrita.
- Suporta padrões de acesso variados (range scans, point lookups, MERGE).
- Compatível com cargas de baixa cardinalidade que matariam um esquema
  de partições (e.g. partitionar por `tier` quebraria; clustering nele,
  não).
- **Predictive Optimization** (ativado a nível de catálogo) cuida de
  OPTIMIZE/VACUUM automaticamente.

---

## Camada Bronze

Zona de pouso raw. Toda tabela aqui:

- Armazena respostas da Riot API verbatim como string JSON no `payload`.
- Marca cada linha com `ingestion_timestamp`, `ingestion_date`
  (gerada) e `source_endpoint` para linhagem.
- É upsertada via `MERGE INTO` na chave natural para idempotência.

---

### `lol_analytics.bronze.raw_matches`

**Grão:** uma linha por `(match_id, platform)` vinda de `match-v5/matches/{matchId}`.
**Chaves de cluster:** `(ingestion_date, match_id)`.
**Chave natural para MERGE:** `(match_id, platform)`.
**Source endpoint:** `/lol/match/v5/matches/{matchId}`.

| Coluna | Tipo | Nullable | Descrição |
|---|---|---|---|
| `match_id` | STRING | Não | Riot match ID, e.g. `BR1_2987654321`. |
| `platform` | STRING | Não | Platform shard (`BR1`, `KR`). |
| `region` | STRING | Não | Routing super-region (`americas`, `asia`). |
| `payload` | STRING | Não | JSON completo da resposta, sem parsing. |
| `payload_hash` | STRING | Não | SHA-256 de `payload` para detecção de mudança.* |
| `ingestion_timestamp` | TIMESTAMP | Não | UTC do momento em que a linha foi escrita. |
| `ingestion_date` | DATE | Não | Gerada: `CAST(ingestion_timestamp AS DATE)`. Chave de cluster. |
| `source_endpoint` | STRING | Não | Path do endpoint da Riot que produziu o payload. |
| `api_key_hash` | STRING | Sim | Últimos 4 chars da API key usada (auditoria). |

> \* A partir da Sprint 1, `payload_hash` está declarado no DDL mas o
> código de ingestão ainda não popula. Implementado na Sprint 2.

---

### `lol_analytics.bronze.raw_match_timeline`

**Grão:** uma linha por `(match_id, platform)` vinda de `match-v5/matches/{matchId}/timeline`.
**Chaves de cluster:** `(ingestion_date, match_id)`.
**Chave natural para MERGE:** `(match_id, platform)`.
**Source endpoint:** `/lol/match/v5/matches/{matchId}/timeline`.

| Coluna | Tipo | Nullable | Descrição |
|---|---|---|---|
| `match_id` | STRING | Não | Riot match ID; FK em espírito para `raw_matches`. |
| `platform` | STRING | Não | Platform shard. |
| `region` | STRING | Não | Routing super-region. |
| `payload` | STRING | Não | Timeline JSON completo (grande — eventos minuto a minuto). |
| `payload_hash` | STRING | Não | SHA-256 de `payload`. |
| `ingestion_timestamp` | TIMESTAMP | Não | UTC do momento em que a linha foi escrita. |
| `ingestion_date` | DATE | Não | Gerada. Chave de cluster. |
| `source_endpoint` | STRING | Não | Path do endpoint da Riot. |
| `api_key_hash` | STRING | Sim | Últimos 4 chars da API key usada. |

> Population começa na Sprint 2 junto com o job de ingestão de timeline.

---

### `lol_analytics.bronze.raw_league_entries`

**Grão:** uma linha por `(puuid, platform, queue_type, snapshot_date)`.
Um "snapshot" é uma pull única de `league-v4` pra um tier num platform.
Snapshots acumulam ao longo do tempo, então a tabela também é uma série
temporal de quem estava em Master+ em qual dia.

**Chaves de cluster:** `(ingestion_date, tier, platform)`.
**Source endpoints:**
- `/lol/league/v4/challengerleagues/by-queue/{queue}`
- `/lol/league/v4/grandmasterleagues/by-queue/{queue}`
- `/lol/league/v4/masterleagues/by-queue/{queue}`

| Coluna | Tipo | Nullable | Descrição |
|---|---|---|---|
| `puuid` | STRING | Não | PUUID encriptado da Riot. |
| `summoner_id` | STRING | Sim | Summoner ID encriptado (legacy; alguns endpoints omitem). |
| `platform` | STRING | Não | Platform shard. |
| `queue_type` | STRING | Não | Queue, e.g. `RANKED_SOLO_5x5`. |
| `tier` | STRING | Não | `CHALLENGER`, `GRANDMASTER` ou `MASTER`. |
| `rank` | STRING | Sim | `I`/`II`/`III`/`IV`. NULL para Master+ (sem divisões). |
| `league_points` | INT | Não | LP no momento do snapshot. |
| `wins` | INT | Não | Vitórias na temporada no momento do snapshot. |
| `losses` | INT | Não | Derrotas na temporada no momento do snapshot. |
| `payload` | STRING | Não | JSON completo do entry, pra evolução de schema. |
| `ingestion_timestamp` | TIMESTAMP | Não | UTC do momento em que a linha foi escrita. |
| `ingestion_date` | DATE | Não | Gerada. Chave de cluster. |
| `source_endpoint` | STRING | Não | Qual endpoint league-v4 produziu a linha. |

> Nenhum job popula essa tabela ainda — DDL é forward-looking,
> implementado na Sprint 2.

---

### `lol_analytics.bronze.ingestion_dead_letter`

**Grão:** uma linha por request da Riot API que falhou terminalmente
(retries esgotados ou 4xx não-retryable retornado).
**Chaves de cluster:** `(failed_at_date, error_class)`.

Essa tabela é o único lugar SQL-queryable pra responder "o que está
quebrado no pipeline agora?" sem grepar logs.

| Coluna | Tipo | Nullable | Descrição |
|---|---|---|---|
| `request_id` | STRING | Não | UUID gerado quando o request foi tentado. |
| `endpoint` | STRING | Não | Nome lógico do endpoint (`get_match`, `get_match_timeline`, ...). |
| `url` | STRING | Não | URL completa chamada. |
| `http_status` | INT | Sim | Último status HTTP visto. NULL para erros de transporte. |
| `error_class` | STRING | Não | Nome da classe de exceção (`RiotApiError`, `TransportError`, ...). |
| `error_message` | STRING | Sim | Mensagem de erro truncada. |
| `request_payload` | STRING | Sim | Query params ou body, se aplicável. |
| `attempt_count` | INT | Não | Quantas tentativas antes de desistir. |
| `failed_at` | TIMESTAMP | Não | UTC da falha final. |
| `failed_at_date` | DATE | Não | Gerada: `CAST(failed_at AS DATE)`. Chave de cluster. |

> Population começa na Sprint 2 junto com o novo runner de ingestão.

---

## Camada Silver

**Status:** ainda não entregue (Sprint 3).

Vai documentar aqui: `dim_champion` (SCD2), `dim_patch`, `dim_summoner`,
`fact_match_participant`, `fact_match_event`.

---

## Camada Gold

**Status:** ainda não entregue (Sprint 4).

Vai documentar aqui: `agg_champion_patch_elo`, `agg_champion_synergy`,
`agg_meta_evolution`, mais as chaves de Liquid Clustering escolhidas.
