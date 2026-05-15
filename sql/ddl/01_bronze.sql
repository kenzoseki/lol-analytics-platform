-- ============================================================
-- CAMADA BRONZE — Zona de pouso raw para respostas da Riot API
-- ============================================================
--
-- Princípios de design:
--   1. Nunca perder dados: append-only, JSON cru preservado verbatim.
--   2. Idempotente: o mesmo match_id ingerido duas vezes = uma linha
--      (MERGE INTO sobre a chave natural).
--   3. Rastreável: toda linha marcada com ingestion_timestamp +
--      source_endpoint + hash da API key (auditoria).
--   4. Schema-tolerante: a Riot adiciona campos entre patches; STRING
--      pra payload bruto permite evoluir o Silver sem reingerir Bronze.
--
-- Decisões de design modernas (Databricks 2025+):
--   - Unity Catalog three-level namespacing: catalog.schema.table.
--   - Column mapping em modo 'name' permite renomear/dropar colunas
--     sem reescrever arquivos e suporta caracteres especiais.
--   - Generated columns derivam ingestion_date do timestamp sem precisar
--     popular manualmente.
--   - Liquid Clustering (CLUSTER BY) substitui PARTITIONED BY: mais
--     flexível, melhor pra cargas com padrões de acesso variados,
--     chaves podem ser alteradas sem reescrita. Recomendação atual
--     da Databricks pra novas tabelas.
--   - Deletion Vectors aceleram MERGE/UPDATE/DELETE — fundamental
--     pra idempotência via MERGE INTO no Bronze.
--   - Predictive Optimization (ativado a nível de catálogo via
--     ALTER CATALOG ... ENABLE PREDICTIVE OPTIMIZATION) cuida de
--     OPTIMIZE/VACUUM automaticamente, dispensando autoOptimize manual.
--
-- Catálogo e schemas (executar uma vez):
-- ============================================================

CREATE CATALOG IF NOT EXISTS lol_analytics
COMMENT 'LoL meta-game analytics platform (Phase 1: BR1 + KR Master+).';

CREATE SCHEMA IF NOT EXISTS lol_analytics.bronze
COMMENT 'Raw landing zone for Riot API responses. Append-only with MERGE for idempotency.';

-- Ativar Predictive Optimization no catálogo é a recomendação atual da
-- Databricks pra dispensar tuning manual de OPTIMIZE/VACUUM. Requer
-- Unity Catalog e managed tables. Executar uma vez:
--
--   ALTER CATALOG lol_analytics ENABLE PREDICTIVE OPTIMIZATION;
--

-- ============================================================
-- bronze.raw_matches
-- ------------------------------------------------------------
-- Grão: uma linha por (match_id, platform) vinda de
-- match-v5/matches/{matchId}.
-- Chave natural pro MERGE: (match_id, platform).
-- ============================================================
CREATE TABLE IF NOT EXISTS lol_analytics.bronze.raw_matches (
    -- Identidade
    match_id              STRING NOT NULL COMMENT 'Riot match ID, e.g. BR1_2987654321',
    platform              STRING NOT NULL COMMENT 'Platform shard (BR1, KR, ...)',
    region                STRING NOT NULL COMMENT 'Routing super-region (americas, asia, ...)',

    -- Payload bruto (não parsear aqui — isso é responsabilidade do Silver)
    payload               STRING NOT NULL COMMENT 'Full match JSON response, unparsed',
    payload_hash          STRING NOT NULL COMMENT 'SHA-256 of payload for change detection',

    -- Lineage
    ingestion_timestamp   TIMESTAMP NOT NULL COMMENT 'UTC time the row was written',
    ingestion_date        DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE))
                          COMMENT 'Generated from ingestion_timestamp; used as clustering key',
    source_endpoint       STRING NOT NULL COMMENT '/lol/match/v5/matches/{matchId}',
    api_key_hash          STRING COMMENT 'Last 4 chars of API key used (audit only)'
)
USING DELTA
CLUSTER BY (ingestion_date, match_id)
TBLPROPERTIES (
    'delta.columnMapping.mode'         = 'name',
    'delta.minReaderVersion'           = '2',
    'delta.minWriterVersion'           = '5',
    'delta.enableDeletionVectors'      = 'true',
    'delta.enableChangeDataFeed'       = 'true'
);

-- ============================================================
-- bronze.raw_match_timeline
-- ------------------------------------------------------------
-- Grão: uma linha por (match_id, platform) vinda de
-- match-v5/matches/{matchId}/timeline.
-- Payload é grande (eventos minuto a minuto). Tabela separada
-- de raw_matches para evitar scans desnecessários quando o
-- timeline não é requerido.
-- ============================================================
CREATE TABLE IF NOT EXISTS lol_analytics.bronze.raw_match_timeline (
    match_id              STRING NOT NULL,
    platform              STRING NOT NULL,
    region                STRING NOT NULL,
    payload               STRING NOT NULL COMMENT 'Full timeline JSON — large, minute-by-minute events',
    payload_hash          STRING NOT NULL,
    ingestion_timestamp   TIMESTAMP NOT NULL,
    ingestion_date        DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE)),
    source_endpoint       STRING NOT NULL,
    api_key_hash          STRING
)
USING DELTA
CLUSTER BY (ingestion_date, match_id)
TBLPROPERTIES (
    'delta.columnMapping.mode'         = 'name',
    'delta.minReaderVersion'           = '2',
    'delta.minWriterVersion'           = '5',
    'delta.enableDeletionVectors'      = 'true',
    'delta.enableChangeDataFeed'       = 'true'
);

-- ============================================================
-- bronze.raw_league_entries
-- ------------------------------------------------------------
-- Grão: uma linha por (puuid, platform, queue_type, snapshot_date).
-- Cada "snapshot" é uma pull do league-v4 pra um tier num platform.
-- Snapshots acumulam ao longo do tempo, formando uma série temporal
-- de quem estava em Master+ em qual dia.
-- ============================================================
CREATE TABLE IF NOT EXISTS lol_analytics.bronze.raw_league_entries (
    puuid                 STRING NOT NULL COMMENT 'Riot encrypted PUUID',
    summoner_id           STRING COMMENT 'Legacy encrypted summoner ID; some endpoints omit',
    platform              STRING NOT NULL,
    queue_type            STRING NOT NULL COMMENT 'e.g. RANKED_SOLO_5x5',
    tier                  STRING NOT NULL COMMENT 'CHALLENGER, GRANDMASTER, MASTER',
    rank                  STRING COMMENT 'I/II/III/IV. NULL for Master+ (no divisions)',
    league_points         INT NOT NULL,
    wins                  INT NOT NULL,
    losses                INT NOT NULL,
    payload               STRING NOT NULL COMMENT 'Full entry JSON for forward compatibility',
    ingestion_timestamp   TIMESTAMP NOT NULL,
    ingestion_date        DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE)),
    source_endpoint       STRING NOT NULL
)
USING DELTA
CLUSTER BY (ingestion_date, tier, platform)
TBLPROPERTIES (
    'delta.columnMapping.mode'         = 'name',
    'delta.minReaderVersion'           = '2',
    'delta.minWriterVersion'           = '5',
    'delta.enableDeletionVectors'      = 'true',
    'delta.enableChangeDataFeed'       = 'true'
);

-- ============================================================
-- bronze.ingestion_dead_letter
-- ------------------------------------------------------------
-- Dead letter queue: requests que falharam permanentemente após
-- retries. Crítico pra debugar problemas da API sem bloquear o
-- pipeline. Uma query SQL responde "o que está quebrado agora?".
-- ============================================================
CREATE TABLE IF NOT EXISTS lol_analytics.bronze.ingestion_dead_letter (
    request_id            STRING NOT NULL COMMENT 'UUID generated at attempt time',
    endpoint              STRING NOT NULL COMMENT 'Logical endpoint name (e.g. get_match)',
    url                   STRING NOT NULL,
    http_status           INT COMMENT 'Last HTTP status; NULL for transport errors',
    error_class           STRING NOT NULL COMMENT 'Exception class (RiotApiError, TransportError, ...)',
    error_message         STRING COMMENT 'Truncated error message',
    request_payload       STRING COMMENT 'Params/body if applicable',
    attempt_count         INT NOT NULL,
    failed_at             TIMESTAMP NOT NULL,
    failed_at_date        DATE GENERATED ALWAYS AS (CAST(failed_at AS DATE))
)
USING DELTA
CLUSTER BY (failed_at_date, error_class)
TBLPROPERTIES (
    'delta.columnMapping.mode'         = 'name',
    'delta.minReaderVersion'           = '2',
    'delta.minWriterVersion'           = '5',
    'delta.enableDeletionVectors'      = 'true'
);

-- ============================================================
-- bronze.ingestion_log
-- ------------------------------------------------------------
-- Eventos estruturados de cada run de ingestão. Permite responder
-- via SQL: "quantas runs rodaram hoje?", "quantos matches o
-- runner X processou?", "qual a taxa de duplicates skipados?".
--
-- Grão: uma linha por evento (started/completed/duplicate/failed)
-- emitido por um runner. Múltiplas linhas por run_id.
-- ============================================================
CREATE TABLE IF NOT EXISTS lol_analytics.bronze.ingestion_log (
    event_id              STRING NOT NULL COMMENT 'UUID per event',
    run_id                STRING NOT NULL COMMENT 'UUID shared across all events of a single runner invocation',
    runner_name           STRING NOT NULL COMMENT 'e.g. match_ingestion, timeline_ingestion, league_entries_ingestion',
    action                STRING NOT NULL COMMENT 'started | completed | inserted | skipped_duplicate | failed',
    platform              STRING COMMENT 'Platform shard, if applicable',
    target_table          STRING COMMENT 'Fully-qualified table written to, if applicable',
    rows_affected         BIGINT COMMENT 'Rows inserted/updated by this event',
    error_class           STRING COMMENT 'Exception class on failure events',
    error_message         STRING COMMENT 'Truncated error message',
    duration_ms           BIGINT COMMENT 'Wall-clock duration for terminal events (completed/failed)',
    emitted_at            TIMESTAMP NOT NULL COMMENT 'UTC time the event was recorded',
    emitted_at_date       DATE GENERATED ALWAYS AS (CAST(emitted_at AS DATE))
)
USING DELTA
CLUSTER BY (emitted_at_date, runner_name)
TBLPROPERTIES (
    'delta.columnMapping.mode'         = 'name',
    'delta.minReaderVersion'           = '2',
    'delta.minWriterVersion'           = '5',
    'delta.enableDeletionVectors'      = 'true',
    'delta.enableChangeDataFeed'       = 'true'
);
