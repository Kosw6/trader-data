CREATE TABLE company (
    id BIGSERIAL PRIMARY KEY,
    cik VARCHAR(10) NOT NULL UNIQUE,
    company_name VARCHAR(255) NOT NULL,
    sic_code VARCHAR(4),
    sic_description VARCHAR(255),
    fiscal_year_end VARCHAR(4),
    country_code VARCHAR(2),
    entity_type VARCHAR(50),
    collected_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE company_alias (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES company(id) ON DELETE CASCADE,
    alias VARCHAR(255) NOT NULL,
    normalized_alias VARCHAR(255) NOT NULL,
    alias_type VARCHAR(30) NOT NULL,
    language VARCHAR(10),
    is_ambiguous BOOLEAN NOT NULL DEFAULT FALSE,
    valid_from DATE,
    valid_to DATE,
    source VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (company_id, normalized_alias, alias_type)
);

CREATE TABLE asset (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT REFERENCES company(id),
    asset_type VARCHAR(30) NOT NULL,
    ticker VARCHAR(32) NOT NULL,
    name VARCHAR(255) NOT NULL,
    share_class VARCHAR(50),
    security_identifier VARCHAR(64),
    exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    timezone VARCHAR(64) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    listed_at DATE,
    delisted_at DATE
);

CREATE UNIQUE INDEX uq_asset_active_exchange_ticker
    ON asset (exchange, ticker)
    WHERE active = TRUE;

CREATE TABLE asset_symbol_history (
    id BIGSERIAL PRIMARY KEY,
    asset_id BIGINT NOT NULL REFERENCES asset(id) ON DELETE CASCADE,
    ticker VARCHAR(32) NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE NULLS NOT DISTINCT (asset_id, ticker, exchange, valid_from)
);

CREATE TABLE industry_classification (
    id BIGSERIAL PRIMARY KEY,
    parent_id BIGINT REFERENCES industry_classification(id),
    provider VARCHAR(30) NOT NULL,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    UNIQUE (provider, code)
);

CREATE TABLE company_industry (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES company(id) ON DELETE CASCADE,
    classification_id BIGINT NOT NULL REFERENCES industry_classification(id),
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    valid_from DATE,
    valid_to DATE,
    UNIQUE NULLS NOT DISTINCT (company_id, classification_id, valid_from)
);

CREATE TABLE price_bar (
    id BIGSERIAL PRIMARY KEY,
    asset_id BIGINT NOT NULL REFERENCES asset(id) ON DELETE CASCADE,
    source VARCHAR(30) NOT NULL,
    "interval" VARCHAR(8) NOT NULL,
    bar_time TIMESTAMPTZ NOT NULL,
    open NUMERIC(24, 8) NOT NULL,
    high NUMERIC(24, 8) NOT NULL,
    low NUMERIC(24, 8) NOT NULL,
    close NUMERIC(24, 8) NOT NULL,
    adjusted_close NUMERIC(24, 8),
    volume NUMERIC(30, 0),
    currency VARCHAR(3) NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    content_hash CHAR(64),
    quality_status VARCHAR(20) NOT NULL DEFAULT 'VALID',
    display_policy VARCHAR(20) NOT NULL DEFAULT 'FULL_OHLC',
    quality_checked_at TIMESTAMPTZ,
    UNIQUE (asset_id, source, "interval", bar_time)
);

CREATE TABLE price_bar_quality_issue (
    id BIGSERIAL PRIMARY KEY,
    price_bar_id BIGINT NOT NULL,
    asset_id BIGINT NOT NULL REFERENCES asset(id) ON DELETE CASCADE,
    source VARCHAR(30) NOT NULL,
    "interval" VARCHAR(8) NOT NULL,
    bar_time TIMESTAMPTZ NOT NULL,
    issue_code VARCHAR(50) NOT NULL,
    field_name VARCHAR(30) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    observed_value NUMERIC(24, 8),
    issue_status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ,
    resolution_source VARCHAR(30),
    resolution_note TEXT,
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    FOREIGN KEY (price_bar_id, asset_id, bar_time)
        REFERENCES price_bar(id, asset_id, bar_time)
        ON DELETE CASCADE,
    UNIQUE (asset_id, source, "interval", bar_time, issue_code, field_name)
);

CREATE TABLE corporate_action (
    id BIGSERIAL PRIMARY KEY,
    asset_id BIGINT NOT NULL REFERENCES asset(id) ON DELETE CASCADE,
    action_type VARCHAR(30) NOT NULL,
    announced_at TIMESTAMPTZ,
    effective_date DATE NOT NULL,
    ex_date DATE,
    record_date DATE,
    payment_date DATE,
    value NUMERIC(30, 10),
    ratio NUMERIC(30, 10),
    currency VARCHAR(3),
    source VARCHAR(30) NOT NULL,
    content_hash CHAR(64),
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (asset_id, action_type, effective_date, source)
);

CREATE TABLE news_story_cluster (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    event_at TIMESTAMPTZ,
    content_fingerprint CHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE news_article (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    external_id VARCHAR(255),
    publisher VARCHAR(255),
    canonical_url TEXT NOT NULL,
    canonical_url_hash CHAR(64) NOT NULL UNIQUE,
    title TEXT NOT NULL,
    summary TEXT,
    body TEXT,
    language VARCHAR(10),
    published_at TIMESTAMPTZ,
    published_at_precision VARCHAR(10),
    discovered_at TIMESTAMPTZ NOT NULL,
    body_collected_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    content_hash CHAR(64),
    raw_object_uri TEXT,
    status VARCHAR(30) NOT NULL,
    rag_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    story_cluster_id BIGINT REFERENCES news_story_cluster(id) ON DELETE SET NULL,
    license_type VARCHAR(30) NOT NULL CHECK (
        license_type IN (
            'API_LICENSE',
            'PUBLISHER_LICENSE',
            'PUBLIC_DOMAIN',
            'LINK_ONLY',
            'UNKNOWN'
        )
    ),
    body_storage_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    redistribution_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    retention_until TIMESTAMPTZ,
    terms_url TEXT,
    license_checked_at TIMESTAMPTZ NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source, external_id)
);

CREATE TABLE news_company_link (
    article_id BIGINT NOT NULL REFERENCES news_article(id) ON DELETE CASCADE,
    company_id BIGINT NOT NULL REFERENCES company(id) ON DELETE CASCADE,
    match_type VARCHAR(30) NOT NULL,
    matched_text TEXT,
    match_confidence NUMERIC(7, 6) NOT NULL,
    relevance_score NUMERIC(7, 6) NOT NULL,
    mention_count INTEGER NOT NULL DEFAULT 0,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    resolver_name VARCHAR(100) NOT NULL,
    resolver_version VARCHAR(100) NOT NULL,
    manually_verified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (article_id, company_id)
);

CREATE TABLE news_asset_link (
    article_id BIGINT NOT NULL REFERENCES news_article(id) ON DELETE CASCADE,
    asset_id BIGINT NOT NULL REFERENCES asset(id) ON DELETE CASCADE,
    match_type VARCHAR(30) NOT NULL,
    matched_text TEXT,
    match_confidence NUMERIC(7, 6) NOT NULL,
    relevance_score NUMERIC(7, 6) NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (article_id, asset_id)
);

CREATE TABLE filing (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES company(id),
    accession_number VARCHAR(30) NOT NULL UNIQUE,
    form_type VARCHAR(20) NOT NULL,
    filing_date DATE NOT NULL,
    report_date DATE,
    accepted_at TIMESTAMPTZ,
    primary_document VARCHAR(255),
    document_url TEXT,
    is_xbrl BOOLEAN,
    is_inline_xbrl BOOLEAN,
    amendment BOOLEAN NOT NULL DEFAULT FALSE,
    raw_object_uri TEXT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE filing_document (
    id BIGSERIAL PRIMARY KEY,
    filing_id BIGINT NOT NULL REFERENCES filing(id) ON DELETE CASCADE,
    document_type VARCHAR(30) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    document_url TEXT,
    raw_object_uri TEXT,
    content_hash CHAR(64),
    UNIQUE (filing_id, file_name)
);

CREATE TABLE financial_metric (
    id BIGSERIAL PRIMARY KEY,
    metric_code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    statement_type VARCHAR(30) NOT NULL,
    value_type VARCHAR(20) NOT NULL,
    default_unit VARCHAR(30)
);

CREATE TABLE financial_metric_mapping (
    id BIGSERIAL PRIMARY KEY,
    taxonomy VARCHAR(50) NOT NULL,
    source_tag VARCHAR(255) NOT NULL,
    metric_id BIGINT NOT NULL REFERENCES financial_metric(id) ON DELETE CASCADE,
    company_id BIGINT REFERENCES company(id) ON DELETE CASCADE,
    priority INTEGER NOT NULL DEFAULT 0,
    valid_from DATE,
    valid_to DATE
);

CREATE TABLE financial_fact (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES company(id),
    filing_id BIGINT NOT NULL REFERENCES filing(id),
    metric_id BIGINT REFERENCES financial_metric(id),
    taxonomy VARCHAR(50) NOT NULL,
    source_tag VARCHAR(255) NOT NULL,
    unit VARCHAR(30) NOT NULL,
    period_start DATE,
    period_end DATE NOT NULL,
    value NUMERIC(38, 10) NOT NULL,
    fiscal_year INTEGER,
    fiscal_period VARCHAR(10),
    form_type VARCHAR(20) NOT NULL,
    frame VARCHAR(50),
    dimensions JSONB,
    source_fact_hash CHAR(64) NOT NULL UNIQUE,
    collected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE macro_series (
    id BIGSERIAL PRIMARY KEY,
    provider VARCHAR(30) NOT NULL,
    external_series_id VARCHAR(100) NOT NULL,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(50) NOT NULL,
    frequency VARCHAR(20) NOT NULL,
    unit VARCHAR(50) NOT NULL,
    seasonal_adjustment VARCHAR(50),
    country_code VARCHAR(2) NOT NULL,
    UNIQUE (provider, external_series_id)
);

CREATE TABLE macro_release (
    id BIGSERIAL PRIMARY KEY,
    provider VARCHAR(30) NOT NULL,
    external_id VARCHAR(255) NOT NULL,
    release_type VARCHAR(50) NOT NULL,
    title VARCHAR(500) NOT NULL,
    reference_period VARCHAR(20),
    scheduled_at TIMESTAMPTZ NOT NULL,
    actual_released_at TIMESTAMPTZ,
    first_available_at TIMESTAMPTZ,
    sequence_number INTEGER,
    status VARCHAR(30) NOT NULL,
    source_url TEXT,
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (provider, external_id)
);

CREATE TABLE macro_observation (
    id BIGSERIAL PRIMARY KEY,
    series_id BIGINT NOT NULL REFERENCES macro_series(id) ON DELETE CASCADE,
    release_id BIGINT REFERENCES macro_release(id) ON DELETE SET NULL,
    reference_period VARCHAR(20) NOT NULL,
    value NUMERIC(38, 10) NOT NULL,
    period_code VARCHAR(10) NOT NULL,
    footnotes JSONB,
    content_hash CHAR(64) NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (series_id, reference_period)
);

CREATE TABLE macro_observation_vintage (
    id BIGSERIAL PRIMARY KEY,
    observation_id BIGINT NOT NULL REFERENCES macro_observation(id) ON DELETE CASCADE,
    release_id BIGINT REFERENCES macro_release(id) ON DELETE SET NULL,
    value NUMERIC(38, 10) NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    availability_source VARCHAR(30) NOT NULL CHECK (
        availability_source IN ('RELEASE', 'API_FIRST_SEEN', 'COLLECTED_FALLBACK')
    ),
    collected_at TIMESTAMPTZ NOT NULL,
    content_hash CHAR(64) NOT NULL,
    is_initial BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (observation_id, content_hash)
);

CREATE TABLE source_object (
    id BIGSERIAL PRIMARY KEY,
    provider VARCHAR(30) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    external_id VARCHAR(255),
    pipeline_job_item_id BIGINT,
    object_uri TEXT NOT NULL,
    object_size BIGINT NOT NULL,
    content_hash CHAR(64) NOT NULL,
    dedupe_key CHAR(64) NOT NULL UNIQUE,
    collected_at TIMESTAMPTZ NOT NULL,
    processing_status VARCHAR(30) NOT NULL
);

CREATE TABLE news_article_version (
    id BIGSERIAL PRIMARY KEY,
    article_id BIGINT NOT NULL REFERENCES news_article(id) ON DELETE CASCADE,
    source_object_id BIGINT NOT NULL REFERENCES source_object(id) ON DELETE RESTRICT,
    version_number INTEGER NOT NULL,
    title TEXT NOT NULL,
    summary TEXT,
    summary_source VARCHAR(30) CHECK (
        summary_source IN ('PROVIDER', 'EXTRACTIVE_ETL', 'NONE')
    ),
    body TEXT,
    source_updated_at TIMESTAMPTZ,
    available_at TIMESTAMPTZ NOT NULL,
    availability_source VARCHAR(30) NOT NULL CHECK (
        availability_source IN (
            'PUBLISHER_UPDATED',
            'API_FIRST_SEEN',
            'COLLECTED_FALLBACK'
        )
    ),
    body_collected_at TIMESTAMPTZ,
    content_hash CHAR(64) NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (article_id, content_hash),
    UNIQUE (article_id, version_number)
);

CREATE UNIQUE INDEX uq_news_article_version_current
    ON news_article_version (article_id)
    WHERE is_current = TRUE;

CREATE TABLE collection_schedule (
    id BIGSERIAL PRIMARY KEY,
    job_type VARCHAR(50) NOT NULL,
    cron_expression VARCHAR(100) NOT NULL,
    timezone VARCHAR(64) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    parameters JSONB NOT NULL DEFAULT '{}'::JSONB,
    last_run_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE pipeline_job (
    id BIGSERIAL PRIMARY KEY,
    schedule_id BIGINT REFERENCES collection_schedule(id) ON DELETE SET NULL,
    job_type VARCHAR(50) NOT NULL,
    source VARCHAR(50) NOT NULL,
    status VARCHAR(30) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    scheduled_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    requested_by VARCHAR(100) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    retry_count INTEGER NOT NULL DEFAULT 0,
    parameters JSONB NOT NULL DEFAULT '{}'::JSONB,
    error_code VARCHAR(100),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE pipeline_job_item (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES pipeline_job(id) ON DELETE CASCADE,
    item_key VARCHAR(255) NOT NULL,
    status VARCHAR(30) NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    checkpoint JSONB,
    error_code VARCHAR(100),
    error_message TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    UNIQUE (job_id, item_key)
);

ALTER TABLE source_object
    ADD CONSTRAINT fk_source_object_pipeline_job_item
    FOREIGN KEY (pipeline_job_item_id)
    REFERENCES pipeline_job_item(id)
    ON DELETE SET NULL;

CREATE TABLE pipeline_outbox (
    id BIGSERIAL PRIMARY KEY,
    source_object_id BIGINT NOT NULL REFERENCES source_object(id) ON DELETE RESTRICT,
    job_item_id BIGINT NOT NULL REFERENCES pipeline_job_item(id) ON DELETE RESTRICT,
    event_type VARCHAR(50) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    message_key VARCHAR(255) NOT NULL,
    idempotency_key CHAR(64) NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    status VARCHAR(30) NOT NULL CHECK (
        status IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'RETRY_WAIT', 'DEAD')
    ),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_pipeline_outbox_publishable
    ON pipeline_outbox (status, next_attempt_at, created_at);

CREATE TABLE data_coverage (
    id BIGSERIAL PRIMARY KEY,
    dataset_type VARCHAR(30) NOT NULL,
    entity_type VARCHAR(30) NOT NULL,
    entity_key VARCHAR(255) NOT NULL,
    period_type VARCHAR(30) NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    status VARCHAR(30) NOT NULL,
    expected_count INTEGER NOT NULL DEFAULT 0,
    actual_count INTEGER NOT NULL DEFAULT 0,
    missing_items JSONB,
    latest_source_at TIMESTAMPTZ,
    last_checked_at TIMESTAMPTZ NOT NULL,
    resolution_job_id BIGINT REFERENCES pipeline_job(id) ON DELETE SET NULL,
    reason_code VARCHAR(100),
    UNIQUE (
        dataset_type,
        entity_type,
        entity_key,
        period_type,
        period_start,
        period_end
    )
);

CREATE TABLE data_gap (
    id BIGSERIAL PRIMARY KEY,
    coverage_id BIGINT NOT NULL REFERENCES data_coverage(id) ON DELETE CASCADE,
    gap_key VARCHAR(255) NOT NULL,
    status VARCHAR(30) NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    resolution_job_id BIGINT REFERENCES pipeline_job(id) ON DELETE SET NULL,
    details JSONB,
    UNIQUE (coverage_id, gap_key)
);

CREATE TABLE record_lineage (
    id BIGSERIAL PRIMARY KEY,
    source_object_id BIGINT NOT NULL REFERENCES source_object(id) ON DELETE RESTRICT,
    target_table VARCHAR(100) NOT NULL,
    target_record_id BIGINT NOT NULL,
    target_record_key VARCHAR(255) NOT NULL,
    pipeline_job_id BIGINT REFERENCES pipeline_job(id) ON DELETE SET NULL,
    transformer_name VARCHAR(100) NOT NULL,
    transformer_version VARCHAR(100) NOT NULL,
    output_hash CHAR(64),
    processed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (
        source_object_id,
        target_table,
        target_record_key,
        transformer_name,
        transformer_version
    )
);

CREATE TABLE asset_alias (
    id BIGSERIAL PRIMARY KEY,
    asset_id BIGINT NOT NULL REFERENCES asset(id) ON DELETE CASCADE,
    alias VARCHAR(255) NOT NULL,
    normalized_alias VARCHAR(255) NOT NULL,
    alias_type VARCHAR(30) NOT NULL,
    language VARCHAR(10),
    source VARCHAR(50) NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (asset_id, normalized_alias, alias_type)
);

CREATE TABLE asset_provider_symbol (
    id BIGSERIAL PRIMARY KEY,
    asset_id BIGINT NOT NULL REFERENCES asset(id) ON DELETE CASCADE,
    provider VARCHAR(30) NOT NULL,
    provider_exchange_code VARCHAR(30) NOT NULL,
    provider_symbol VARCHAR(64) NOT NULL,
    source_record_id VARCHAR(100),
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    collected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE NULLS NOT DISTINCT (
        asset_id,
        provider,
        provider_exchange_code,
        provider_symbol,
        source_record_id
    )
);

CREATE TABLE asset_company_match (
    id BIGSERIAL PRIMARY KEY,
    asset_id BIGINT NOT NULL REFERENCES asset(id) ON DELETE CASCADE,
    company_id BIGINT NOT NULL REFERENCES company(id) ON DELETE CASCADE,
    match_method VARCHAR(50) NOT NULL,
    match_confidence NUMERIC(7, 6) NOT NULL CHECK (
        match_confidence >= 0 AND match_confidence <= 1
    ),
    status VARCHAR(30) NOT NULL CHECK (
        status IN ('CANDIDATE', 'CONFIRMED', 'REVIEW_REQUIRED', 'REJECTED')
    ),
    sec_source_object_id BIGINT REFERENCES source_object(id) ON DELETE RESTRICT,
    market_source_object_id BIGINT REFERENCES source_object(id) ON DELETE RESTRICT,
    evidence JSONB NOT NULL DEFAULT '{}'::JSONB,
    matched_at TIMESTAMPTZ NOT NULL,
    reviewed_at TIMESTAMPTZ,
    reviewed_by VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (asset_id, company_id)
);

CREATE UNIQUE INDEX uq_asset_provider_symbol_current_identity
    ON asset_provider_symbol (provider, provider_exchange_code, provider_symbol)
    WHERE is_current = TRUE;

CREATE UNIQUE INDEX uq_asset_provider_symbol_current_per_provider
    ON asset_provider_symbol (asset_id, provider)
    WHERE is_current = TRUE;

CREATE UNIQUE INDEX uq_asset_company_match_confirmed
    ON asset_company_match (asset_id)
    WHERE status = 'CONFIRMED';
