-- ============================================================
-- Fraud Platform — PostgreSQL schema
-- Partitioned by month on hot tables; audit_log is append-only.
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ------------------------------------------------ customers
CREATE TABLE IF NOT EXISTS customer_profiles (
    customer_id      TEXT PRIMARY KEY,
    tenure_days      INT     NOT NULL DEFAULT 0,
    risk_band        TEXT    NOT NULL DEFAULT 'LOW'
                     CHECK (risk_band IN ('LOW','MEDIUM','HIGH')),
    avg_ticket       NUMERIC(14,2) NOT NULL DEFAULT 0,
    std_ticket       NUMERIC(14,2) NOT NULL DEFAULT 0,
    home_country     CHAR(2) NOT NULL DEFAULT 'IN',
    kyc_verified     BOOLEAN NOT NULL DEFAULT TRUE,
    chargebacks_12m  INT     NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ------------------------------------------------ transactions (partitioned)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id    TEXT        NOT NULL,
    card_id           TEXT        NOT NULL,
    account_id        TEXT        NOT NULL,
    customer_id       TEXT        NOT NULL,
    merchant_id       TEXT        NOT NULL,
    merchant_category TEXT,
    merchant_country  CHAR(2),
    amount            NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    currency          CHAR(3)     NOT NULL,
    channel           TEXT        NOT NULL,
    device_id         TEXT,
    ip_address        INET,
    latitude          DOUBLE PRECISION,
    longitude         DOUBLE PRECISION,
    occurred_at       TIMESTAMPTZ NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_id, occurred_at)
) PARTITION BY RANGE (occurred_at);

CREATE TABLE IF NOT EXISTS transactions_default PARTITION OF transactions DEFAULT;

CREATE INDEX IF NOT EXISTS ix_txn_card_time
    ON transactions (card_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_txn_customer_time
    ON transactions (customer_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_txn_merchant
    ON transactions (merchant_id);

-- ------------------------------------------------ decisions
CREATE TABLE IF NOT EXISTS decisions (
    transaction_id  TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL,
    card_id         TEXT NOT NULL,
    amount          NUMERIC(14,2),
    currency        CHAR(3),
    risk_score      NUMERIC(5,2) NOT NULL,
    decision        TEXT NOT NULL
                    CHECK (decision IN ('APPROVE','CHALLENGE','REVIEW','DECLINE')),
    rules_score     NUMERIC(5,2),
    ml_score        NUMERIC(5,2),
    graph_score     NUMERIC(5,2),
    reasons         JSONB NOT NULL DEFAULT '[]',
    missing_signals TEXT[] DEFAULT '{}',
    model_version   TEXT,
    policy_version  TEXT,
    latency_ms      NUMERIC(10,2),
    decided_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dec_score ON decisions (risk_score DESC);
CREATE INDEX IF NOT EXISTS ix_dec_time  ON decisions (decided_at DESC);

-- ------------------------------------------------ cases
CREATE TABLE IF NOT EXISTS cases (
    case_id        TEXT PRIMARY KEY,
    transaction_id TEXT UNIQUE NOT NULL,
    customer_id    TEXT NOT NULL,
    priority       TEXT NOT NULL DEFAULT 'P2' CHECK (priority IN ('P1','P2','P3')),
    risk_score     NUMERIC(5,2) NOT NULL,
    status         TEXT NOT NULL DEFAULT 'OPEN'
                   CHECK (status IN ('OPEN','IN_REVIEW','CLOSED')),
    analyst        TEXT,
    note           TEXT,
    reasons        JSONB NOT NULL DEFAULT '[]',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_case_queue ON cases (status, priority, risk_score DESC);

-- ------------------------------------------------ audit (append only)
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id       TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    actor          TEXT NOT NULL DEFAULT 'system',
    payload        JSONB NOT NULL,
    prev_hash      TEXT,
    hash           TEXT NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_audit_txn ON audit_log (transaction_id, occurred_at);

-- Block UPDATE and DELETE at the database level: the audit trail is immutable.
CREATE OR REPLACE FUNCTION audit_is_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_immutable ON audit_log;
CREATE TRIGGER trg_audit_immutable
    BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION audit_is_immutable();

-- ------------------------------------------------ monthly partition helper
CREATE OR REPLACE FUNCTION ensure_txn_partition(month_start DATE)
RETURNS void AS $$
DECLARE
    pname TEXT := 'transactions_' || to_char(month_start, 'YYYY_MM');
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF transactions
         FOR VALUES FROM (%L) TO (%L)',
        pname, month_start, month_start + INTERVAL '1 month');
END;
$$ LANGUAGE plpgsql;

SELECT ensure_txn_partition(date_trunc('month', now())::date);
SELECT ensure_txn_partition((date_trunc('month', now()) + INTERVAL '1 month')::date);
