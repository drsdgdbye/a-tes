-- Event store Accounting Service: поток финансовых событий, проекции баланса и пользователей,
-- идемпотентность обработки Kafka-событий и transactional outbox.

CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID        NOT NULL UNIQUE,
    event_type  TEXT        NOT NULL,
    aggregate_id UUID       NOT NULL,
    payload     BYTEA       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_aggregate_created ON events (aggregate_id, created_at);
CREATE INDEX IF NOT EXISTS idx_events_created ON events (created_at);

CREATE TABLE IF NOT EXISTS account_balances (
    user_id       UUID        PRIMARY KEY,
    balance_cents BIGINT      NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    user_id    UUID        PRIMARY KEY,
    name       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

-- Идемпотентность обработки: PK event_id = id исходного Kafka-события.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID        PRIMARY KEY,
    event_type   TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL
);

-- Transactional outbox: события для публикации в Kafka (payload — сериализованный protobuf).
CREATE TABLE IF NOT EXISTS outbox (
    id          BIGSERIAL   PRIMARY KEY,
    aggregate_id UUID       NOT NULL,
    event_type  TEXT        NOT NULL,
    payload     BYTEA       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    published   BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (published, id);