-- Read-side проекции Analytics Service: задачи, балансы попугов, ежедневная статистика
-- и идемпотентность обработки Kafka-событий.
-- Отклонение от спеки §9.4: бизнес-поля без DEFAULT (AGENTS §9) — значения задаёт домен/сервис.

CREATE TABLE IF NOT EXISTS tasks (
    task_id               UUID        PRIMARY KEY,
    title                 VARCHAR(500) NOT NULL,
    assign_fee_cents      BIGINT      NOT NULL,
    complete_reward_cents BIGINT      NOT NULL,
    status                VARCHAR(50) NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL,
    completed_at          TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_tasks_completed ON tasks (completed_at);
CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks (created_at);

CREATE TABLE IF NOT EXISTS popug_balances (
    user_id       UUID        PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    balance_cents BIGINT      NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS daily_stats (
    date                           DATE        PRIMARY KEY,
    top_management_earnings_cents  BIGINT      NOT NULL,
    popugs_total                   INT         NOT NULL,
    popugs_negative                INT         NOT NULL,
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Идемпотентность обработки: PK event_id = id исходного Kafka-события.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID        PRIMARY KEY,
    event_type   VARCHAR(100) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
