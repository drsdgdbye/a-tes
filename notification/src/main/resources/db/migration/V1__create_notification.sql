-- Notification Service: контакты попугов по каналам доставки, журнал отправленных
-- уведомлений и идемпотентность обработки Kafka-событий.
-- Отклонение от спеки §9.5: popug_telegram обобщён в popug_contacts (мультиканальность),
-- sent_notifications дедуплицируется по (event_id, channel, address); бизнес-поля без DEFAULT (AGENTS §9).

CREATE TABLE IF NOT EXISTS popug_contacts (
    popug_id    UUID        NOT NULL,
    channel     VARCHAR(20) NOT NULL,
    address     VARCHAR(100) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (popug_id, channel)
);

-- Журнал отправки: PK (event_id, channel, address) — перед отправкой INSERT, дубликат 23505 -> skip.
-- Адрес в PK: одна доставка на (событие, канал, адресата) — админская рассылка по каналу не дублируется,
-- но несколько админов одного канала получают по одному сообщению (M-NTF-04).
CREATE TABLE IF NOT EXISTS sent_notifications (
    event_id   UUID         NOT NULL,
    popug_id   UUID         NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    channel    VARCHAR(20)  NOT NULL,
    address    VARCHAR(100) NOT NULL,
    sent_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, channel, address)
);

-- Идемпотентность обработки: PK event_id = id исходного Kafka-события.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID        PRIMARY KEY,
    event_type   VARCHAR(100) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
