CREATE TABLE outbox (
  id           BIGSERIAL    PRIMARY KEY,
  aggregate_id UUID         NOT NULL,
  event_type   VARCHAR(100) NOT NULL,
  payload      BYTEA        NOT NULL,
  created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  published    BOOLEAN      NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_outbox_unpublished ON outbox(published, created_at)
  WHERE published = FALSE;