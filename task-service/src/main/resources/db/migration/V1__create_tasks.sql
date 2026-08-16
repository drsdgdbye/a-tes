CREATE TABLE tasks (
  id                     UUID PRIMARY KEY,
  title                  VARCHAR(500) NOT NULL,
  description            TEXT,
  status                 VARCHAR(50)  NOT NULL
                           CHECK (status IN ('open', 'completed')),
  assignee_id            UUID         NOT NULL,
  assign_fee_cents       BIGINT       NOT NULL CHECK (assign_fee_cents > 0),
  complete_reward_cents  BIGINT       NOT NULL CHECK (complete_reward_cents > 0),
  created_at             TIMESTAMPTZ  NOT NULL DEFAULT now(),
  completed_at           TIMESTAMPTZ,
  version                BIGINT       NOT NULL
);

CREATE INDEX idx_tasks_assignee ON tasks(assignee_id, status);
CREATE INDEX idx_tasks_status   ON tasks(status);