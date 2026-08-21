CREATE TABLE users (
  user_id    UUID         PRIMARY KEY,
  name       VARCHAR(255) NOT NULL,
  role       VARCHAR(50)  NOT NULL CHECK (role IN ('popug','manager','accountant','admin')),
  created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_users_role ON users(role);
