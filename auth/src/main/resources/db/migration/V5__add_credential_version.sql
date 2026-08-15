-- Контроль версий учётных данных: версия инкрементируется при каждой смене пароля.
-- Refresh-токены снапшотят версию на момент выдачи; при несовпадении с текущей — 401.
ALTER TABLE users ADD COLUMN version INT NOT NULL DEFAULT 0;
ALTER TABLE refresh_tokens ADD COLUMN version INT NOT NULL DEFAULT 0;