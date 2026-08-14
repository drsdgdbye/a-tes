-- Seed-админ. Пароль по умолчанию: admin
-- bcrypt-хэш: $2a$12$kCaYBlLxPClPHsDOyiZwq./fj07AUDEy9n4S3S3UuUhbsKu72JxKC
INSERT INTO users (id, name, email, password_hash, role)
VALUES (
  '00000000-0000-0000-0000-000000000001',
  'Admin',
  'admin@uberpopug.inc',
  '$2a$12$kCaYBlLxPClPHsDOyiZwq./fj07AUDEy9n4S3S3UuUhbsKu72JxKC',
  'admin'
);