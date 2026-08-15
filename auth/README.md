# aTES Auth Service

Сервис аутентификации и управления пользователями платформы
aTES (UberPopug Inc Task Exchange).

## Что делает сервис

- Аутентификация по логину/паролю (`POST /auth/login`) с выдачей пары токенов:
  access JWT (15 минут, ES256) и refresh-токен (7 дней, UUIDv4).
- Саморегистрация (`POST /auth/register`) с ролью `popug` и auto-login; управляется
  конфиг-флагом (`GET /auth/config` сообщает фронту, включена ли она).
- Обновление сессии (`POST /auth/refresh`) с ротацией refresh-токена.
- Завершение сессии (`POST /auth/logout`) — отзыв refresh-токена.
- Смена собственного пароля (`POST /users/me/password`) и сброс пароля админом
  (`PATCH /users/{id}/password`) с инкрементом версии учётных данных.
- Управление пользователями: создание, просмотр, список, изменение роли/статуса
  (админ-операции, self-disable запрещён).
- Публикация доменного события `UserCreated` в Kafka-топик `auth.user.created`
  через transactional outbox.
- Публикация публичного JWK-ключа (`GET /auth/keys`) для локальной верификации
  JWT на стороне Gateway.

## Технологический стек

| Компонент | Библиотека |
|-----------|------------|
| Язык | Scala 3.8.4 |
| Эффекты | ZIO 2 (zio, zio-streams, zio-kafka, zio-logging) |
| HTTP | tapir + zio-http, zio-json |
| Конфиг | zio-config (HOCON + env-оверрайды) |
| ORM | Quill (quill-jdbc-zio) |
| Миграции | Flyway |
| БД | PostgreSQL |
| JWT | nimbus-jose-jwt (ES256, эфемерный ключ при старте) |
| Пароли | bcrypt (at.favre.lib) |
| Сообщения | Kafka (protobuf-схемы в модуле `common`) |

## Структура проекта

```
auth/
├── README.md
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   ├── application.conf            # конфигурация (HOCON)
│   │   │   └── db/migration/               # Flyway V1..V5
│   │   └── scala/inc/uberpopug/auth/
│   │       ├── Main.scala                  # точка входа, сборка ZLayer-графа
│   │       ├── api/                        # tapir-эндпоинты, DTO, маппинг ошибок
│   │       ├── service/                    # use case'ы: AuthService, TokenService, ...
│   │       ├── repository/                 # интерфейсы + Quill-реализации
│   │       ├── db/                         # HikariCP, Quill-контекст, Flyway
│   │       ├── domain/                     # доменная модель и валидация
│   │       └── config/                     # case classes конфига
│   └── test/scala/inc/uberpopug/auth/      # ZIO Test: домен, hasher, токены, сервис
```

Слои зависят строго внутрь: `api → service → repository → db`, `domain` — чистые
функции без эффектов. Общие типы (`UserId`, `DomainError`, protobuf-схемы) — в
модуле `common` на верхнем уровне репозитория.

## Запуск

Требования: JDK 21, sbt 2.0.6, работающие Postgres и Kafka.

Через docker-compose (из корня репозитория):

```bash
docker-compose up -d postgres kafka
sbt auth/Docker/publishLocal
docker-compose up -d auth
```

Локально из IDE или sbt:

```bash
sbt auth/run
```

По умолчанию сервис слушает порт `10001`, БД — `jdbc:postgresql://localhost:5432/ates`,
Kafka — `localhost:29092`. При первом старте Flyway создаёт схему и seed-админа.

## Конфигурация

Все параметры в `auth/src/main/resources/application.conf`, секция `ates`.
Каждое значение можно переопределить env-переменной.

| Параметр | Env | По умолчанию |
|----------|-----|--------------|
| `ates.server.port` | `ATES_SERVER_PORT` | `10001` |
| `ates.database.url` | `ATES_DB_URL` | `jdbc:postgresql://localhost:5432/ates` |
| `ates.database.user` | `ATES_DB_USER` | `ates` |
| `ates.database.password` | `ATES_DB_PASSWORD` | `ates` |
| `ates.database.maxPoolSize` | `ATES_DB_MAX_POOL` | `10` |
| `ates.kafka.bootstrapServers` | `ATES_KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` |
| `ates.kafka.topicUserCreated` | `ATES_KAFKA_TOPIC_USER_CREATED` | `auth.user.created` |
| `ates.jwt.issuer` | `ATES_JWT_ISSUER` | `ates-auth` |
| `ates.jwt.accessTtlSeconds` | `ATES_JWT_ACCESS_TTL` | `900` |
| `ates.jwt.refreshTtlSeconds` | `ATES_JWT_REFRESH_TTL` | `604800` |
| `ates.outbox.batchSize` | `ATES_OUTBOX_BATCH_SIZE` | `50` |
| `ates.outbox.pollIntervalSeconds` | `ATES_OUTBOX_POLL_INTERVAL` | `2` |
| `ates.auth.registrationEnabled` | `ATES_AUTH_REGISTRATION` | `true` |

## API

Все эндпоинты объявлены декларативно через tapir; OpenAPI генерируется из кода.
Защищённые эндпоинты требуют заголовок `Authorization: Bearer <accessToken>`.

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| GET | `/health` | public | Liveness-проверка |
| GET | `/ready` | public | Readiness-проверка (в текущей версии без проверки БД) |
| POST | `/auth/login` | public | Логин, выдача токенов |
| POST | `/auth/register` | public | Саморегистрация (роль `popug`, auto-login) |
| GET | `/auth/config` | public | Публичные capabilities (регистрация включена?) |
| POST | `/auth/refresh` | public | Ротация refresh-токена, выдача новой пары |
| POST | `/auth/logout` | public | Отзыв refresh-токена |
| GET | `/auth/keys` | public | Публичный JWK (ES256) |
| POST | `/users` | admin | Создание пользователя |
| GET | `/users` | admin | Список пользователей (limit/offset) |
| GET | `/users/{id}` | любой с JWT | Пользователь по id |
| PATCH | `/users/{id}` | admin | Изменение роли/статуса |
| POST | `/users/me/password` | любой с JWT | Смена своего пароля (с проверкой старого) |
| PATCH | `/users/{id}/password` | admin | Сброс пароля пользователя |

### Примеры

Логин:

```bash
curl -s localhost:10001/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"login":"admin@uberpopug.inc","password":"admin"}'
```

```json
{
  "accessToken": "<jwt>",
  "refreshToken": "0b6a8f5e-...",
  "tokenType": "Bearer",
  "expiresIn": 900
}
```

Создание пользователя (с полученным access-токеном):

```bash
curl -s localhost:10001/users \
  -H "Authorization: Bearer <accessToken>" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Popug One","email":"popug@ates.io","password":"secret","role":"popug"}'
```

```json
{
  "id": "7d2f5c1a-...",
  "name": "Popug One",
  "email": "popug@ates.io",
  "role": "popug",
  "status": "active",
  "createdAt": "2026-01-01T00:00:00Z",
  "updatedAt": "2026-01-01T00:00:00Z"
}
```

Публичный ключ:

```bash
curl -s localhost:10001/auth/keys
```

```json
{
  "keys": [
    {
      "kid": "ates-auth-1a2b3c4d",
      "kty": "EC",
      "crv": "P-256",
      "x": "<base64url>",
      "y": "<base64url>",
      "alg": "ES256",
      "use": "sig"
    }
  ]
}
```

### Ошибки

Тело ошибки — единый формат `{ "error": "<код>", "message": "<описание>" }`.

| Код | HTTP | Ситуация |
|-----|------|----------|
| `VALIDATION_ERROR` | 400 | Некорректные входные данные |
| `UNAUTHORIZED` | 401 | Неверные креды, невалидный/истёкший токен |
| `NOT_FOUND` | 404 | Пользователь не найден |
| `BUSINESS_RULE_VIOLATION` | 409 | Email занят, self-disable, конфликт |
| `FORBIDDEN` | 403 | Недостаточно прав, пользователь отключён |
| `INTERNAL_ERROR` | 500 | Ошибка персистентности |

## Seed-админ

При миграции создаётся администратор:

- email: `admin@uberpopug.inc`
- пароль: `admin`

Пароль зашифрован bcrypt (cost 12). В проде пароль нужно сменить.

## Схема БД

- `users` — пользователи (id, name, email UNIQUE, password_hash, role, status,
  version — версия учётных данных, инкрементируется при смене пароля).
- `refresh_tokens` — issued refresh-токены (хранится SHA-256-хэш, не сам токен;
  `version` — снапшот версии на момент выдачи, несовпадение → 401).
- `outbox` — transactional outbox для событий `UserCreated`.
- `processed_events` — дедупликация полученных событий (зарезервировано).

## Тесты

```bash
sbt "auth/Test/testFull"
```

Покрытие: доменная валидация, bcrypt (hash/verify), TokenService (JWT, expiry,
TTL refresh) и AuthService (логин, ротация, админ-права, отзыв токенов) на
in-memory репозиториях.

## Известные особенности

- ES256-ключ генерируется при каждом старте (эфемерный): после рестарта сервиса
  ранее выданные access-токены становятся невалидными. В roadmap — хранение ключа.
- `processed_events` создана для дедупликации при интеграции с другими сервисами,
  consumer-ы появятся на следующих этапах.