# aTES — UberPopug Inc Task Exchange System

Монорепозиторий с 6 микросервисами (Scala 3, ZIO 2, tapir, Quill, Kafka, PostgreSQL).

## Архитектура

```
                    ┌─────────────────────────────────────────────┐
                    │              Frontend (:80)                 │
                    │  nginx: статика HTML/CSS/JS, /api/* proxy   │
                    └────────────────────┬────────────────────────┘
                                         │ /api/*
                    ┌────────────────────▼────────────────────────┐
                    │           Gateway (:10002)                  │
                    │  JWT-верификация, проксирование по путям    │
                    │  Resilience (CB/Retry/Timeout/Bulkhead/RL)  │
                    └──┬──────────────┬──────────────────┬────────┘
                       │ /auth/*      │ /tasks/*         │ /accounts/*, /analytics/*
          ┌────────────▼───────┐  ┌───▼──────────────┐  ┌─────────────────────┐
          │  Auth (:10001)     │  │ TaskService (:10003)│  │ Accounting (:10004) │
          │  JWT (ES256), outbox│  │ CRUD + outbox    │  │ Event store, outbox │
          └────────┬───────────┘  └──────┬───────────┘  └────────┬────────────┘
                   │ Kafka events        │ Kafka events          │ Kafka events
                   ▼                     ▼                       ▼
          ┌───────────────────────────────────────────────────────────────────┐
          │                          Kafka                                    │
          │  auth.user.created | task.* | accounting.payment.processed        │
          └──────┬─────────────────────────────┬─────────────────────────────┘
                 │                             │
      ┌──────────▼──────────┐       ┌──────────▼──────────┐
      │  Analytics (:10005) │       │  Notification (:10006)│
      │  Read-side проекции │       │  Telegram-бот         │
      └─────────────────────┘       └─────────────────────┘
```

## Сервисы и порты

| Сервис        | Порт   | Описание                        | sbt id        |
|--------------|--------|---------------------------------|---------------|
| Auth         | 10001  | JWT (ES256), регистрация, outbox| `auth`        |
| Gateway      | 10002  | Проксирование, resilience       | `gateway`     |
| TaskService  | 10003  | CRUD задач, shuffle, outbox     | `taskService` |
| Accounting   | 10004  | Event store, выплаты            | `accounting`  |
| Analytics    | 10005  | Read-side проекции, отчёты      | `analytics`   |
| Notification | 10006  | Telegram-бот, consume-only      | `notification`|
| Frontend     | 80     | nginx (статика + /api/* proxy)  | —             |

## Инфраструктура

| Сервис          | Порт      | Описание                            |
|----------------|-----------|-------------------------------------|
| PostgreSQL      | 5432      | 5 БД: ates_auth, ates_task, ates_accounting, ates_analytics, ates_notification |
| Kafka           | 9092      | Топики: auth.user.created, task.*, accounting.payment.processed |
| Schema Registry | 8081      | Avro/Protobuf registry              |
| Jaeger          | 16686 (UI), 4317 (OTLP) | Распределённое трейсирование |
| Prometheus      | 9090      | Метрики + алерты                    |
| Grafana         | 3000      | Дашборды (авто-провижининг)         |

## Запуск

### Все сервисы

```bash
docker compose up -d --build
```

Зависимости: Postgres, Kafka, ZooKeeper, Schema Registry, Jaeger, Prometheus, Grafana + 6 сервисов + frontend.

### Ключевые cred-ы

- **Auth seed-админ**: `admin@uberpopug.inc` / `admin`
- **Postgres**: `ates` / `ates` (хост: `localhost:5432`)
- **Kafka**: `localhost:9092`

### Переменные окружения (env)

Все сервисы используют `ATES_*` env-переменные для конфигурации. Основные:

- `ATES_AUTH_PORT=10001`, `ATES_GATEWAY_PORT=10002`, ...
- `ATES_NOTIFICATION_CHANNELS_TELEGRAM_BOT_TOKEN` — токен Telegram-бота (обязательно для notification)
- `ATES_NOTIFICATION_CHANNELS_TELEGRAM_ADMIN_ADDRESSES` — telegram-id админов через запятую
- `ATES_ACCOUNTING_PAYOUT_USE_UTC=false` — для E2E-тестов (interval-режим)

### Запуск отдельного сервиса (sbt)

```bash
sbt auth/run          # Auth :10001
sbt gateway/run       # Gateway :10002
sbt taskService/run   # TaskService :10003
sbt accounting/run    # Accounting :10004
sbt analytics/run     # Analytics :10005
sbt notification/run  # Notification :10006
```

## Тестирование

```bash
# Юнит-тесты (增量 в sbt 2.x — только изменённые)
sbt test

# Полный прогон (все тесты)
sbt testFull

# По модулю
sbt "auth/testFull"
sbt "gateway/testFull"
sbt "taskService/testFull"
sbt "accounting/testFull"
sbt "analytics/testFull"
sbt "notification/testFull"

# E2E (требует Docker-образов)
sbt "e2e/testFull"

# Форматирование
sbt scalafmtAll
sbt scalafmtCheckAll
```

## Мониторинг

- **Jaeger UI**: http://localhost:16686 — распределённое трейсирование (OTel javaagent)
- **Prometheus**: http://localhost:9090 — метрики + алерт-правила
- **Grafana**: http://localhost:3000 — дашборды (автоматический провижининг)

### Кастомные метрики

| Метрика                       | Сервисы                          | Описание                           |
|------------------------------|----------------------------------|------------------------------------|
| `dlq_messages_total`         | accounting, analytics, notification | Счётчик событий в DLQ            |
| `kafka_consumer_lag`         | accounting, analytics, notification, task-service | Задержка consumer'а |
| `hikari_pool_active_connections` | task-service, accounting, analytics, notification | Активные соединения БД |
| `rezilience_circuit_breaker_state` | gateway | Состояние CB (0=closed, 1=half-open, 2=open) |

### Алерт-правила

- `DLQNonEmpty` — сообщения в DLQ > 0
- `KafkaConsumerLagHigh` — задержка consumer > 100
- `CircuitBreakerOpen` — CB в состоянии open

## Стек

- Scala 3.8.4, sbt 2.0.6, JDK 21
- ZIO 2, tapir, zio-http, zio-json, zio-config, zio-kafka, zio-logging
- Quill (Postgres), Flyway, HikariCP
- scalapb (protobuf), rezilience (CB/Retry/Timeout/Bulkhead/RateLimiter)
- OTel javaagent 2.9.0 (Jaeger exporter)

## Структура модулей

```
common/           — Protobuf-схемы, общие типы, DomainError
auth/             — JWT, регистрация, outbox
gateway/          — Проксирование, resilience
task-service/     — CRUD задач, shuffle, outbox
accounting/       — Event store, выплаты
analytics/        — Read-side проекции
notification/     — Telegram-бот
frontend/         — nginx + статика
e2e/              — End-to-end тесты (testcontainers)
```
