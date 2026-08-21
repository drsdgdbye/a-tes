# aTES — Awesome Task Exchange System

Таск-трекер с рандомным ассайном и корпоративным аккаунтингом: каждая задача
назначается случайному попугу (сотруднику), при ассайне списывается комиссия,
при выполнении — начисляется награда. В конце дня балансы обнуляются,
выплачиваются зарплаты, а отрицательный долг переносится на следующий. Проект
учебный — построенный на event-driven архитектуре с event sourcing, transactional
outbox, Kafka, circuit breakers и распределённым трейсингом.

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

## Сервисы

| Сервис | Порт | Роль |
|--------|------|------|
| Auth | 10001 | JWT-авторизация (ES256), регистрация, outbox |
| Gateway | 10002 | Проксирование, JWT-верификация, resilience (CB/Retry/Timeout/Bulkhead/RateLimiter) |
| TaskService | 10003 | CRUD задач, случайный ассайн, shuffle, outbox |
| Accounting | 10004 | Event store, балансы, аудитлог, ежедневные выплаты |
| Analytics | 10005 | Read-side проекции из Kafka, отчёты (доход менеджмента, попуги в минусе, дорогие задачи) |
| Notification | 10006 | Telegram-бот, consume-only (уведомления о назначении, выполнении, выплатах) |
| Frontend | 80 | nginx: статика HTML/CSS/JS + `/api/*` → Gateway |

## Инфраструктура

| Компонент | Порт | Назначение |
|-----------|------|------------|
| PostgreSQL | 5432 | 5 изолированных БД: `ates_auth`, `ates_task`, `ates_accounting`, `ates_analytics`, `ates_notification` |
| Kafka | 9092 | Шина событий: `auth.user.created`, `task.*`, `accounting.payment.processed` |
| Schema Registry | 8081 | Registry protobuf-схем |
| Jaeger | 16686 / 4317 | Распределённое трейсирование (OTLP) |
| Prometheus | 9090 | Метрики + алерт-правила |
| Grafana | 3000 | Дашборды (авто-провижининг) |

## Запуск

```bash
# Весь стек
docker compose up -d --build

# Seed-админ: admin@uberpopug.inc / admin
# Postgres: ates / ates
# Kafka: localhost:9092
```

Сервисы конфигурируются через `ATES_*` env-переменные. Основные:
- `ATES_NOTIFICATION_CHANNELS_TELEGRAM_BOT_TOKEN` — токен Telegram-бота
- `ATES_NOTIFICATION_CHANNELS_TELEGRAM_ADMIN_ADDRESSES` — telegram-id админов
- `ATES_ACCOUNTING_PAYOUT_USE_UTC=false` — interval-режим (для E2E)

Запуск отдельного сервиса:

```bash
sbt auth/run          # :10001
sbt gateway/run       # :10002
sbt taskService/run   # :10003
sbt accounting/run    # :10004
sbt analytics/run     # :10005
sbt notification/run  # :10006
```

## Тестирование

```bash
sbt test              # инкрементный (только изменённые)
sbt testFull          # полный прогон
sbt "auth/testFull"   # по модулю
sbt "e2e/testFull"    # E2E (требует Docker-образов)
sbt scalafmtAll       # форматирование
sbt scalafmtCheckAll  # проверка формата
```

## Мониторинг

- **Jaeger UI** — http://localhost:16686 (OTel javaagent, сквозной трейс)
- **Prometheus** — http://localhost:9090 (метрики + алерты)
- **Grafana** — http://localhost:3000 (дашборды: DLQ, consumer lag, Hikari pool, CircuitBreaker)

## Стек

| Уровень | Технологии |
|---------|------------|
| Язык | Scala 3.8.4, JDK 21, sbt 2.0.6 |
| Эффекты | ZIO 2, zio-http, zio-logging |
| API | tapir, zio-json |
| БД | PostgreSQL 16, Quill, Flyway, HikariCP |
| События | Kafka (zio-kafka), protobuf (scalapb), transactional outbox |
| Конфиг | zio-config (HOCON + env) |
| Resilience | rezilience (CircuitBreaker, Retry, TimeLimiter, Bulkhead, RateLimiter) |
| Авторизация | JWT (ES256, nimbus-jose-jwt), bcrypt |
| Метрики | zio-metrics-connectors (Prometheus) |
| Трейсинг | OTel javaagent 2.9.0 → Jaeger |
| Формат | scalafmt (maxColumn=120) |

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
