# aTES Accounting Service

Бухгалтерия платформы aTES (UberPopug Inc Task Exchange): event store финансовых
операций попугов, балансы, аудитлог, доход менеджмента, ежедневные выплаты.
События принимаются из Kafka (task-service / auth), выплаты публикуются обратно
через transactional outbox.

## Что делает сервис

- Event store: финансовые события по счёту попуга (`TaskPriceRecorded` —
  цены задачи, `AccountDebited` — списание AssignFee, `AccountCredited` —
  возврат AssignFee / начисление CompleteReward, `AccountPayout` — выплата).
  Баланс — проекция суммы `deltaCents` событий (инвариант event store).
- Kafka consumer `auth.user.created`, `task.created`, `task.assigned`,
  `task.completed` (protobuf): наполняет event store, проекцию пользователей и
  балансы. Идемпотентность через `processed_events` (PK = id исходного
  Kafka-события, SQLState 23505).
- Poison-pill → DLQ: невалидный protobuf, событие для несуществующего счёта,
  нарушение бизнес-правила публикуются в `internal.dead-letter`
  (`DeadLetterRecord`); транзиентные ошибки роняют поток — consumer
  переподписывается заново.
- Ежедневные выплаты: cron (QUARTZ, UTC) или фиксированный интервал; для
  каждого счёта `amount = max(0, balance)`, положительный баланс обнуляется,
  отрицательный (долг) сохраняется. `PaymentProcessed` пишется в outbox в той
  же транзакции, что и `AccountPayout`. Детерминированный `event_id`
  (`UUID.nameUUIDFromBytes("userId:date")`) делает повторный запуск за ту же
  дату идемпотентным. Срез по времени: события, пришедшие во время выплаты,
  уходят на следующий день.
- Transactional outbox: события выплат публикуются релеем в Kafka
  (`accounting.payment.processed`).
- API (через Gateway): баланс, аудитлог, доход менеджмента и ежедневная
  статистика.
- Аутентификация делегирована Gateway: identity-заголовки
  `X-Auth-User-Id` / `X-Auth-User-Role` (антиспуфинг — как в task-service).
- `GET /health`, `GET /ready`, `GET /metrics` (Prometheus).

## Технологический стек

| Компонент | Библиотека |
|-----------|------------|
| Язык | Scala 3.8.4 |
| Эффекты | ZIO 2 (zio, zio-http, zio-logging) |
| БД | Postgres + Quill (zio-jdbc), HikariCP |
| Миграции | Flyway |
| Kafka | zio-kafka (consumer/producer), протокол protobuf |
| Планировщик | zio-cron (`io.github.jkobejs`, QUARTZ) |
| API | tapir (endpoints) + zio-http (server) |
| Сериализация | zio-json (`DeriveJsonCodec.gen`) |
| Конфиг | zio-config (HOCON + env-оверрайды) |
| Метрики | zio-metrics-connectors (Prometheus) |

## Структура проекта

```
accounting/
├── README.md
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   ├── application.conf            # конфигурация (HOCON)
│   │   │   └── db/migration/               # Flyway V1
│   │   └── scala/inc/uberpopug/accounting/
│   │       ├── Main.scala                  # точка входа, сборка ZLayer-графа
│   │       ├── api/                        # DTO, tapir-эндпоинты, маппинг ошибок
│   │       ├── service/                    # AccountingService, Consumer, EventProcessor, PayoutService/Scheduler, OutboxRelay
│   │       ├── repository/                 # EventStore, OutboxRepository
│   │       ├── domain/                     # AccountEvent, AuditLogEntry, BalanceCalculator, PayoutCalculator, DailyStats
│   │       ├── db/                         # DataSource, Quill Context, Flyway
│   │       └── config/                     # case classes конфига
│   └── test/scala/inc/uberpopug/accounting/  # ZIO Test (M-ACC-01..24 + property-based)
```

Слои зависят строго внутрь: `api → service → repository → db/domain`,
`domain` не знает об инфраструктуре.

## Запуск

Требования: JDK 21, sbt 2.0.6, запущенные Postgres и Kafka.

Через docker-compose (из корня репозитория):

```bash
docker-compose up -d postgres kafka
sbt accounting/Docker/publishLocal
docker-compose up -d accounting
```

Локально из IDE или sbt:

```bash
sbt accounting/run
```

По умолчанию сервис слушает порт `10004`, БД `ates_accounting`, Kafka — `localhost:29092`.
Доступ через Gateway (`http://localhost:10002/accounts*`) с `Authorization: Bearer`.

## Конфигурация

Все параметры в `accounting/src/main/resources/application.conf`, секция `ates`.
Каждое значение можно переопределить env-переменной.

| Параметр | Env | По умолчанию |
|----------|-----|--------------|
| `ates.server.port` | `ATES_SERVER_PORT` | `10004` |
| `ates.database.url` | `ATES_DB_URL` | `jdbc:postgresql://localhost:5432/ates_accounting` |
| `ates.database.user` | `ATES_DB_USER` | `ates` |
| `ates.database.password` | `ATES_DB_PASSWORD` | `ates` |
| `ates.database.maxPoolSize` | `ATES_DB_MAX_POOL` | `10` |
| `ates.kafka.bootstrapServers` | `ATES_KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` |
| `ates.kafka.consumerGroupId` | `ATES_KAFKA_CONSUMER_GROUP` | `ates-accounting` |
| `ates.kafka.topicUserCreated` | `ATES_KAFKA_TOPIC_USER_CREATED` | `auth.user.created` |
| `ates.kafka.topicTaskCreated` | `ATES_KAFKA_TOPIC_TASK_CREATED` | `task.created` |
| `ates.kafka.topicTaskAssigned` | `ATES_KAFKA_TOPIC_TASK_ASSIGNED` | `task.assigned` |
| `ates.kafka.topicTaskCompleted` | `ATES_KAFKA_TOPIC_TASK_COMPLETED` | `task.completed` |
| `ates.kafka.topicPaymentProcessed` | `ATES_KAFKA_TOPIC_PAYMENT_PROCESSED` | `accounting.payment.processed` |
| `ates.kafka.topicDlq` | `ATES_KAFKA_TOPIC_DLQ` | `internal.dead-letter` |
| `ates.outbox.batchSize` | `ATES_OUTBOX_BATCH_SIZE` | `50` |
| `ates.outbox.pollIntervalSeconds` | `ATES_OUTBOX_POLL_INTERVAL` | `2` |
| `ates.payout.useUtc` | `ATES_PAYOUT_USE_UTC` | `true` |
| `ates.payout.cronExpression` | `ATES_PAYOUT_CRON` | `0 0 23 * * ?` (QUARTZ, UTC) |
| `ates.payout.intervalSeconds` | `ATES_PAYOUT_INTERVAL` | `86400` |

## API

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| GET | `/health` | public | Liveness-проверка |
| GET | `/ready` | public | Readiness (БД/Kafka) |
| GET | `/metrics` | public | Метрики Prometheus |
| GET | `/accounts/me/balance` | все | Текущий баланс счёта |
| GET | `/accounts/me/audit-log` | все | Страница аудитлога (`limit`/`offset`) |
| GET | `/accounts/top-management-earnings?date=YYYY-MM-DD` | admin, accountant | Доход менеджмента за день |
| GET | `/accounts/daily-stats?from&to=YYYY-MM-DD` | admin, accountant | Ежедневная статистика за диапазон |

Identity передаётся заголовками `X-Auth-User-Id` / `X-Auth-User-Role`
(инжектятся Gateway, проверяются в `AccountingServerLogic`).

### Ошибки

Тело ошибки — единый формат `{ "error": "<код>", "message": "<описание>" }`.

| Код | HTTP | Ситуация |
|-----|------|----------|
| `VALIDATION_ERROR` | 400 | Невалидный id/дата |
| `FORBIDDEN` | 403 | Недостаточно прав (отчёты) |
| `NOT_FOUND` | 404 | Счёт не найден |
| `INTERNAL_ERROR` | 500 | Ошибка персистентности |

## Тесты

```bash
sbt "accounting/Test/testFull"
```

Покрытие (M-ACC-01..24 + property-based):
- маппинг Kafka-событий в event store, идемпотентность, poison → AccountNotFound;
- баланс, отрицательный баланс, аудитлог (типы операций, сортировка, пагинация);
- доход менеджмента и ежедневная статистика (включая 10+ событий в день);
- выплаты: сумма, обнуление, долг, срез по времени, outbox, идемпотентность cron;
- доменные политики `BalanceCalculator` / `PayoutCalculator` /
  `ManagementEarnings` / `DailyStatsCalculator` — включая property-based проверки
  (сумма дельт = баланс, `max(0, balance)`, отрицательные счета в статистике).

## Известные особенности

- До первого события `auth.user.created` счёт попуга не существует — `404 NOT_FOUND`.
- `GET /metrics` — отдельная zio-http-route в `Main` (как в gateway), не через tapir.
- При рестарте сервиса детерминированный `event_id` выплаты не даёт двойной
  выплаты за ту же дату (идемпотентность в транзакции event store).