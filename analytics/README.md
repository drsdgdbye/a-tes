# aTES Analytics Service

Read-side сервис аналитики платформы aTES (UberPopug Inc Task Exchange):
агрегирует статистику из Kafka-событий (task-service / auth / accounting) в
проекции `tasks`, `popug_balances`, `daily_stats`. Данные — только проекции,
собственного бизнеса у сервиса нет (нужен для дашборда аналитики в UI).

## Что делает сервис

- Kafka consumer (`auth.user.created`, `task.created`, `task.assigned`,
  `task.completed`, `accounting.payment.processed`, protobuf): наполняет
  проекции. Идемпотентность через `processed_events` (PK = id исходного
  Kafka-события, SQLState 23505), каждое событие обрабатывается в своей
  транзакции.
- `tasks`: задачи с ценами и статусом — база для «самой дорогой задачи».
  `TaskCreated` вставляет строку, `TaskCompleted` обновляет `status`/`completedAt`.
- `popug_balances`: текущий баланс попуга. `TaskAssigned` списывает assignFee
  с нового исполнителя и возвращает его старому (refund), `TaskCompleted`
  начисляет completeReward, `PaymentProcessed` списывает выплату и обновляет
  имя. Баланс не является SSOT финансов (это accounting) — read-side проекция
  для «попугов в минусе».
- `daily_stats`: ежедневная агрегация. `TaskCreated` добавляет
  `assignFee − completeReward` в доход менеджмента за дату создания,
  `UserCreated` увеличивает `popugs_total`, `PaymentProcessed` пересчитывает
  `popugs_negative` (снимок «на момент обработки выплаты»).
- Poison-pill → DLQ: невалидный protobuf / данные (`InvalidValue`,
  `BusinessRuleViolation`) публикуются в `analytics.dlq` (`DeadLetterRecord`);
  транзиентные ошибки (включая событие для ещё не существующего попуга — при
  catch-up `auth.user.created` приходит позже) роняют поток: consumer
  переподписывается с exponential backoff и событие переобрабатывается.
- API (через Gateway, только admin): доход менеджмента за период, попуги в
  минусе, самая дорогая задача за день/неделю/месяц.
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
| Kafka | zio-kafka (consumer), протокол protobuf |
| API | tapir (endpoints) + zio-http (server) |
| Сериализация | zio-json (`DeriveJsonCodec.gen`) |
| Конфиг | zio-config (HOCON + env-оверрайды) |
| Метрики | zio-metrics-connectors (Prometheus) |

## Структура проекта

```
analytics/
├── README.md
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   ├── application.conf            # конфигурация (HOCON)
│   │   │   └── db/migration/               # Flyway V1
│   │   └── scala/inc/uberpopug/analytics/
│   │       ├── Main.scala                  # точка входа, сборка ZLayer-графа
│   │       ├── api/                        # DTO, tapir-эндпоинты, маппинг ошибок
│   │       ├── service/                    # AnalyticsService, Consumer, EventProcessor
│   │       ├── repository/                 # AnalyticsStore
│   │       ├── domain/                     # Role, TaskStatus, AnalyticsPeriod, DailyStat
│   │       ├── db/                         # DataSource, Quill Context, Flyway
│   │       └── config/                     # case classes конфига
│   └── test/scala/inc/uberpopug/analytics/  # ZIO Test (M-ANL-01..14 + property-based)
```

Слои зависят строго внутрь: `api → service → repository → db/domain`,
`domain` не знает об инфраструктуре.

## Запуск

Требования: JDK 21, sbt 2.0.6, запущенные Postgres и Kafka.

Через docker-compose (из корня репозитория):

```bash
docker-compose up -d postgres kafka
sbt analytics/Docker/publishLocal
docker-compose up -d analytics
```

Локально из IDE или sbt:

```bash
sbt analytics/run
```

По умолчанию сервис слушает порт `10005`, БД `ates_analytics`, Kafka — `localhost:29092`.
Доступ через Gateway (`http://localhost:10002/analytics*`) с `Authorization: Bearer`.

## Конфигурация

Все параметры в `analytics/src/main/resources/application.conf`, секция `ates`.
Каждое значение можно переопределить env-переменной.

| Параметр | Env | По умолчанию |
|----------|-----|--------------|
| `ates.server.port` | `ATES_SERVER_PORT` | `10005` |
| `ates.database.url` | `ATES_DB_URL` | `jdbc:postgresql://localhost:5432/ates_analytics` |
| `ates.database.user` | `ATES_DB_USER` | `ates` |
| `ates.database.password` | `ATES_DB_PASSWORD` | `ates` |
| `ates.database.maxPoolSize` | `ATES_DB_MAX_POOL` | `10` |
| `ates.kafka.bootstrapServers` | `ATES_KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` |
| `ates.kafka.consumerGroupId` | `ATES_KAFKA_CONSUMER_GROUP` | `ates-analytics` |
| `ates.kafka.topicUserCreated` | `ATES_KAFKA_TOPIC_USER_CREATED` | `auth.user.created` |
| `ates.kafka.topicTaskCreated` | `ATES_KAFKA_TOPIC_TASK_CREATED` | `task.created` |
| `ates.kafka.topicTaskAssigned` | `ATES_KAFKA_TOPIC_TASK_ASSIGNED` | `task.assigned` |
| `ates.kafka.topicTaskCompleted` | `ATES_KAFKA_TOPIC_TASK_COMPLETED` | `task.completed` |
| `ates.kafka.topicPaymentProcessed` | `ATES_KAFKA_TOPIC_PAYMENT_PROCESSED` | `accounting.payment.processed` |
| `ates.kafka.topicDlq` | `ATES_KAFKA_TOPIC_DLQ` | `analytics.dlq` |

## API

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| GET | `/health` | public | Liveness-проверка |
| GET | `/ready` | public | Readiness (БД/Kafka) |
| GET | `/metrics` | public | Метрики Prometheus |
| GET | `/analytics/top-management-earnings?from&to=YYYY-MM-DD` | admin | Доход менеджмента за диапазон дат |
| GET | `/analytics/popugs-in-minus` | admin | Попуги с отрицательным балансом |
| GET | `/analytics/most-expensive-task?period=day\|week\|month&date=YYYY-MM-DD` | admin | Самая дорогая закрытая задача за период |

Окно периода — скользящее от опорной даты: `day = [D, D]`, `week = [D, D+6]`,
`month = [D, D+29]`. `items` — самая дорогая задача за каждый день периода,
`overall` — самая дорогая за весь период.

Identity передаётся заголовками `X-Auth-User-Id` / `X-Auth-User-Role`
(инжектятся Gateway, проверяются в `AnalyticsServerLogic`).

### Ошибки

Тело ошибки — единый формат `{ "error": "<код>", "message": "<описание>" }`.

| Код | HTTP | Ситуация |
|-----|------|----------|
| `VALIDATION_ERROR` | 400 | Невалидный id/дата/период |
| `FORBIDDEN` | 403 | Недостаточно прав (не admin) |
| `INTERNAL_ERROR` | 500 | Ошибка персистентности |

## Тесты

```bash
sbt "analytics/Test/testFull"
```

Покрытие (M-ANL-01..14 + property-based):
- маппинг Kafka-событий в проекции `tasks` / `popug_balances` / `daily_stats`,
  идемпотентность (duplicate event), событие для несуществующего попуга →
  транзиентная ошибка, out-of-order события;
- доход менеджмента: `sum(assignFee) − sum(completeReward)` за период, пустой
  диапазон;
- попуги в минусе: только отрицательный баланс, пустой список не ошибка;
- самая дорогая задача: period=day/week/month, пустой период, детерминизм
  при равенстве reward;
- доступ: не-admin → 403, admin → 200;
- доменная политика `AnalyticsPeriod.windowFrom` — property-based проверки окон.

## Известные особенности

- Баланс в `popug_balances` — read-side проекция для отчёта «попуги в минусе»;
  SSOT финансов — event store в accounting.
- `daily_stats.popugs_negative` — снимок на момент обработки `PaymentProcessed`
  (пересчитывается только при выплате, не при каждом событии).
- `GET /metrics` — отдельная zio-http-route в `Main` (как в gateway), не через tapir.
- До первого события `auth.user.created` попуга нет в проекции — события по нему
  роняют поток (транзиентная ошибка), пока `UserCreated` не дойдёт при catch-up.