# CHANGELOG

## [Unreleased]

## [0.1.0] — Проект и инфраструктура

- Инициализация git-репозитория
- `build.sbt` — root-агрегатор, 7 модулей (common, auth, task-service, accounting, analytics, notification, gateway)
- `project/plugins.sbt` — scalafmt, sbt-native-packager, scalapb
- `.scalafmt.conf` (maxColumn=120, trailing commas)
- `docker-compose.yml` — Postgres 16, Kafka+ZooKeeper, Schema Registry, Jaeger, Prometheus, Grafana
- `docker/postgres/init.sql` — `CREATE DATABASE ates_auth, ates_task, ates_accounting, ates_analytics, ates_notification`
- `docker/prometheus.yml` — scrape config на все сервисы
- Модуль `common`: Protobuf-схемы (UserCreated, TaskCreated, TaskAssigned, TaskCompleted, PaymentProcessed, DeadLetterRecord)
- Модуль `common`: общие Scala-типы (`UserId`, `TaskId`, `Money`)

## [0.2.0] — Auth Service

- Доменная модель: `User`, `Role` (Popug/Manager/Accountant/Admin), `UserStatus` (Active/Disabled), `Email`, `PasswordHash`
- Flyway V1: таблицы `users`, `refresh_tokens`
- Flyway V2: seed admin (id=00000000-0000-0000-0000-000000000001)
- Flyway V3: таблица `outbox`
- Flyway V4: таблица `processed_events`
- `UserRepository` (Quill) — create, findByEmail, findById, list, update
- `PasswordHasher` (bcrypt) — hash, verify
- `TokenService` — issueAccess (15m), issueRefresh (7d), verifyAccess, refreshAccess
- `AuthService` — login, refresh, logout, createUser, getUser, listUsers, updateUser
- Tapir-эндпоинты: `POST /auth/login`, `POST /auth/refresh`, `POST /auth/logout`, `POST /users`, `GET /users`, `GET /users/{id}`, `PATCH /users/{id}`
- `OutboxRelay` — публикация `UserCreated` в Kafka
- `GET /health`, `GET /ready`, `GET /metrics`
- Docker-образ (sbt-native-packager)

## [0.3.0] — API Gateway

- Загрузка публичного ключа Auth при старте, периодическое обновление
- JWT-верификация (проверка подписи, `exp`, `sub`, `role`) — без network call к Auth
- Проксирование по префиксам: `/auth/*` → auth, `/tasks/*` → task-service, `/accounts/*` → accounting, `/analytics/*` → analytics
- CircuitBreaker + Retry + TimeLimiter (zio-resilience4j) на каждый downstream-сервис
- `GET /health`, `GET /ready`, `GET /metrics`
- Docker-образ

## [0.4.0] — TaskService

- Доменная модель: `Task`, `TaskStatus` (Open/Completed), `TaskTitle`, `TaskDescription`, `Money`
- Flyway V1: таблица `tasks`
- Flyway V2: таблица `outbox`
- Flyway V3: таблица `processed_events`
- `TaskRepository` (Quill) — create, findById, findByAssignee, findAll, updateStatus, updateAssignee, getOpenTasks
- `PricingPolicy` — `generateAssignFee()` (rand 10..20), `generateCompleteReward()` (rand 20..40)
- `AssignmentPolicy` — `assignRandom()` (исключить Admin/Manager), `shuffle()` (перетасовать, исключить self-reassign)
- Kafka consumer: `UserCreated` → кэш eligible-попугов (роль = Popug)
- `TaskService` — create (домен + outbox в одной транзакции), complete (проверка assignee, status=open, optimistic lock), shuffle (refund + charge + outbox)
- Tapir-эндпоинты: `POST /tasks`, `GET /tasks`, `GET /tasks/all`, `GET /tasks/{id}`, `PATCH /tasks/{id}/complete`, `POST /tasks/shuffle`
- `OutboxRelay` — публикация `TaskCreated`, `TaskAssigned`, `TaskCompleted` в Kafka
- `GET /health`, `GET /ready`, `GET /metrics`
- Docker-образ

## [0.5.0] — AccountingService (Event Sourcing)

- Доменная модель: `AccountEvent` ADT — `TaskPriceRecorded`, `AccountDebited`, `AccountCredited`, `AccountPayout`
- Flyway V1: таблица `events` (event store, `event_id UNIQUE`)
- Flyway V2: таблица `account_balances` (проекция текущего баланса)
- Flyway V3: таблица `outbox`
- Flyway V4: таблица `processed_events`
- `EventStore` — append, getEvents(userId), getDailyEvents(date)
- Kafka consumer: `UserCreated` → создать счёт (INSERT account_balances)
- Kafka consumer: `TaskCreated` → записать `TaskPriceRecorded`
- Kafka consumer: `TaskAssigned` → `AccountDebited` (новый ассайн) или `AccountCredited` (возврат) + `AccountDebited` (реассайн)
- Kafka consumer: `TaskCompleted` → `AccountCredited`
- `BalanceCalculator` — currentBalance, dailyBalance, managementEarnings
- Tapir-эндпоинты: `GET /accounts/me/balance`, `GET /accounts/me/audit-log`, `GET /accounts/top-management-earnings`, `GET /accounts/daily-stats`
- `PayoutCalculator` — calculate (max(0, balance)), обнуление счёта
- Cron выплат (zio-schedule, конец дня) + outbox → `PaymentProcessed`
- `GET /health`, `GET /ready`, `GET /metrics`
- Docker-образ

## [0.6.0] — AnalyticsService

- Flyway V1: таблицы `tasks`, `popug_balances`, `daily_stats`, `processed_events`
- Kafka consumer: `TaskCreated` → insert/update `tasks`
- Kafka consumer: `TaskCompleted` → update `tasks.status`, `tasks.completedAt`
- Kafka consumer: `TaskAssigned` → update `popug_balances` (debit/credit)
- Kafka consumer: `PaymentProcessed` → update `popug_balances`, `daily_stats`
- Tapir-эндпоинты: `GET /analytics/top-management-earnings`, `GET /analytics/popugs-in-minus`, `GET /analytics/most-expensive-task`
- `GET /health`, `GET /ready`, `GET /metrics`
- Docker-образ

## [0.7.0] — NotificationService (Telegram)

- Flyway V1: таблицы `popug_telegram`, `sent_notifications`, `processed_events`
- Kafka consumer: `TaskAssigned` → Telegram-уведомление попугу
- Kafka consumer: `TaskCompleted` → Telegram-уведомление попугу
- Kafka consumer: `PaymentProcessed` → Telegram-уведомление попугу
- Защита от лавины: событие старше 5 минут → не отправлять
- Нет chat_id → уведомить админа о невозможности доставки
- Дедупликация: `sent_notifications(event_id PK)` — не слать дубликаты
- `GET /health`, `GET /ready`, `GET /metrics`
- Docker-образ

## [0.8.0] — Фронтенд

- HTML-структура: страница логина, дашборд таск-трекера, дашборд аккаунтинга, дашборд аналитики
- JS: логин, сохранение токенов в localStorage, авто-refresh при 401
- JS: таск-трекер — список моих задач, создать задачу, отметить выполненной, кнопка «заассайнить все»
- JS: аккаунтинг — мой баланс, аудитлог, доход менеджмента (admin/accountant)
- JS: аналитика — доход менеджмента, попуги в минусе, самая дорогая задача (admin)
- `nginx.conf` — статика на `:80`, `/api/*` → `gateway:10002`
- Docker-образ (nginx + статика)

## [0.9.0] — Интеграция и мониторинг

- End-to-end тест: полный цикл задача → ассайн → выполнение → выплата
- End-to-end тест: shuffle с возвратом денег
- End-to-end тест: отрицательный баланс и перенос на следующий день
- End-to-end тест: отказоустойчивость (CircuitBreaker, падение сервиса, восстановление)
- Jaeger: сквозной трейс Gateway → TaskService → Kafka → Accounting
- Grafana дашборды: метрики сервисов, Kafka consumer lag, CircuitBreaker status
- Prometheus-алерты: DLQ непустой, consumer lag > N, CircuitBreaker open
- `README.md` — инструкция по запуску, архитектурная схема
