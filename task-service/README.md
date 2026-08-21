# aTES Task Service

Таск-трекер платформы aTES (UberPopug Inc Task Exchange): создание задач,
случайное назначение попугам с ценой за ассайн и наградой за выполнение,
перетасовка задач. События публикуются в Kafka через transactional outbox.

## Что делает сервис

- Создание задачи (`POST /tasks`): рандомная цена ассайна (10..20 USD) и
  награда за выполнение (20..40 USD) через `PricingPolicy`; задача сразу
  назначается случайному попугу через `AssignmentPolicy`.
- Список задач: мои (`GET /tasks`) и все (`GET /tasks/all`) с пагинацией.
- Завершение задачи (`PATCH /tasks/{id}/complete`): только исполнитель,
  optimistic-lock по `version`, переход `open → completed` один раз.
- Перетасовка задач (`POST /tasks/shuffle`): только admin/manager; каждая
  задача перераспределяется случайному попугу (не прежнему), при <2 попугах
  назначение не меняется.
- Kafka consumer `auth.user.created` (`UserCreated`, protobuf): все роли
  записываются в проекцию `users` (идемпотентно); роль `popug` также
  добавляется в кэш `EligiblePopugs`; идемпотентность через
  `processed_events` (PK `event_id`, SQLState 23505).
- Transactional outbox: события `TaskCreated` / `TaskAssigned` /
  `TaskCompleted` пишутся в `outbox` в той же транзакции, что и данные,
  и публикуются релеем в Kafka.
- Аутентификация делегирована Gateway: identity-заголовки
  `X-Auth-User-Id` / `X-Auth-User-Role` инжектятся Gateway после верификации
  JWT (клиентские `x-auth-*` вырезаются — антиспуфинг).
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
task-service/
├── README.md
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   ├── application.conf            # конфигурация (HOCON)
│   │   │   └── db/migration/               # Flyway V1–V4
│   │   └── scala/inc/uberpopug/taskservice/
│   │       ├── Main.scala                  # точка входа, сборка ZLayer-графа
│   │       ├── api/                        # DTO, tapir-эндпоинты, маппинг ошибок
│   │       ├── service/                    # TaskService, OutboxRelay, UserCreatedConsumer
│   │       ├── repository/                 # TaskRepository, OutboxRepository, ProcessedEventsRepository, UserRepository
│   │       ├── domain/                     # Task, PricingPolicy, AssignmentPolicy
│   │       ├── db/                         # DataSource, Quill Context, Flyway
│   │       └── config/                     # case classes конфига
│   └── test/scala/inc/uberpopug/taskservice/  # ZIO Test
```

Слои зависят строго внутрь: `api → service → repository → db/domain`,
`domain` не знает об инфраструктуре.

## Запуск

Требования: JDK 21, sbt 2.0.6, запущенные Postgres и Kafka.

Через docker-compose (из корня репозитория):

```bash
docker-compose up -d postgres kafka
sbt taskService/Docker/publishLocal
docker-compose up -d task-service
```

Локально из IDE или sbt:

```bash
sbt taskService/run
```

По умолчанию сервис слушает порт `10003`, БД `ates_task`, Kafka — `localhost:9092`.
Доступ через Gateway (`http://localhost:10002/tasks*`) с `Authorization: Bearer`.

## Конфигурация

Все параметры в `task-service/src/main/resources/application.conf`, секция `ates`.
Каждое значение можно переопределить env-переменной.

| Параметр | Env | По умолчанию |
|----------|-----|--------------|
| `ates.server.port` | `ATES_SERVER_PORT` | `10003` |
| `ates.database.url` | `ATES_DB_URL` | `jdbc:postgresql://localhost:5432/ates_task` |
| `ates.database.user` | `ATES_DB_USER` | `ates` |
| `ates.database.password` | `ATES_DB_PASSWORD` | `ates` |
| `ates.database.maxPoolSize` | `ATES_DB_MAX_POOL` | `10` |
| `ates.kafka.bootstrapServers` | `ATES_KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` |
| `ates.kafka.consumerGroupId` | `ATES_KAFKA_CONSUMER_GROUP` | `ates-task-service` |
| `ates.kafka.topicUserCreated` | `ATES_KAFKA_TOPIC_USER_CREATED` | `auth.user.created` |
| `ates.outbox.batchSize` | `ATES_OUTBOX_BATCH_SIZE` | `50` |
| `ates.outbox.pollIntervalSeconds` | `ATES_OUTBOX_POLL_INTERVAL` | `2` |

## API

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| GET | `/health` | public | Liveness-проверка |
| GET | `/ready` | public | Readiness (БД/Kafka) |
| GET | `/metrics` | public | Метрики Prometheus |
| POST | `/tasks` | все | Создание задачи (auto-assign) |
| GET | `/tasks` | все | Мои задачи (пагинация) |
| GET | `/tasks/all` | все | Все задачи (пагинация) |
| GET | `/tasks/{id}` | все | Задача по id |
| PATCH | `/tasks/{id}/complete` | assignee | Завершение задачи |
| POST | `/tasks/shuffle` | admin, manager | Перетасовка задач |

Identity передаётся заголовками `X-Auth-User-Id` / `X-Auth-User-Role`
(инжектятся Gateway, проверяются в `TaskServerLogic`).

### Ошибки

Тело ошибки — единый формат `{ "error": "<код>", "message": "<описание>" }`.

| Код | HTTP | Ситуация |
|-----|------|----------|
| `VALIDATION_ERROR` | 400 | Невалидные title/description/id |
| `UNAUTHORIZED` | 401 | Отсутствуют identity-заголовки |
| `FORBIDDEN` | 403 | Недостаточно прав / не исполнитель |
| `NOT_FOUND` | 404 | Задача не найдена |
| `BUSINESS_RULE_VIOLATION` | 409 | Нарушение бизнес-правила (нет попугов и т.п.) |
| `CONFLICT` | 409 | Optimistic lock conflict |
| `INTERNAL_ERROR` | 500 | Ошибка персистентности |

## Тесты

```bash
sbt "taskService/Test/testFull"
```

Покрытие: домен (`Task`, `TaskTitle`, `TaskDescription`), `PricingPolicy`
(границы rand-интервалов), `AssignmentPolicy` (assignRandom/shuffle,
исключение self-reassign), `TaskService` (create/complete/shuffle на моках),
consumer `UserCreated` (дедупликация, роль popug).

## Известные особенности

- Кэш кандидатов на назначение (`EligiblePopugs`) загружается из БД (`users` таблица) при старте и наполняется из Kafka.
  Кэш выживает перезапуски — назначение работает сразу после старта.
- Таблица `users` — проекция из Auth Service: заполняется из `UserCreated` Kafka-событий, содержит все роли (для идемпотентности).
- `GET /metrics` — отдельная zio-http-route в `Main` (как в gateway), не через tapir.