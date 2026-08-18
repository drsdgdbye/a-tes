# aTES Notification Service

Consume-only сервис платформы aTES (UberPopug Inc Task Exchange): доставляет
Telegram-уведомления попугам о назначенных/выполненных задачах и выплатах.
События принимаются из Kafka (task-service / accounting), наружу отдаёт только
`/health`, `/ready`, `/metrics` — мутирующего API нет.

## Что делает сервис

- Kafka consumer `task.assigned`, `task.completed`, `accounting.payment.processed`
  (protobuf): маппит события в доменные `NotificationEvent` и формирует тексты
  сообщений.
- Каналы доставки через абстракцию `NotificationChannel` + реестр `ChannelRegistry`:
  сейчас реализован только Telegram, добавление email/sms = новая реализация трейта
  + секция конфига + запись в реестре (пайплайн не меняется).
- Telegram-канал: HTTP Bot API `sendMessage` (`java.net.http`, без доп.
  зависимостей) под resilience-цепочкой (спека §7.2): RateLimiter 25 msg/s →
  CircuitBreaker (5 ошибок → open, 60s half-open) → TimeLimiter 10s → Retry
  (exponential backoff, 5 попыток). Все параметры — в конфиге канала.
- Защита от лавины: событие старше 5 минут не отправляется никому (спека §3.6).
- Нет маппинга `popug_id → адрес канала` → уведомление админским адресам канала
  (из конфига) о невозможности доставки.
- Идемпотентность: `processed_events(event_id PK)` на уровне обработки +
  `sent_notifications(event_id, channel, address)` вставляется ДО отправки (спека §7.4) —
  дубликаты не отправляются (дедуп по адресату, админская рассылка доходит каждому админу).
- Poison pill (невалидный protobuf/данные) и исчерпанные попытки доставки →
  DLQ `notification.dlq` (`DeadLetterRecord`); транзиентные ошибки роняют поток:
  consumer переподписывается с exponential backoff и событие переобрабатывается.

## Технологический стек

| Компонент | Библиотека |
|-----------|------------|
| Язык | Scala 3.8.4 |
| Эффекты | ZIO 2 (zio, zio-http, zio-logging) |
| БД | Postgres + Quill (zio-jdbc), HikariCP |
| Миграции | Flyway |
| Kafka | zio-kafka (consumer/producer), протокол protobuf |
| Resilience | rezilience (RateLimiter, CircuitBreaker, Timeout, Retry) |
| HTTP-клиент | `java.net.http` (JDK) |
| Сериализация | zio-json |
| Конфиг | zio-config (HOCON + env-оверрайды) |
| Метрики | zio-metrics-connectors (Prometheus) |

## Структура проекта

```
notification/
├── README.md
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   ├── application.conf            # конфигурация (HOCON)
│   │   │   └── db/migration/               # Flyway V1
│   │   └── scala/inc/uberpopug/notification/
│   │       ├── Main.scala                  # точка входа, сборка ZLayer-графа
│   │       ├── service/                    # NotificationConsumer, EventProcessor, NotificationChannel, TelegramChannel, ChannelRegistry
│   │       ├── repository/                 # NotificationStore
│   │       ├── domain/                     # ChannelType, NotificationEvent, NotificationTextBuilder, StalenessPolicy
│   │       ├── db/                         # DataSource, Quill Context, Flyway
│   │       └── config/                     # case classes конфига
```

Слои зависят строго внутрь: `service → repository → db/domain`,
`domain` не знает об инфраструктуре.

## Запуск

Требования: JDK 21, sbt 2.0.6, запущенные Postgres и Kafka.

Через docker-compose (из корня репозитория):

```bash
docker-compose up -d postgres kafka
sbt notification/Docker/publishLocal
ATES_CHANNELS_TELEGRAM_BOT_TOKEN=<token> ATES_CHANNELS_TELEGRAM_ADMIN_ADDRESSES=<chatId1,chatId2> docker-compose up -d notification
```

Локально из IDE или sbt:

```bash
sbt notification/run
```

По умолчанию сервис слушает порт `10006`, БД `ates_notification`, Kafka — `localhost:29092`.

## Конфигурация

Все параметры в `notification/src/main/resources/application.conf`, секция `ates`.
Каждое значение можно переопределить env-переменной. Токен бота и админские
адреса не запекаются в образ — задаются только env при запуске контейнера.

| Параметр | Env | По умолчанию |
|----------|-----|--------------|
| `ates.server.port` | `ATES_SERVER_PORT` | `10006` |
| `ates.database.url` | `ATES_DB_URL` | `jdbc:postgresql://localhost:5432/ates_notification` |
| `ates.database.user` | `ATES_DB_USER` | `ates` |
| `ates.database.password` | `ATES_DB_PASSWORD` | `ates` |
| `ates.database.maxPoolSize` | `ATES_DB_MAX_POOL` | `10` |
| `ates.kafka.bootstrapServers` | `ATES_KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` |
| `ates.kafka.consumerGroupId` | `ATES_KAFKA_CONSUMER_GROUP` | `ates-notification` |
| `ates.kafka.topicTaskAssigned` | `ATES_KAFKA_TOPIC_TASK_ASSIGNED` | `task.assigned` |
| `ates.kafka.topicTaskCompleted` | `ATES_KAFKA_TOPIC_TASK_COMPLETED` | `task.completed` |
| `ates.kafka.topicPaymentProcessed` | `ATES_KAFKA_TOPIC_PAYMENT_PROCESSED` | `accounting.payment.processed` |
| `ates.kafka.topicDlq` | `ATES_KAFKA_TOPIC_DLQ` | `notification.dlq` |
| `ates.channels.telegram.botToken` | `ATES_CHANNELS_TELEGRAM_BOT_TOKEN` | `""` (нет отправок → DLQ) |
| `ates.channels.telegram.adminAddresses` | `ATES_CHANNELS_TELEGRAM_ADMIN_ADDRESSES` | `""` (CSV) |
| `ates.channels.telegram.rateLimitPerSecond` | `ATES_CHANNELS_TELEGRAM_RATE_LIMIT_PER_SECOND` | `25` |
| `ates.channels.telegram.sendTimeoutSeconds` | `ATES_CHANNELS_TELEGRAM_SEND_TIMEOUT_SECONDS` | `10` |
| `ates.channels.telegram.retryAttempts` | `ATES_CHANNELS_TELEGRAM_RETRY_ATTEMPTS` | `5` |

`adminAddresses` — список через запятую: `ATES_CHANNELS_TELEGRAM_ADMIN_ADDRESSES=123456789,987654321`.

## API

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| GET | `/health` | public | Liveness-проверка |
| GET | `/ready` | public | Readiness (проверка БД) |
| GET | `/metrics` | public | Метрики Prometheus |

## Форматы сообщений

| Событие | Текст |
|---------|-------|
| `TaskAssigned` | «Вам назначена задача «title»» |
| `TaskCompleted` | «Задача «title» выполнена. Начислено: $X» |
| `PaymentProcessed` | «Выплата за DD.MM: $XX» (формат из спеки) |

## Отклонения от спеки

1. Топик `accounting.payment.processed` (в спеке §6.7 — `accounting.payment_processed`):
   фактический топик accounting-сервиса (как в accounting/analytics).
2. Таблица `popug_telegram` (§9.5) обобщена в `popug_contacts(popug_id, channel,
   address)` и `sent_notifications` дедуплицируется по `(event_id, channel,
   address)` — требование мультиканальности (email/sms); админская рассылка
   доставляет по одному сообщению каждому админу канала (M-NTF-04).
3. `GET /health`, `GET /ready` — zio-http-роуты, а не tapir (нет бизнес-API).
4. Resilience — rezilience вместо zio-resilience4j (прецедент gateway).
5. `popug_contacts` заполняется out-of-band (спека §3.6: consume-only, без
   мутирующего API). Админские адреса — из конфига канала.

## Тесты

```bash
sbt "notification/Test/testFull"
```

Юнит-тесты (ZIO Test, моки — реальный Telegram/Kafka/Postgres не нужны):
- `NotificationTextBuilderSpec` — SSOT форматов сообщений (M-NTF-12), включая
  property-based на форматирование денег и дат;
- `StalenessPolicySpec` — граница «5 минут» + property-based (M-NTF-05/06);
- `ChannelTypeSpec` — парсинг каналов;
- `NotificationEventProcessorSpec` — доставка по типам событий, уведомление
  админам при отсутствии маппинга, защита от лавины (TestClock), дедупликация
  через `processed_events` и `sent_notifications` (M-NTF-01..07);
- `TelegramChannelSpec` — resilience-цепочка на mock `RawTelegramClient` с
  тестовыми параметрами: retry 5 попыток, CircuitBreaker open → fast-fail,
  RateLimiter (concurrency ≤ лимит), TimeLimiter (M-NTF-08..11);
- `NotificationConsumerSpec` — классификация poison pill / транзиентных ошибок
  (суть DLQ-ветки, M-NTF-08).

Полный перечень — `docs/test-plan.md` §6 (таблица M-NTF → тест).

## Известные особенности

- Без `botToken` (или без реального бота) отправки не проходят → `TelegramSendFailed`
  → после ретраев в DLQ; видно в логах и метрике `dlq_messages_total`.
- `GET /metrics` — отдельная zio-http-route в `Main` (как в gateway/analytics).
- При рестарте сервиса события старше 5 минут отбрасываются (защита от лавины
  уведомлений), события не дублируются (`processed_events` + `sent_notifications`).
