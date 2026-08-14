# aTES — Awesome Task Exchange System

Учебный проект для практики проектирования распределённых систем. Мы специально
гиперболизируем проблематику, чтобы было интереснее.

## 1. Контекст и проблема

Топ-менеджмент UberPopug Inc столкнулся с проблемой производительности
сотрудников. Чтобы повысить производительность, было принято решение выкинуть
текущий таск-трекер и написать особый Awesome Task Exchange System (aTES),
который должен будет увеличить производительность сотрудников на неопределённый
процент. Чтобы попуги развивались и изучали новые направления, была придумана
инновационная схема ассайна каждой задачи на случайного сотрудника. А для
повышения мотивации топ-менеджмент решил сделать корпоративный аккаунтинг в
таск-трекере, чтобы по количеству выполненных задач выплачивать сотрудникам
зарплату. При этом задачи оцениваются с плавающим коэффициентом (местами
отрицательным).

## 2. Общие требования

### 2.1. Таск-трекер

- Таск-трекер — отдельный дашборд, доступен всем сотрудникам.
- Авторизация — через общий сервис UberPopug Inc (форма клюва).
- Только задачи. Проектов, скоупов и спринтов нет.
- Новые таски может создавать кто угодно. У задачи: описание, статус
  (выполнена/нет), рандомно выбранный попуг (кроме менеджера и администратора).
- Менеджеры или администраторы — кнопка «заассайнить задачи»: все открытые
  задачи рандомно перетасовываются между сотрудниками. Кнопку можно нажимать
  без ограничений.
- Создать не заассайненную задачу нельзя. Любая задача имеет попуга-исполнителя.
- Каждый сотрудник видит список своих задач и может отметить задачу выполненной.

### 2.2. Аккаунтинг

- Отдельный дашборд. Полный доступ — админы и бухгалтеры. Обычные попуги видят
  только свой баланс и лог операций.
- У каждого сотрудника — счёт. Лог операций: за что списано/начислено.
- Расценки:
  - assignFee = rand(10..20)\$ — списывается при ассайне
  - completeReward = rand(20..40)\$ — начисляется при выполнении
  - Цены определяются единоразово в момент создания задачи
  - Цены всегда с положительным знаком (тип операции определяет знак)
  - Отрицательный баланс переносится на следующий день
- Заработок топ-менеджмента: sum(assignFee) − sum(completeReward) за день
- Конец дня:
  - Расчёт выплаты, отправка уведомления
  - Баланс обнуляется, в аудитлоге — запись о выплате
  - Информация выводится по дням

### 2.3. Аналитика

- Отдельный дашборд, только для админов.
- Заработок топ-менеджмента за сегодня.
- Количество попугов в минусе.
- Самая дорогая задача за день / неделю / месяц (наивысшая completeReward среди
  закрытых задач за период).

## 3. Доменная модель

### 3.1. Общие типы (`modules/common`)

Типы, разделяемые всеми сервисами. В коде — `opaque type`, в БД и Protobuf —
примитивы (UUID как string, деньги как int64 центов).

| Тип | Базовый тип | Инвариант |
|-----|------------|-----------|
| `UserId` | UUID | Непустой |
| `TaskId` | UUID | Непустой |
| `Money` | BigDecimal | 2 знака после запятой |

`Money` — всегда в долларах в домене. В БД и Protobuf хранится в центах
(`int64`). Операции: `+`, `−`, `*`, `toCents`, `fromCents`, `isPositive`,
`isNegative`, `isZero`.

### 3.2. Auth Service

#### Сущности

| Сущность | Поля | Тип |
|----------|------|-----|
| `User` (aggregate root) | `id: UserId` | UUID |
| | `name: String` | строка |
| | `email: Email` | opaque type, валидация `@` |
| | `passwordHash: PasswordHash` | opaque type, bcrypt |
| | `role: Role` | enum |
| | `status: UserStatus` | enum |
| | `createdAt: Instant` | UTC |
| | `updatedAt: Instant` | UTC |

#### Value objects

| Тип | Значения | Инвариант |
|-----|----------|-----------|
| `Role` | `Popug`, `Manager`, `Accountant`, `Admin` | — |
| `UserStatus` | `Active`, `Disabled` | — |
| `Email` | opaque `String` | Содержит `@`, непустой |
| `PasswordHash` | opaque `String` | bcrypt, 60 символов |

#### Инварианты

- Email уникален в системе
- Нельзя отключить самого себя (admin)
- Только Admin меняет роль другому пользователю
- Пользователь не удаляется — только переводится в `Disabled`
- Refresh-токен можно отозвать (logout, disable user)

#### Доменные события

| Событие | Триггер | Публикуется в Kafka |
|---------|---------|---------------------|
| `UserCreated` | Админ создал пользователя | Да → TaskService, Accounting |

#### Доменные сервисы

```
PasswordHasher:
  hash(plain: String): PasswordHash
  verify(plain: String, hash: PasswordHash): Boolean

TokenService:
  issueAccess(userId: UserId, role: Role): Jwt       // срок: 15 минут
  issueRefresh(userId: UserId): RefreshToken          // срок: 7 дней
  verifyAccess(token: String): Either[AuthError, (UserId, Role)]
  refreshAccess(refreshToken: String): Either[AuthError, Jwt]
```

### 3.3. TaskService

#### Сущности

| Сущность | Поля | Тип |
|----------|------|-----|
| `Task` (aggregate root) | `id: TaskId` | UUID |
| | `title: TaskTitle` | opaque, ≤500 символов |
| | `description: Option[TaskDescription]` | opaque, непустой |
| | `status: TaskStatus` | enum |
| | `assigneeId: UserId` | UUID |
| | `assignFee: Money` | 10..20, целое |
| | `completeReward: Money` | 20..40, целое |
| | `createdAt: Instant` | UTC |
| | `completedAt: Option[Instant]` | UTC |
| | `version: Long` | optimistic lock |

#### Value objects

| Тип | Базовый тип | Инвариант |
|-----|------------|-----------|
| `TaskTitle` | `String` | Непустой, ≤500 символов |
| `TaskDescription` | `String` | Непустой (опциональный) |
| `TaskStatus` | enum | `Open`, `Completed` |

#### Инварианты

- Задача всегда имеет исполнителя — `assigneeId` никогда не пуст
- Исполнитель не может иметь роль `Manager` или `Admin`
- Переход статуса: только `Open → Completed`, обратный переход запрещён
- Только исполнитель (`assigneeId`) может завершить задачу
- `assignFee ∈ [10, 20]`, целое число долларов, строго > 0
- `completeReward ∈ [20, 40]`, целое число долларов, строго > 0
- При реассайне: `newAssigneeId ≠ oldAssigneeId` — задача пропускается

#### Доменные события

| Событие | Триггер |
|---------|---------|
| `TaskCreated` | Задача создана + сразу заассайнена (публикуется вместе с TaskAssigned) |
| `TaskAssigned` | Первичный ассайн (при создании) или реассайн (shuffle). `oldAssigneeId` пуст при первом ассайне |
| `TaskCompleted` | Исполнитель отметил задачу выполненной |

#### Доменные сервисы

```
AssignmentPolicy:
  assignRandom(eligiblePopugs: List[UserId]): UserId
    // выбрать случайного попуга, исключая Manager и Admin
  shuffle(tasks: List[Task], popugs: List[UserId]): List[(TaskId, UserId, UserId)]
    // перетасовать: вернуть список (taskId, oldAssigneeId, newAssigneeId)
    // newAssigneeId ≠ oldAssigneeId

PricingPolicy:
  generateAssignFee(): Money       // rand(10..20)
  generateCompleteReward(): Money   // rand(20..40)
```

### 3.4. AccountingService (Event Sourcing)

#### Агрегат: Account

Агрегат — поток событий для конкретного `UserId`. Баланс — проекция,
вычисляемая проигрыванием событий.

#### События event store

| Событие | Источник | Поля |
|---------|----------|------|
| `TaskPriceRecorded` | Kafka: `TaskCreated` | `taskId, userId, assignFee, completeReward` |
| `AccountDebited` | Kafka: `TaskAssigned` (списание) | `userId, amount, taskId, reason = TaskAssigned` |
| `AccountCredited` | Kafka: `TaskAssigned` (возврат) или `TaskCompleted` | `userId, amount, taskId, reason = AssignmentRefund \| TaskCompleted` |
| `AccountPayout` | Cron выплаты | `userId, amount, date` |

#### Проекции

| Проекция | Источник | Поля |
|----------|----------|------|
| `AccountBalance` | События Account | `userId, balanceCents, updatedAt` |
| `AuditLogEntry` | События Account | `id, type: Debit\|Credit\|Payout, amount, taskId?, description, timestamp` |

#### Инварианты

- Счёт создаётся первым событием — до этого операции невозможны
- Отрицательный баланс разрешён и переносится на следующий день
- Выплата = `max(0, balance)`, никогда не отрицательна
- Каждая финансовая операция (Debit/Credit) имеет `taskId`, кроме Payout
- Дебет и кредит всегда симметричны: `sum(events) = balance`

#### Доменные сервисы

```
BalanceCalculator:
  currentBalance(userId: UserId, events: List[AccountEvent]): Money
  dailyBalance(userId: UserId, date: LocalDate, events: List[AccountEvent]): Money

PayoutCalculator:
  calculate(accounts: List[(UserId, Money)]): List[(UserId, Money, LocalDate)]
    // для каждого: amount = max(0, balance), счёт обнуляется
```

### 3.5. AnalyticsService

Read-side сервис без сильной доменной логики. Все данные — проекции
из Kafka-событий.

#### Проекции

| Проекция | Источник событий | Поля |
|----------|-----------------|------|
| `TaskProjection` | `TaskCreated`, `TaskCompleted` | `taskId, title, assignFee, completeReward, status, completedAt` |
| `PopugBalance` | `TaskAssigned`, `TaskCompleted`, `PaymentProcessed` | `userId, name, balanceCents` |
| `DailyStats` | Все события | `date, managementEarningsCents, popugsTotal, popugsNegative` |

#### Инварианты

- `managementEarnings = sum(assignFee) − sum(completeReward)` за указанный день
- `popugsNegative` = количество попугов с `balanceCents < 0`
- Самая дорогая задача за период = `max(completeReward)` среди закрытых задач
  в указанном диапазоне дат

### 3.6. NotificationService

Consume-only сервис, без мутирующего API.

#### Проекции

| Проекция | Поля |
|----------|------|
| `TelegramMapping` | `popugId: UserId, chatId: String` |

#### Инварианты

- Нет маппинга `popugId → chatId` → уведомить админа о невозможности доставки
- Событие старше 5 минут (разница `now − event.timestamp`) → не отправлять
  (защита от лавины уведомлений при рестарте сервиса)
- Не отправлять дубликаты (проверка по `eventId` в таблице `sent_notifications`)
- Событие старше 5 минут + нет маппинга → не слать ни админу, ни попугу
  (уведомление потеряло актуальность)

## 4. Архитектура

### 4.1. Обзор системы

Пять микросервисов + API Gateway + фронтенд, обменивающиеся событиями через Kafka.

| Компонент | Назначение |
|-----------|-----------|
| **API Gateway** | Единая точка входа, JWT-верификация, проксирование в сервисы |
| **Auth Service** | Пользователи, роли, JWT (access + refresh) |
| **TaskService** | CRUD задач, ассайн, генерация цен, отметка выполнения |
| **AccountingService** | Event sourcing счетов, аудитлог, cron выплат |
| **AnalyticsService** | Агрегация статистики из событий |
| **NotificationService** | Telegram-уведомления |
| **Frontend (nginx)** | Статика HTML/CSS/JS |

Ключевые архитектурные решения:

- **Kafka + transactional outbox** — гарантированная доставка событий.
  Каждый продюсер пишет в свою БД событие и запись в outbox в одной транзакции.
  Фоновый relay публикует из outbox в Kafka.
- **Eventual consistency** — сервисы сходятся асинхронно, без распределённых
  транзакций.
- **Event-Carried State Transfer** — каждое событие самодостаточно: потребитель
  не делает callback к продюсеру.
- **Protobuf + Schema Registry** — схемы событий, валидация совместимости
  (FORWARD_TRANSITIVE).
- **Event sourcing** в Accounting — все мутации как события, баланс = проекция.
- **JWT (access + refresh)** — Gateway проверяет access-токен локально (публичный
  ключ), refresh хранится в Auth DB.

### 4.2. Сервисы

| Сервис | Ответственность | Хранилище | Паттерн |
|--------|----------------|-----------|---------|
| Auth | Пользователи, роли, JWT, refresh-токены | `ates_auth` | CRUD |
| TaskService | Задачи, ассайн, цены, статусы | `ates_task` | CRUD + outbox |
| Accounting | Счета, балансы, аудитлог, выплаты | `ates_accounting` | Event sourcing + outbox |
| Analytics | Агрегация статистики | `ates_analytics` | Projections |
| Notification | Telegram-уведомления | `ates_notification` | Consumer |
| Gateway | JWT-верификация, проксирование | — | Stateless |

### 4.3. События Kafka

| Событие | Producer | Consumers |
|---------|----------|-----------|
| `UserCreated` | Auth | TaskService, Accounting |
| `TaskCreated` | TaskService | Accounting, Analytics |
| `TaskAssigned` | TaskService | Accounting, Analytics, Notification |
| `TaskCompleted` | TaskService | Accounting, Analytics, Notification |
| `PaymentProcessed` | Accounting | Analytics, Notification |

### 4.4. Потоки данных

```
Auth ──UserCreated──────────→ TaskService, Accounting

TaskService ──TaskCreated────→ Accounting, Analytics
TaskService ──TaskAssigned───→ Accounting, Analytics, Notification
TaskService ──TaskCompleted──→ Accounting, Analytics, Notification

Accounting ──PaymentProcessed→ Analytics, Notification
```

Синхронные вызовы — только HTTP через Gateway (инициированы пользователем).
Сервисы никогда не ходят напрямую друг к другу по HTTP.

### 4.5. Data Flow по сценариям

#### Создание задачи

```
POST /api/tasks → Gateway → TaskService
  → создаёт task (status=open)
  → выбирает случайного попуга (роль popug)
  → генерирует assignFee = rand(10..20)$, completeReward = rand(20..40)$
  → в одной транзакции: INSERT task + INSERT outbox(TaskCreated + TaskAssigned)
  → возвращает { id, title, assigneeId, assignFee, completeReward }
  → outbox-relay публикует TaskCreated и TaskAssigned в Kafka
```

#### Реассайн («заассайнить все»)

```
POST /api/tasks/shuffle → Gateway → TaskService
  → читает все задачи status=open
  → для каждой:
    - oldAssigneeId = текущий assignee
    - newAssigneeId = случайный попуг (не admin/manager, не oldAssigneeId)
    - UPDATE task.assignee_id
    - INSERT outbox(TaskAssigned{ oldAssigneeId, newAssigneeId, assignFee })
  → возвращает { tasksReassigned: N }

При обработке TaskAssigned в Accounting:
  - если oldAssigneeId не пуст: возврат assignFee на oldAssigneeId (Credited)
  - списание assignFee с newAssigneeId (Debited)
```

Операция синхронная. При нагрузке ≤100 пользователей latency допустима.

#### Выполнение задачи

```
PATCH /api/tasks/{id}/complete → Gateway → TaskService
  → проверяет: статус = open, caller = assignee
  → UPDATE task.status = 'completed', completed_at = now()
  → INSERT outbox(TaskCompleted)
  → возвращает обновлённую задачу

При обработке TaskCompleted в Accounting:
  → начисление completeReward на assignee_id (Credited)
  → пополнение баланса
```

#### Выплата в конце дня

```
Cron в AccountingService срабатывает в конце дня:
  → для каждого пользователя с ненулевым балансом:
    - INSERT events(Payout{ userId, balance, date })
    - UPDATE account_balances SET balance_cents = 0
    - INSERT outbox(PaymentProcessed{ userId, amount, date })
  → outbox-relay публикует PaymentProcessed в Kafka

При обработке PaymentProcessed:
  → Notification — отправляет Telegram: «Выплата за DD.MM: $XX»
  → Analytics — обновляет daily_stats
```

## 5. REST API контракт

Все эндпоинты доступны через Gateway. Gateway проверяет JWT и проксирует запрос
в нужный сервис. Роли проверяются на стороне сервиса.

### 5.1. Auth Service

```
POST /auth/login
  Request:  { "login": "string", "password": "string" }
  Response: { "accessToken": "<jwt>", "refreshToken": "<uuid>",
              "tokenType": "Bearer", "expiresIn": 900 }

POST /auth/refresh
  Request:  { "refreshToken": "<uuid>" }
  Response: { "accessToken": "<jwt>", "refreshToken": "<uuid>",
              "tokenType": "Bearer", "expiresIn": 900 }

POST /auth/logout
  Request:  { "refreshToken": "<uuid>" }
  Response: 204 No Content

POST /users                                          ← admin
  Request:  { "name": "string", "email": "string",
              "role": "popug" | "manager" | "accountant" | "admin" }
  Response: { "id": "<uuid>", "name": "string", "email": "string",
              "role": "string", "createdAt": "<iso8601>" }

GET /users                                           ← admin
  Query:    ?limit=int&offset=int
  Response: { "items": [{ "id": "<uuid>", "name": "string",
                          "email": "string", "role": "string" }], "total": int }

GET /users/{id}                                      ← все
  Response: { "id": "<uuid>", "name": "string", "email": "string",
              "role": "string", "createdAt": "<iso8601>" }

PATCH /users/{id}                                    ← admin
  Request:  { "role"?: "string", "status"?: "active" | "disabled" }
  Response: { "id": "<uuid>", "name": "string", "email": "string",
              "role": "string", "status": "string", "createdAt": "<iso8601>" }
```

### 5.2. TaskService

```
POST /tasks                                          ← все
  Request:  { "title": "string", "description"?: "string" }
  Response: { "id": "<uuid>", "title": "string", "description": "string|null",
              "status": "open", "assigneeId": "<uuid>",
              "assignFee": "15.00", "completeReward": "32.50",
              "createdAt": "<iso8601>" }

GET /tasks                                           ← все (мои)
  Query:    ?limit=int&offset=int
  Response: { "items": [TaskDto], "total": int }

GET /tasks/all                                       ← все
  Query:    ?limit=int&offset=int
  Response: { "items": [TaskDto], "total": int }

GET /tasks/{id}                                      ← все
  Response: TaskDto

PATCH /tasks/{id}/complete                           ← assignee
  Response: { "id": "<uuid>", "title": "string", "status": "completed",
              "assigneeId": "<uuid>", "assignFee": "15.00",
              "completeReward": "30.00",
              "createdAt": "<iso8601>", "completedAt": "<iso8601>" }

POST /tasks/shuffle                                  ← admin, manager
  Response: { "tasksReassigned": 42 }
```

**TaskDto:**
```json
{ "id":             "<uuid>",
  "title":          "string",
  "description":    "string|null",
  "status":         "open" | "completed",
  "assigneeId":     "<uuid>",
  "assignFee":      "15.00",
  "completeReward": "32.75",
  "createdAt":      "<iso8601>",
  "completedAt":    "<iso8601>|null" }
```

### 5.3. AccountingService

```
GET /accounts/me/balance                             ← все
  Response: { "userId": "<uuid>", "balance": "-25.00", "date": "2024-03-15" }

GET /accounts/me/audit-log                           ← все
  Query:    ?limit=int&offset=int
  Response: { "items": [{
               "id":          "<uuid>",
               "type":        "assign" | "complete" | "refund" | "payout",
               "amount":      "15.00",
               "taskId":      "<uuid>|null",
               "description": "string",
               "timestamp":   "<iso8601>"
             }], "total": int }

GET /accounts/top-management-earnings                ← admin, accountant
  Query:    ?date=2024-03-15
  Response: { "amount": "250.00", "date": "2024-03-15" }
  // amount = sum(assignFee за день) − sum(completeReward за день)

GET /accounts/daily-stats                            ← admin, accountant
  Query:    ?from=2024-03-10&to=2024-03-15
  Response: { "items": [{
               "date":            "2024-03-15",
               "earnings":        "250.00",
               "popugsTotal":     15,
               "popugsNegative":  3
             }] }
```

### 5.4. AnalyticsService

```
GET /analytics/top-management-earnings               ← admin
  Query:    ?from=2024-03-10&to=2024-03-15
  Response: { "items": [{ "date": "2024-03-15", "amount": "250.00" }],
              "total": "1250.00" }

GET /analytics/popugs-in-minus                       ← admin
  Response: { "count": 5,
              "items": [{ "userId": "<uuid>", "name": "string",
                          "balance": "-25.00" }] }

GET /analytics/most-expensive-task                   ← admin
  Query:    ?period=day&date=2024-03-15
            // period: day | week | month
  Response: { "items": [
               { "date": "2024-03-15", "taskId": "<uuid>",
                 "title": "string", "amount": "38.00" },
               { "date": "2024-03-14", "taskId": "<uuid>",
                 "title": "string", "amount": "23.00" },
               { "date": "2024-03-13", "taskId": "<uuid>",
                 "title": "string", "amount": "28.00" }
             ],
             "overall": { "taskId": "<uuid>", "title": "string",
                          "amount": "38.00" } }
  // items — самая дорогая за каждый день периода
  // overall — самая дорогая за весь период
```

### 5.5. Общие правила

**Типы данных в JSON:**

| Тип | Формат |
|-----|--------|
| UUID | `"550e8400-e29b-41d4-a716-446655440000"` |
| Decimal (деньги) | `"25.50"` — строка, 2 знака после запятой |
| DateTime | `"2024-03-15T14:30:00Z"` — ISO 8601 UTC |
| Enum | `"open"`, `"popug"` — lowercase |
| Optional | `null` или отсутствующее поле |

**Ошибки (все эндпоинты):**
```json
{ "error": "ERROR_CODE", "message": "Human readable description" }
```

Коды ошибок: `UNAUTHORIZED`, `FORBIDDEN`, `NOT_FOUND`, `VALIDATION_ERROR`,
`BUSINESS_RULE_VIOLATION`, `INTERNAL_ERROR`.

**Health checks (каждый сервис):**

| Метод | Путь | Ответ |
|-------|------|-------|
| `GET` | `/health` | `200 {"status":"ok"}` |
| `GET` | `/ready` | `200 {"status":"ok","checks":{"db":"ok","kafka":"ok"}}` или `503` |
| `GET` | `/metrics` | `text/plain` (Prometheus) |

## 6. Protobuf-события

Все `.proto` в `modules/common/src/main/protobuf/`. Сборка: `scalapb` + `sbt-protoc`.
Деньги — в центах (`int64`), UUID — `string`, время — Unix millis (`int64`).

### 6.1. `auth/user_created.proto`

```protobuf
syntax = "proto3";

message UserCreated {
  string event_id   = 1;  // UUIDv4
  int64  timestamp  = 2;  // Unix millis
  int32  version    = 3;  // старт = 1
  string user_id    = 4;  // UUID
  string name       = 5;
  string email      = 6;
  string role       = 7;  // popug | manager | accountant | admin
}
```

### 6.2. `task/task_created.proto`

```protobuf
syntax = "proto3";

message TaskCreated {
  string event_id              = 1;
  int64  timestamp             = 2;
  int32  version               = 3;
  string task_id               = 4;
  string title                 = 5;
  string description           = 6;  // "" если отсутствует
  string assignee_id           = 7;  // рандомный попуг
  int64  assign_fee_cents      = 8;  // rand(1000..2000)
  int64  complete_reward_cents = 9;  // rand(2000..4000)
}
```

### 6.3. `task/task_assigned.proto`

```protobuf
syntax = "proto3";

message TaskAssigned {
  string event_id          = 1;
  int64  timestamp         = 2;
  int32  version           = 3;
  string task_id           = 4;
  string task_title        = 5;  // ECST: для уведомлений
  string new_assignee_id   = 6;  // кому назначили
  string old_assignee_id   = 7;  // с кого сняли; "" при первом ассайне
  int64  assign_fee_cents  = 8;  // сумма списания/возврата
}
```

Правила обработки:

- `old_assignee_id == ""` → списание `assign_fee_cents` с `new_assignee_id`
- `old_assignee_id != ""` → возврат `assign_fee_cents` на `old_assignee_id`,
  списание `assign_fee_cents` с `new_assignee_id`

### 6.4. `task/task_completed.proto`

```protobuf
syntax = "proto3";

message TaskCompleted {
  string event_id              = 1;
  int64  timestamp             = 2;
  int32  version               = 3;
  string task_id               = 4;
  string task_title            = 5;
  string assignee_id           = 6;
  int64  complete_reward_cents = 7;
}
```

### 6.5. `accounting/payment_processed.proto`

```protobuf
syntax = "proto3";

message PaymentProcessed {
  string event_id     = 1;
  int64  timestamp    = 2;
  int32  version      = 3;
  string popug_id     = 4;
  string popug_name   = 5;
  int64  amount_cents = 6;
  string date         = 7;  // "2024-03-15"
}
```

### 6.6. `internal/dead_letter_record.proto`

```protobuf
syntax = "proto3";

message DeadLetterRecord {
  string original_topic   = 1;
  bytes  original_value   = 2;
  string error_message    = 3;
  int64  failed_at        = 4;
  int64  original_offset  = 5;
  int32  partition        = 6;
}
```

### 6.7. Топики Kafka

| Топик | Схема | Партиции | Ключ | Retention |
|-------|-------|---------|------|-----------|
| `auth.user.created` | `UserCreated` | 3 | `user_id` | 7d |
| `task.created` | `TaskCreated` | 3 | `task_id` | 7d |
| `task.assigned` | `TaskAssigned` | 3 | `task_id` | 7d |
| `task.completed` | `TaskCompleted` | 3 | `task_id` | 7d |
| `accounting.payment_processed` | `PaymentProcessed` | 3 | `popug_id` | 7d |
| `auth.dlq` | `DeadLetterRecord` | 1 | — | 30d |
| `task-service.dlq` | `DeadLetterRecord` | 1 | — | 30d |
| `accounting.dlq` | `DeadLetterRecord` | 1 | — | 30d |
| `analytics.dlq` | `DeadLetterRecord` | 1 | — | 30d |
| `notification.dlq` | `DeadLetterRecord` | 1 | — | 30d |

Ключ `task_id` для task-событий — все события одной задачи в одной партиции,
гарантирует порядок обработки.

## 7. Отказоустойчивость

### 7.1. Модель деградации

| Сервис упал | Работает | Сломано | Риск |
|-------------|----------|---------|------|
| **Auth** | Существующие JWT-сессии (Gateway проверяет подпись локально) | Логин, создание/редактирование пользователей, refresh | Низкий |
| **TaskService** | Accounting/Analytics/Notification работают на имеющихся данных | Создание/закрытие задач, ассайн | Средний |
| **Accounting** | TaskService работает, события копятся в Kafka | Балансы, аудитлог, выплаты. Нагонит при восстановлении | Средний |
| **Analytics** | Всё кроме дашборда аналитики | Дашборд аналитики | Низкий |
| **Notification** | Все сервисы работают | Telegram-уведомления. При восстановлении: проверка timestamp события, старше 5 минут — не слать. Если у попуга нет telegram_chat_id — уведомление админу | Низкий |
| **Gateway** | Ничего | Всё | Критический |

### 7.2. Паттерны устойчивости

Реализация: `zio-resilience4j` + нативные `ZIO#timeout` / `Schedule`.

| Паттерн | Где | Параметры |
|---------|-----|-----------|
| **TimeLimiter** | Gateway → сервисы | Чтение: 5s, запись: 10s |
| **TimeLimiter** | Обработка одного Kafka-события | 30s |
| **TimeLimiter** | Notification → Telegram API | 10s |
| **Retry** | Gateway → сервисы (транзиентные ошибки) | Exponential backoff, max 3 |
| **Retry** | Outbox-relay → Kafka | Бесконечно, фиксированный интервал |
| **Retry** | Kafka consumer (транзиентные) | Exponential backoff, max 5 |
| **CircuitBreaker** | Gateway → каждый сервис | 5 ошибок → open, 30s half-open |
| **CircuitBreaker** | Notification → Telegram API | 5 ошибок → open, 60s half-open |
| **Bulkhead** | Gateway → сервисы | Max 20 concurrent на сервис |
| **RateLimiter** | Gateway → сервисы | 200 rps на сервис |
| **RateLimiter** | Notification → Telegram | 25 msg/s (лимит Telegram API) |

### 7.3. Kafka как буфер

- Падение потребителя не блокирует продюсеров — события накапливаются в топиках
  (retention 7 дней).
- При восстановлении потребитель нагоняет пропущенные события.
- Outbox-таблица гарантирует: события не теряются даже при падении Kafka
  (relay повторяет до бесконечности).
- Ограничение: если потребитель лежит дольше retention — события потеряны.
  Покрывается мониторингом consumer lag.

### 7.4. Дедупликация

Kafka + outbox-relay дают at-least-once. Каждый потребитель обязан быть
идемпотентным.

| Уровень | Стратегия |
|---------|-----------|
| Kafka consumer (все сервисы) | Таблица `processed_events(event_id PK)`. Перед обработкой — INSERT, duplicate key → skip |
| Accounting (event sourcing) | `event_id UNIQUE` в events-таблице, повторный INSERT игнорируется |
| Notification → Telegram | `sent_notifications(event_id PK)`, перед отправкой — INSERT |
| Gateway → сервисы | `Idempotency-Key` header, кэш ответов на 1 час |

### 7.5. Dead Letter Queue

Poison pill (невалидные данные, необрабатываемое событие после всех retry) →
отдельный DLQ-топик на сервис. Основной consumer продолжает работу.

Метрика `dlq_messages_total > 0` → Prometheus-алерт. Разбор: ручной, через
прямой доступ к Kafka. Админские эндпоинты `GET/POST/DELETE /admin/dlq`
добавляются после MVP.

### 7.6. Пограничные сценарии

| Сценарий | Поведение |
|----------|-----------|
| Нет доступных попугов для ассайна (только admin/manager) | Ошибка `BUSINESS_RULE_VIOLATION` при создании задачи. Задача не создаётся |
| Реассайн на того же попуга (`newAssigneeId == oldAssigneeId`) | Пропустить. Деньги не возвращаются и не списываются |
| Задача закрыта во время shuffle | Optimistic locking: shuffle обновляет `WHERE version=N AND status='open'`. Закрытые задачи пропускаются |
| Попуг отключён (`status=disabled`) | Открытые задачи остаются на нём. Баланс не обнуляется. Админ может переассайнить через shuffle |
| Часовой пояс выплат | Расчёт по UTC. UI отображает в локальной зоне пользователя |
| TaskCompleted пришло во время выплатного cron | Срез по `created_at` события: строго до начала расчёта — в сегодняшний день, после — в следующий |
| Попуг без telegram_chat_id | NotificationService уведомляет администраторов о невозможности доставки |
| Отрицательный баланс на конец дня | `Payout.amount_cents = max(0, balance)`. Долг переносится на следующий день. В аудитлоге — запись выплаты с суммой = 0 |
| Открытие выполненной задачи (reopen) | Не поддерживается. Переход статуса только open → completed |

## 8. Observability

### 8.1. Логирование

- `zio-logging` + Logback, структурированный JSON в stdout
- `trace_id` и `span_id` в каждом логе — корреляция с трейсингом
- Ошибки через типизированный error channel, не `println`

### 8.2. Трейсинг

- **OpenTelemetry** (`zio-telemetry`) — автоинструментация HTTP (tapir/zio-http),
  Kafka (producer/consumer), JDBC (Quill)
- Контекст трейса пробрасывается через Kafka-заголовки (W3C traceparent)
- Сквозная трассировка: Gateway → TaskService → Kafka → AccountingService
- Экспорт в **Jaeger** (один бинарь, `docker compose`, UI на порту 16686)

### 8.3. Метрики

- `zio-metrics-connectors` → Prometheus-эндпоинт `GET /metrics` на каждом сервисе
- **Prometheus** скрейпит → **Grafana** дашборды (порт 3000)
- Встроенные метрики ZIO: количество файбер, размеры очередей, тайминги
- Бизнесовые метрики:
  - Скорость создания/выполнения задач
  - Сумма списаний/начислений
  - Kafka consumer lag
  - `dlq_messages_total`
  - Статус CircuitBreaker на Gateway

### 8.4. Health Checks

| Эндпоинт | Назначение | Kubernetes probe |
|----------|-----------|-----------------|
| `GET /health` | Liveness — жив ли процесс | `livenessProbe` |
| `GET /ready` | Readiness — БД и Kafka доступны? | `readinessProbe` |
| `GET /metrics` | Prometheus scrape target | — |

## 9. Базы данных и миграции

Один инстанс Postgres, логическое разделение на БД. Миграции — Flyway,
в `modules/<service>/src/main/resources/db/migration/`.

Bootstrap (`docker/postgres/init.sql`):
```sql
CREATE DATABASE ates_auth;
CREATE DATABASE ates_task;
CREATE DATABASE ates_accounting;
CREATE DATABASE ates_analytics;
CREATE DATABASE ates_notification;
```

### 9.1. Auth Service — `ates_auth`

**V1__create_users.sql:**
```sql
CREATE TABLE users (
  id         UUID PRIMARY KEY,
  name       VARCHAR(255) NOT NULL,
  email      VARCHAR(255) NOT NULL UNIQUE,
  role       VARCHAR(50)  NOT NULL CHECK (role IN ('popug', 'manager', 'accountant', 'admin')),
  status     VARCHAR(50)  NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
  created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE refresh_tokens (
  id         UUID PRIMARY KEY,
  user_id    UUID         NOT NULL REFERENCES users(id),
  hash       VARCHAR(255) NOT NULL UNIQUE,
  expires_at TIMESTAMPTZ  NOT NULL,
  revoked    BOOLEAN      NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_refresh_tokens_user_id ON refresh_tokens(user_id);
CREATE INDEX idx_refresh_tokens_hash    ON refresh_tokens(hash);
```

**V2__seed_admin.sql:**
```sql
INSERT INTO users (id, name, email, role)
VALUES ('00000000-0000-0000-0000-000000000001', 'Admin', 'admin@uberpopug.inc', 'admin');
```

### 9.2. TaskService — `ates_task`

**V1__create_tasks.sql:**
```sql
CREATE TABLE tasks (
  id                     UUID PRIMARY KEY,
  title                  VARCHAR(500) NOT NULL,
  description            TEXT,
  status                 VARCHAR(50)  NOT NULL DEFAULT 'open'
                           CHECK (status IN ('open', 'completed')),
  assignee_id            UUID         NOT NULL,
  assign_fee_cents       BIGINT       NOT NULL CHECK (assign_fee_cents > 0),
  complete_reward_cents  BIGINT       NOT NULL CHECK (complete_reward_cents > 0),
  created_at             TIMESTAMPTZ  NOT NULL DEFAULT now(),
  completed_at           TIMESTAMPTZ,
  version                INT          NOT NULL DEFAULT 1
);

CREATE INDEX idx_tasks_assignee ON tasks(assignee_id, status);
CREATE INDEX idx_tasks_status   ON tasks(status);
```

**V2__create_outbox.sql:**
```sql
CREATE TABLE outbox (
  id           BIGSERIAL    PRIMARY KEY,
  aggregate_id UUID         NOT NULL,
  event_type   VARCHAR(100) NOT NULL,
  payload      BYTEA        NOT NULL,
  created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  published    BOOLEAN      NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_outbox_unpublished ON outbox(published, created_at)
  WHERE published = FALSE;
```

**V3__create_processed_events.sql:**
```sql
CREATE TABLE processed_events (
  event_id     UUID         PRIMARY KEY,
  event_type   VARCHAR(100) NOT NULL,
  processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

### 9.3. AccountingService — `ates_accounting`

**V1__create_event_store.sql:**
```sql
CREATE TABLE events (
  id            BIGSERIAL    PRIMARY KEY,
  event_id      UUID         NOT NULL UNIQUE,
  event_type    VARCHAR(100) NOT NULL,
  aggregate_id  UUID         NOT NULL,
  payload       BYTEA        NOT NULL,
  metadata      JSONB,
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_events_aggregate ON events(aggregate_id, created_at);
CREATE INDEX idx_events_event_id  ON events(event_id);
```

**V2__create_account_balances.sql:**
```sql
CREATE TABLE account_balances (
  user_id       UUID PRIMARY KEY,
  balance_cents BIGINT NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**V3__create_outbox.sql:**
```sql
CREATE TABLE outbox (
  id           BIGSERIAL    PRIMARY KEY,
  aggregate_id UUID         NOT NULL,
  event_type   VARCHAR(100) NOT NULL,
  payload      BYTEA        NOT NULL,
  created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  published    BOOLEAN      NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_outbox_unpublished ON outbox(published, created_at)
  WHERE published = FALSE;
```

**V4__create_processed_events.sql:**
```sql
CREATE TABLE processed_events (
  event_id     UUID         PRIMARY KEY,
  event_type   VARCHAR(100) NOT NULL,
  processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

### 9.4. AnalyticsService — `ates_analytics`

**V1__create_tables.sql:**
```sql
CREATE TABLE tasks (
  task_id               UUID PRIMARY KEY,
  title                 VARCHAR(500) NOT NULL,
  assign_fee_cents      BIGINT NOT NULL,
  complete_reward_cents BIGINT NOT NULL,
  status                VARCHAR(50) NOT NULL DEFAULT 'open',
  created_at            TIMESTAMPTZ NOT NULL,
  completed_at          TIMESTAMPTZ
);
CREATE INDEX idx_tasks_completed ON tasks(completed_at);

CREATE TABLE popug_balances (
  user_id       UUID PRIMARY KEY,
  name          VARCHAR(255) NOT NULL,
  balance_cents BIGINT NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE daily_stats (
  date                           DATE PRIMARY KEY,
  top_management_earnings_cents  BIGINT NOT NULL DEFAULT 0,
  popugs_total                   INT    NOT NULL DEFAULT 0,
  popugs_negative                INT    NOT NULL DEFAULT 0,
  updated_at                     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE processed_events (
  event_id     UUID         PRIMARY KEY,
  event_type   VARCHAR(100) NOT NULL,
  processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

### 9.5. NotificationService — `ates_notification`

**V1__create_tables.sql:**
```sql
CREATE TABLE popug_telegram (
  popug_id         UUID PRIMARY KEY,
  telegram_chat_id VARCHAR(100) NOT NULL,
  created_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE sent_notifications (
  event_id   UUID         PRIMARY KEY,
  popug_id   UUID         NOT NULL,
  event_type VARCHAR(100) NOT NULL,
  sent_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE processed_events (
  event_id     UUID         PRIMARY KEY,
  event_type   VARCHAR(100) NOT NULL,
  processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

### 9.6. Сводка таблиц

| Сервис | Таблицы |
|--------|---------|
| Auth | `users`, `refresh_tokens` |
| TaskService | `tasks`, `outbox`, `processed_events` |
| Accounting | `events`, `account_balances`, `outbox`, `processed_events` |
| Analytics | `tasks`, `popug_balances`, `daily_stats`, `processed_events` |
| Notification | `popug_telegram`, `sent_notifications`, `processed_events` |
| Gateway | — (stateless) |

## 10. Организация проекта

### 10.1. Монорепо

Единый sbt-проект с независимыми модулями. Scala 3.8.4, sbt 2.0.6, JDK 21.

```
a-tes/
├── build.sbt                         # root aggregator
├── project/
│   ├── build.properties              # sbt.version=2.0.6
│   └── plugins.sbt                   # scalafmt, sbt-native-packager, scalapb
├── .scalafmt.conf
├── docker-compose.yml
├── docs/a-tes.md
├── frontend/                         # HTML/CSS/JS
│   └── Dockerfile                    # nginx
├── docker/
│   ├── postgres/init.sql
│   ├── prometheus.yml
│   └── grafana/dashboards/
└── modules/
    ├── common/                       # shared: proto-схемы, Kafka-конфиг, доменные типы
    ├── auth/
    │   ├── src/main/scala/
    │   └── src/main/resources/db/migration/
    ├── task-service/
    │   ├── src/main/scala/
    │   └── src/main/resources/db/migration/
    ├── accounting/
    │   ├── src/main/scala/
    │   └── src/main/resources/db/migration/
    ├── analytics/
    │   ├── src/main/scala/
    │   └── src/main/resources/db/migration/
    ├── notification/
    │   ├── src/main/scala/
    │   └── src/main/resources/db/migration/
    └── gateway/
        └── src/main/scala/           # stateless, без БД
```

### 10.2. Docker

Сборка образов — `sbt-native-packager` (плагин `sbt-docker` или
`Docker/publishLocal`). Каждый модуль собирает свой образ.

Фронтенд — отдельный Dockerfile с nginx, проксирующий `/api/*` → `gateway:10002`.

### 10.3. docker-compose.yml

Контейнеры:

| Контейнер | Порт | Назначение |
|-----------|------|-----------|
| `postgres` | 5432 | Один инстанс, 5 БД |
| `kafka` + `zookeeper` | 9092 | Event bus |
| `schema-registry` | 8081 | Protobuf-схемы |
| `jaeger` | 16686 | UI трейсинга |
| `prometheus` | 9090 | Сбор метрик |
| `grafana` | 3000 | Дашборды |
| `auth` | — | Сервис 1 |
| `task-service` | — | Сервис 2 |
| `accounting` | — | Сервис 3 |
| `analytics` | — | Сервис 4 |
| `notification` | — | Сервис 5 |
| `gateway` | 10002 | API Gateway |
| `nginx` | 80 | Фронтенд + прокси `/api/*` → gateway:10002 |

## 11. Технические дополнения

- Никакого сложного UI-дизайна — банальный бутстрап или чистый HTML.
- Никакого реалтайма — рефреш страницы для всех дашбордов.
- Нагрузка: не более 100 пользователей в минуту.
