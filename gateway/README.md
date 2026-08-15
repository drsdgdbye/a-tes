# aTES API Gateway

Единая точка входа платформы aTES (UberPopug Inc Task Exchange): локальная
JWT-верификация по публичному ключу Auth и reverse proxy в downstream-сервисы
с resilience-политиками.

## Что делает сервис

- Верифицирует access-токены локально (ES256, подпись + `exp` + `sub` + `role`)
  без network call к Auth: ключ загружается при старте и периодически обновляется.
- Проксирует запросы по префиксам: `/auth/*` и `/users*` → auth,
  `/tasks*` → task-service, `/accounts*` → accounting, `/analytics*` → analytics.
  Неизвестный путь → `404`, без проверки JWT.
- Публичные пути Auth (`/auth/login`, `/auth/register`, `/auth/config`,
  `/auth/refresh`, `/auth/logout`, `/auth/keys`) проксируются без авторизации
  (их обслуживает сам Auth). Роли проверяются на стороне сервисов.
- Resilience на каждый downstream-сервис (rezilience): CircuitBreaker, Retry
  (только транзиентные ошибки), TimeLimiter (чтение/запись), Bulkhead, RateLimiter.
- `GET /health`, `GET /ready`, `GET /metrics` (Prometheus).

## Технологический стек

| Компонент | Библиотека |
|-----------|------------|
| Язык | Scala 3.8.4 |
| Эффекты | ZIO 2 (zio, zio-http, zio-logging) |
| HTTP-клиент | zio-http `Client` (batched) |
| JWT | nimbus-jose-jwt (ES256) |
| Resilience | rezilience (CB, retry, timeout, bulkhead, rate-limiter) |
| Конфиг | zio-config (HOCON + env-оверрайды) |
| Метрики | zio-metrics-connectors (Prometheus) |

## Структура проекта

```
gateway/
├── README.md
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   └── application.conf            # конфигурация (HOCON)
│   │   └── scala/inc/uberpopug/gateway/
│   │       ├── Main.scala                  # точка входа, сборка ZLayer-графа
│   │       ├── api/                        # маршруты, маппинг ошибок
│   │       ├── proxy/                      # RouteResolver, Resilience, DownstreamClient
│   │       ├── security/                   # JwtVerifier, KeysFetcher, KeyManager
│   │       └── config/                     # case classes конфига
│   └── test/scala/inc/uberpopug/gateway/   # ZIO Test (JWT, keys, routes, resilience, proxy)
```

Слои зависят строго внутрь: `api → proxy/security → config`. Сервис stateless.

## Запуск

Требования: JDK 21, sbt 2.0.6, запущенный Auth Service.

Через docker-compose (из корня репозитория):

```bash
docker-compose up -d postgres kafka auth
sbt gateway/Docker/publishLocal
docker-compose up -d gateway
```

Локально из IDE или sbt:

```bash
sbt gateway/run
```

По умолчанию сервис слушает порт `10002`, ключи берёт с
`http://localhost:10001/auth/keys`, downstream — на портах 10001/10003/10004/10005.
Если ключи ещё не загружены, `/ready` возвращает `503`, а защищённые запросы — `503`.

## Конфигурация

Все параметры в `gateway/src/main/resources/application.conf`, секция `ates`.
Каждое значение можно переопределить env-переменной.

| Параметр | Env | По умолчанию |
|----------|-----|--------------|
| `ates.server.port` | `ATES_SERVER_PORT` | `10002` |
| `ates.jwt.issuer` | `ATES_JWT_ISSUER` | `ates-auth` |
| `ates.jwt.keysUrl` | `ATES_JWT_KEYS_URL` | `http://localhost:10001/auth/keys` |
| `ates.jwt.refreshIntervalSeconds` | `ATES_JWT_KEYS_REFRESH_INTERVAL` | `60` |
| `ates.jwt.startupRetryIntervalSeconds` | `ATES_JWT_KEYS_STARTUP_RETRY_INTERVAL` | `2` |
| `ates.services.auth.baseUrl` | `ATES_SERVICES_AUTH_BASE_URL` | `http://localhost:10001` |
| `ates.services.taskService.baseUrl` | `ATES_SERVICES_TASK_SERVICE_BASE_URL` | `http://localhost:10003` |
| `ates.services.accounting.baseUrl` | `ATES_SERVICES_ACCOUNTING_BASE_URL` | `http://localhost:10004` |
| `ates.services.analytics.baseUrl` | `ATES_SERVICES_ANALYTICS_BASE_URL` | `http://localhost:10005` |
| `ates.resilience.circuitBreaker.maxFailures` | `ATES_RESILIENCE_CB_MAX_FAILURES` | `5` |
| `ates.resilience.circuitBreaker.resetIntervalSeconds` | `ATES_RESILIENCE_CB_RESET_INTERVAL` | `30` |
| `ates.resilience.retry.maxRetries` | `ATES_RESILIENCE_RETRY_MAX` | `3` |
| `ates.resilience.retry.initialDelayMillis` | `ATES_RESILIENCE_RETRY_INITIAL_DELAY` | `100` |
| `ates.resilience.retry.backoffFactor` | `ATES_RESILIENCE_RETRY_BACKOFF_FACTOR` | `2.0` |
| `ates.resilience.timeLimiter.readTimeoutSeconds` | `ATES_RESILIENCE_TIMEOUT_READ` | `5` |
| `ates.resilience.timeLimiter.writeTimeoutSeconds` | `ATES_RESILIENCE_TIMEOUT_WRITE` | `10` |
| `ates.resilience.bulkhead.maxConcurrent` | `ATES_RESILIENCE_BULKHEAD_MAX` | `20` |
| `ates.resilience.rateLimiter.rps` | `ATES_RESILIENCE_RATELIMIT_RPS` | `200` |

## API

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| GET | `/health` | public | Liveness-проверка |
| GET | `/ready` | public | Ключи Auth загружены? `200`/`503` |
| GET | `/metrics` | public | Метрики Prometheus |
| * | `/auth/login`, `/auth/refresh`, `/auth/logout`, `/auth/register`, `/auth/config`, `/auth/keys` | public | Проксируются в Auth без JWT |
| * | `/auth/*`, `/users*` | JWT | Проксируются в Auth |
| * | `/tasks*` | JWT | Проксируются в TaskService |
| * | `/accounts*` | JWT | Проксируются в Accounting |
| * | `/analytics*` | JWT | Проксируются в Analytics |
| * | прочие | — | `404` |

Проксирование сохраняет метод, путь, query, полезные заголовки (hop-by-hop
отбрасываются) и тело. Метод/роли проверяются на стороне сервисов.

### Примеры

```bash
curl -s localhost:10002/auth/keys
curl -s localhost:10002/tasks -H "Authorization: Bearer <accessToken>"
```

### Ошибки

Тело ошибки — единый формат `{ "error": "<код>", "message": "<описание>" }`.

| Код | HTTP | Ситуация |
|-----|------|----------|
| `unauthorized` | 401 | Отсутствует/невалидный/истёкший JWT |
| `not_found` | 404 | Неизвестный маршрут |
| `service_unavailable` | 503 | Ключи не загружены / circuit breaker open |
| `call_timed_out` | 504 | Таймаут чтения/записи downstream |
| `internal_error` | 500 | Ошибка чтения тела / сборки запроса |

## Тесты

```bash
sbt "gateway/Test/testFull"
```

Покрытие: JwtVerifier (подпись, `exp` через TestClock, issuer/subject/role),
KeyManager (startup-ретраи и обновление ключей), RouteResolver (маршрутизация
и публичные пути), Resilience (retry/circuit breaker/timeout на TestClock),
GatewayProxy (сквозной прокси-флоу через zio-http Server/Client).

## Известные особенности

- Ключ ES256 у Auth эфемерный: после рестарта Auth старые access-токены
  невалидны, Gateway подхватит новый ключ при следующем обновлении.
- `Idempotency-Key` пробрасывается в downstream как есть; кэш ответов на Gateway
  (спека §7.4) — TODO на следующий этап.