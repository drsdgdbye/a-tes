# aTES Frontend

Статический фронтенд платформы aTES (UberPopug Inc Task Exchange): чистые
HTML/CSS/JS без бандлера, раздаётся nginx. Все запросы идут на `/api/*`
относительно origin фронтенда, nginx срезает префикс и проксирует их в API
Gateway (`gateway:10002`). Без реалтайма — страницы перезагружаются.

## Страницы

| Страница | Файл | Роль | Содержимое |
|----------|------|------|------------|
| Логин + регистрация | `index.html` | public | вход, саморегистрация (кнопка по флагу `GET /auth/config`) |
| Таск-трекер | `tasks.html` | все | список моих задач, создать, выполнить; «Заассайнить все» — admin/manager |
| Аккаунтинг | `accounting.html` | все | баланс, аудитлог; доход менеджмента — admin/accountant |
| Аналитика | `analytics.html` | admin | доход менеджмента, попуги в минусе, самая дорогая задача |

Навигация рендерится `js/common.js` по роли из JWT (декод payload клиентом).

## Структура

```
frontend/
├── Dockerfile        # nginx:1.27-alpine + статика
├── nginx.conf        # статика на :80, /api/* → gateway:10002 (rewrite)
├── index.html        # логин / регистрация
├── tasks.html        # дашборд таск-трекера
├── accounting.html   # дашборд аккаунтинга
├── analytics.html    # дашборд аналитики
├── css/style.css
└── js/
    ├── api.js        # fetch-обёртка: Bearer, авто-refresh при 401, logout
    ├── auth.js       # логин/регистрация, декод JWT (sub, role)
    ├── common.js     # нав-бар по ролям, формат $/дат, сообщения
    ├── tasks.js
    ├── accounting.js
    └── analytics.js
```

## Запуск

Требуется поднятый стек (postgres, kafka, сервисы, gateway):

```bash
docker compose up -d --build frontend
# UI: http://localhost:80  (логин / tasks.html / accounting.html / analytics.html)
```

Seed-админ: `admin@uberpopug.inc` / `admin`. Новые попуги — через кнопку
«Зарегистрироваться» на странице входа.

Для dev-просмотра статики без Docker можно открыть файлы напрямую — но API
доступен только через nginx (same-origin, `/api/*`).

## Как это работает

- Токены (`access` + `refresh`) хранятся в `localStorage`
  (`ates_access_token` / `ates_refresh_token`).
- Каждый запрос идёт с `Authorization: Bearer <access>`. При `401` `js/api.js`
  обновляет пару через `POST /auth/refresh` и повторяет запрос один раз; если
  refresh не удался — токены очищаются, редирект на логин.
- Роль пользователя читается из payload access-токена (`role`), поэтому
  навигация и кнопки прячутся по роли; backend всё равно проверяет права
  (роли на UI — косметика).

## Используемые эндпоинты

Публичные (без JWT): `POST /auth/login`, `POST /auth/register`,
`GET /auth/config`, `POST /auth/refresh`, `POST /auth/logout`.

С JWT (через gateway): `GET/POST /tasks`, `PATCH /tasks/{id}/complete`,
`POST /tasks/shuffle`, `GET /accounts/me/balance`,
`GET /accounts/me/audit-log`, `GET /accounts/top-management-earnings`,
`GET /analytics/top-management-earnings`, `GET /analytics/popugs-in-minus`,
`GET /analytics/most-expensive-task`.

Контракты тел/ответов — в `docs/a-tes.md` §5 (деньги — строки `"25.00"`,
даты — ISO-8601 UTC, ошибки — `{ "error", "message" }`).

## Ограничения

- Статический JS не покрывается автотестами (нет фреймворка); проверка —
  E2E-прогон стека (см. `docs/test-plan.md` §7 и версию `[0.9.0]`).
- Управления пользователями (создание/смена пароля админом) на UI нет —
  вне скоупа 0.8.0.
