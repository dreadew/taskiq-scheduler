# Taskiq Scheduler

[![CI](https://github.com/dreadew/taskiq-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/dreadew/taskiq-scheduler/actions/workflows/ci.yml)

Асинхронный сервис для выполнения фоновых задач с мониторингом и очередями.

### Настройка переменных окружения

Поиск файлов идет следующим образом:

- Основное приложение: .env -> .env.local
- Тесты: .env.test

Строки подключения и переменные указанные ниже из из `.env` используются при запуске приложения через Docker, поэтому необходимо, чтобы там не указывался `localhost` и были указаны корректные порты. Пример:

```text
ASYNC_DSN=postgresql+asyncpg://postgres:postgres@postgres:5432/postgres
SYNC_DSN=postgresql+psycopg2://postgres:postgres@postgres:5432/postgres
NATS_HOST=nats
NATS_PORT=4222
```

Строки подключения и переменные указанные ниже из `.env.local` используются при локальном запуске приложения, поэтому в них нельзя в качестве хоста указывать названия из Docker. Пример:

```text
ASYNC_DSN=postgresql+asyncpg://postgres:postgres@localhost:5433/postgres
SYNC_DSN=postgresql+psycopg2://postgres:postgres@localhost:5433/postgres
NATS_HOST=localhost
NATS_PORT=4222
```

Строки подключения и переменные указанные, используемые для тестов нужно так же указывать с названиями хостов из Docker. Однако для тестов можно создать отдельную БД. Пример:

```text
ASYNC_DSN=postgresql+asyncpg://postgres:postgres@postgres:5432/postgres
SYNC_DSN=postgresql+psycopg2://postgres:postgres@postgres:5432/postgres
NATS_HOST=nats
NATS_PORT=4222
```

Все остальные переменные не отличаются для `.env`, `.env.local` и `.env.test`

##### _Важный момент - фикстура clean_db удаляет все записи из указанной БД в тестах, поэтому ее запрещено использовать в production БД_

### Запуск тестов

Без интеграционных тестов:

```bash
poetry run pytest -m "not integration" -v
```

Интеграционные тесты:

```bash
poetry run pytest -m "integration" -v
```

Все тесты:

```bash
poetry run pytest -v
```

### Создание миграции

Автогенерация:

```bash
alembic revision --autogenerate -m "<comment>"
```

Ручное создание:

```bash
alembic revision -m "<comment>"
```

### Применение миграций

Применить все:

```bash
alembic upgrade head
```

Применить до конкретной ревизии:

```bash
alembic upgrade <revision_id>
```

### Откат

На 1 миграцию:

```bash
alembic downgrade -1
```

До конкретной:

```bash
alembic dowgrade <revision_id>
```

До базовой:

```bash
alembic downgrade base
```

### Просмотр истории

Все миграции:

```bash
alembic history --verbose
```

Краткая история:

```bash
alembic history
```

Показать текущую миграцию в БД:

```bash
alembic current
```

Показать какие миграции не применены:

```bash
alembic heads
```
