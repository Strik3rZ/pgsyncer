# PGSyncer

**PGSyncer** — инструмент для синхронизации структуры и данных между двумя базами PostgreSQL, написанный на Go.

## Возможности

- **Синхронизация схемы (DDL)**:
  - Через `pg_dump`
  - Или ручным обходом объектов: таблицы, индексы, представления, функции, триггеры, расширения и т. д.

- **Синхронизация данных**:
  - Полный дифф (сравнение всех строк)
  - Чанкование по первичному ключу (PK) для больших таблиц
  - Инкрементальная синхронизация на основе поля `updated_at`
  - Параллельная обработка таблиц (worker pool)
  - Опциональное удаление лишних объектов в резервной БД

- **FDW (foreign data wrapper)**:
  - Копирование данных напрямую из основной БД в резервную через FDW (`postgres_fdw`)

- **Batch upsert**:
  - Обновление/вставка строк через `INSERT ... ON CONFLICT`

---

## Установка

1. Убедитесь, что установлен Go **1.18+**
2. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/Strik3rZ/pgsyncer.git
   cd pgsyncer
   ```

3. Скомпилируйте бинарник:
   ```bash
   go build -o pgsyncer .
   ```

4. (Необязательно) Установите в `$GOPATH/bin`:
   ```bash
   go install .
   ```

---

## Использование

Минимально требуется указать строки подключения к основной и резервной БД:

```bash
./pgsyncer \
  --maindsn="postgres://user:pass@localhost:5432/main_db?sslmode=disable" \
  --standindsn="postgres://user:pass@localhost:5432/standin_db?sslmode=disable"
```

---

## Основные флаги

(полный список: `--help`)

| Флаг | Тип | Описание |
|------|-----|----------|
| `--maindsn` | string | Строка подключения к основной БД |
| `--standindsn` | string | Строка подключения к резервной БД |
| `--sync-schema` | bool (по умолч. `true`) | Синхронизировать структуру (DDL) через `pg_dump` |
| `--sync-data` | bool (по умолч. `true`) | Синхронизировать данные |
| `--clean-extra` | bool (по умолч. `false`) | Удалять объекты в резервной БД, которых нет в основной |
| `--fdw-mode` | bool (по умолч. `false`) | Использовать `postgres_fdw` для копирования |
| `--use-updated-at` | bool (по умолч. `false`) | Инкрементальная синхронизация по полю `updated_at` |
| `--last-sync-time` | string | Время последней синхронизации (`YYYY-MM-DD HH:MM:SS`) |
| `--chunk-size` | int (по умолч. `10000`) | Размер чанка для больших таблиц |
| `--schema` | string (по умолч. `public`) | Схема для синхронизации |
| `--workers` | int (по умолч. `4`) | Кол-во параллельных воркеров |
| `--pgdump` | string (по умолч. `pg_dump`) | Путь к утилите `pg_dump` |
| `--force-psql` | bool (по умолч. `false`) | Применять DDL через `psql -f -`, а не `ExecContext` |

---

## Пример: инкрементальная синхронизация

```bash
./pgsyncer \
  --maindsn="postgres://user:pass@mainhost:5432/main_db?sslmode=disable" \
  --standindsn="postgres://user:pass@standinhost:5432/standin_db?sslmode=disable" \
  --use-updated-at \
  --last-sync-time="2025-03-01 00:00:00"
```

---

## Принцип работы

### 1. Синхронизация схемы (`--sync-schema`)
- Запускается `pg_dump --schema-only --schema=...` для основной БД
- Полученный SQL применяется в резервной БД:
  - через `psql` (если `--force-psql`)
  - или напрямую через `ExecContext`

### 2. Синхронизация данных (`--sync-data`)
- Начинается транзакция с уровнем `REPEATABLE READ` для фиксации состояния
- Из `information_schema.tables` собирается список таблиц
- Каждая таблица обрабатывается:
  - Параллельно (через пул воркеров)
  - Сравнение строк по PK
  - Используются:
    - **Чанки**: `WHERE pk BETWEEN ...` (для больших таблиц)
    - **updated_at**: `WHERE updated_at > ?` (если включено)
  - Строки вставляются или обновляются через `INSERT ... ON CONFLICT`
  - При `--clean-extra` удаляются лишние таблицы

### 3. FDW Mode (`--fdw-mode`)
- В резервной БД:
  - Создаётся расширение `postgres_fdw`
  - Добавляется foreign server и user mapping
  - `IMPORT FOREIGN SCHEMA` подключает таблицы
- Таблицы подключаются как foreign, и можно делать `INSERT INTO ... SELECT * FROM ...`

---

## Системные требования

- PostgreSQL 9.5+  
  - (требуется `ON CONFLICT DO UPDATE` и `postgres_fdw`; рекомендуется 10+)
- Go 1.18+  
- Утилиты `pg_dump`, `psql` (если используется `--force-psql`)

---
