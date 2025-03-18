```markdown
PGSyncer

Инструмент для синхронизации данных и структуры (DDL) между двумя PostgreSQL-базами, написанный на Go.

Возможности

- Синхронизация схемы (DDL) через `pg_dump` или ручной обход объектов (таблицы, индексы, представления, функции, триггеры, расширения и т. д.)
- Синхронизация данных:
  - Полный дифф (сравнение всех строк)  
  - Чанкование (по PK) для больших таблиц  
  - Инкрементальная синхронизация, если в таблицах есть `updated_at`  
  - Параллельная обработка таблиц (worker pool)  
  - Автоматическое удаление «лишних» таблиц в резервной БД (опционально)  
- Поддержка FDW (foreign data wrapper), позволяющая напрямую копировать данные из основной БД в резервную.  
- Batch upsert (INSERT ... ON CONFLICT), чтобы быстрее обновлять/вставлять строки.  

Установка

1. Убедитесь, что у вас установлен Go (1.18+).  
2. Клонируйте репозиторий:
   git clone https://github.com/Striker2k18/pgsyncer.git
   cd pgsyncer

3. Скомпилируйте:
   go build -o pgsyncer .

4. (Необязательно) Установите бинарный файл в `$GOPATH/bin` или другое место в PATH:
   go install .


Использование

Минимально программа требует две строки подключения – к основной (–maindsn) и резервной (–standindsn). Пример:


./pgsyncer \
  --maindsn="postgres://user:pass@localhost:5432/main_db?sslmode=disable" \
  --standindsn="postgres://user:pass@localhost:5432/standin_db?sslmode=disable"


Основные флаги (см. `--help` для полного списка):
- `--sync-schema` (bool, по умолч. true) – синхронизировать структуру (DDL) через `pg_dump`.
- `--sync-data` (bool, по умолч. true) – синхронизировать данные.
- `--clean-extra` (bool, по умолч. false) – удалять «лишние» объекты в резервной БД, которые отсутствуют в основной.
- `--fdw-mode` (bool, по умолч. false) – включить режим использования FDW.
- `--use-updated-at` (bool, по умолч. false) – включить инкрементальную синхронизацию на основе поля `updated_at`.
- `--chunk-size` (int, по умолч. 10000) – размер чанка при синхронизации больших таблиц.
- `--schema` (string, по умолч. `public`) – какую схему синхронизировать.
- `--workers` (int, по умолч. 4) – число параллельных воркеров для синхронизации таблиц.
- `--last-sync-time` (string) – время последней синхронизации в формате `YYYY-MM-DD HH:MM:SS`, нужно при `--use-updated-at`.
- `--pgdump` (string, по умолч. `pg_dump`) – путь к утилите pg_dump.
- `--force-psql` (bool, по умолч. false) – применять DDL через `psql -f -`, а не через ExecContext.

Пример запуска с инкрементальной синхронизацией:

./pgsyncer \
  --maindsn="postgres://user:pass@mainhost:5432/main_db?sslmode=disable" \
  --standindsn="postgres://user:pass@standinhost:5432/standin_db?sslmode=disable" \
  --use-updated-at \
  --last-sync-time="2025-03-01 00:00:00"


## Принцип работы

1. Синхронизация схемы (если `--sync-schema`):  
   - Запускается `pg_dump --schema-only --schema=...` против основной БД.  
   - Полученный SQL-прослойка применяется в резервной БД (или через `psql`, или непосредственно через `Exec`).  

2. Синхронизация данных (если `--sync-data`):
   - Создаётся транзакция `REPEATABLE READ` в основной БД, чтобы зафиксировать «моментальный срез» данных.  
   - Список таблиц собирается из `information_schema`.  
   - Каждая таблица обрабатывается (возможно, параллельно) и сравнивается с соответствующей таблицей в резервной БД (по PK).  
   - Поддерживается:
     - chunk-based (при большом количестве строк и числовом PK, запросы `WHERE pk BETWEEN ...`)  
     - updated_at-based (смотрим, какие строки обновлялись с момента `--last-sync-time`)  
     - batch upsert (INSERT ... ON CONFLICT) для ускорения обновления и вставки.  
   - При `--clean-extra` удаляются таблицы, которые есть в резервной БД, но отсутствуют в основной.

3. FDW Mode (если `--fdw-mode`):
   - В резервной БД автоматически создаётся (если нет) расширение `postgres_fdw`, сервер, user mapping, и через `IMPORT FOREIGN SCHEMA` подключаются таблицы из основной БД как foreign tables.  
   - Это даёт возможность выполнять `INSERT INTO local_table SELECT * FROM foreign_table` напрямую, без выгрузки в Go.

Системные требования

- PostgreSQL 9.5+ (из-за `ON CONFLICT DO UPDATE` и postgres_fdw; лучше 10+).  
- Go 1.18+.  
- Утилиты `pg_dump`, `psql` (если включён `--force-psql`).
