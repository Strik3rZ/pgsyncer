package main

import (
    "context"
    "database/sql"
    "fmt"
    "log"
    "sync"
)

// SyncData — основной процесс синхронизации данных
func SyncData(cfg *Config) error {
    log.Println("[Data] Начало синхронизации данных...")

    // 1) Открываем транзакцию REPEATABLE READ в mainDB (для «моментального среза»).
    txOpts := &sql.TxOptions{
        Isolation: sql.LevelRepeatableRead,
        ReadOnly:  false,
    }
    mainTx, err := mainDB.BeginTx(context.Background(), txOpts)
    if err != nil {
        return fmt.Errorf("BeginTx mainDB: %v", err)
    }
    defer mainTx.Rollback()

    // 2) Получаем список таблиц в main
    mainTables, err := listTables(mainTx, cfg.Schema)
    if err != nil {
        return fmt.Errorf("listTables(mainDB): %v", err)
    }
    log.Printf("[Data] Найдено %d таблиц в схеме %q основной БД", len(mainTables), cfg.Schema)
    if len(mainTables) == 0 {
        log.Println("[Data] В основной БД нет таблиц для обработки, выходим.")
        if err := mainTx.Commit(); err != nil {
            return fmt.Errorf("Commit mainTx (no tables): %v", err)
        }
        return nil
    }

    // 3) Если нужно, удаляем «лишние» таблицы в standinDB
    standinTables, err := listTables(standinDB, cfg.Schema)
    if err != nil {
        return fmt.Errorf("listTables(standinDB): %v", err)
    }
    if cfg.CleanExtra {
        log.Println("[Data] Удаление лишних таблиц включено (clean-extra).")
        if err := dropExtraTables(standinTables, mainTables, cfg.Schema); err != nil {
            return err
        }
    }

    // 4) Готовим worker pool для параллельной обработки
    workerCount := cfg.Workers
    if workerCount < 1 {
        workerCount = 1
    }
    log.Printf("[Data] Запускаем %d воркеров для синхронизации %d таблиц", workerCount, len(mainTables))

    // Канал с названиями таблиц
    tableCh := make(chan string, len(mainTables))
    for _, t := range mainTables {
        tableCh <- t
    }
    close(tableCh)

    var wg sync.WaitGroup
    errCh := make(chan error, workerCount)

    // Запускаем воркеры
    for i := 0; i < workerCount; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            for tbl := range tableCh {
                log.Printf("[Worker %d] Начало syncTableData для таблицы %s", workerID, tbl)
                if err := syncTableData(cfg, mainTx, tbl); err != nil {
                    log.Printf("[Worker %d] Ошибка syncTableData(%s): %v", workerID, tbl, err)
                    errCh <- err
                    // Выходим из воркера, чтобы не продолжать
                    return
                }
            }
            log.Printf("[Worker %d] Завершение воркера (таблицы закончились).", workerID)
        }(i + 1)
    }

    // Ждём, когда все горутины закончат
    wg.Wait()
    close(errCh)

    // Смотрим, были ли ошибки
    for e := range errCh {
        if e != nil {
            // Прерываемся на первой попавшейся ошибке.
            return e
        }
    }

    // Если дошли сюда — значит все воркеры закончили без ошибок
    if err := mainTx.Commit(); err != nil {
        return fmt.Errorf("Commit mainTx: %v", err)
    }
    log.Println("[Data] Синхронизация данных успешно завершена (все таблицы).")
    return nil
}

// listTables — возвращает список таблиц (table_name) из information_schema.tables для заданной схемы
func listTables(db interface {
    QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}, schema string) ([]string, error) {
    q := `
SELECT table_name
FROM information_schema.tables
WHERE table_schema = $1
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
`
    rows, err := db.QueryContext(context.Background(), q, schema)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var tables []string
    for rows.Next() {
        var t string
        if err := rows.Scan(&t); err != nil {
            return nil, err
        }
        tables = append(tables, t)
    }
    return tables, rows.Err()
}

// dropExtraTables — удаляет в standinDB те таблицы, которых нет в mainDB
func dropExtraTables(standinTables, mainTables []string, schema string) error {
    mainSet := make(map[string]bool, len(mainTables))
    for _, t := range mainTables {
        mainSet[t] = true
    }
    for _, t := range standinTables {
        if !mainSet[t] {
            dropSQL := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s" CASCADE;`, schema, t)
            if _, err := standinDB.Exec(dropSQL); err != nil {
                log.Printf("[CLEAN] Не смогли удалить лишнюю таблицу %s: %v", t, err)
            } else {
                log.Printf("[CLEAN] Удалена таблица %s, которой нет в mainDB", t)
            }
        }
    }
    return nil
}
