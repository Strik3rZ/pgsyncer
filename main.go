package main

import (
    "database/sql"
    "fmt"
    "log"
    "os"
    "os/exec"

    _ "github.com/jackc/pgx/v5/stdlib"
)

var (
    mainDB    *sql.DB
    standinDB *sql.DB
)

func main() {
    // 1) Считываем конфиг
    cfg := ParseConfigFromFlags()

    // 2) Подключаемся к БД
    var err error
    mainDB, err = sql.Open("pgx", cfg.MainDSN)
    if err != nil {
        log.Fatalf("Ошибка подключения к main DB: %v", err)
    }
    defer mainDB.Close()

    standinDB, err = sql.Open("pgx", cfg.StandinDSN)
    if err != nil {
        log.Fatalf("Ошибка подключения к standin DB: %v", err)
    }
    defer standinDB.Close()

    // Проверим работоспособность
    if err := mainDB.Ping(); err != nil {
        log.Fatalf("Ping mainDB: %v", err)
    }
    if err := standinDB.Ping(); err != nil {
        log.Fatalf("Ping standinDB: %v", err)
    }

    // 3) Убедимся, что pg_dump доступен
    if _, err := exec.LookPath(cfg.PgDumpPath); err != nil {
        log.Printf("[WARN] pg_dump не найден в PATH: %v", err)
        // если критично — можно сделать fatal
    }

    // 4) Если включён FDWMode — настраиваем fdw
    if cfg.FDWMode {
        if err := setupFDW(cfg); err != nil {
            log.Fatalf("setupFDW ошибка: %v", err)
        }
    }

    // 5) Синхронизация структуры (DDL)
    if cfg.SyncSchema {
        if err := SyncSchema(cfg); err != nil {
            log.Fatalf("SyncSchema ошибка: %v", err)
        }
    }

    // 6) Синхронизация данных
    if cfg.SyncData {
        if err := SyncData(cfg); err != nil {
            log.Fatalf("SyncData ошибка: %v", err)
        }
    }

    fmt.Println("Синхронизация завершена.")
    os.Exit(0)
}
