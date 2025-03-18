package main

import (
    "flag"
    "log"
    "os"
    "time"
)

type Config struct {
    MainDSN         string        // DSN основной БД
    StandinDSN      string        // DSN резервной БД
    SyncSchema      bool          // Синхронизировать структуру?
    SyncData        bool          // Синхронизировать данные?
    CleanExtra      bool          // Удалять объекты, отсутствующие в mainDB?
    FDWMode         bool          // Использовать FDW (foreign data wrapper)?
    UseUpdatedAt    bool          // Использовать столбец updated_at?
    ChunkSize       int           // Размер чанка для chunk-based синхронизации
    Schema          string        // Какую схему синхронизируем
    Workers         int           // Кол-во потоков для синхронизации таблиц
    LastSyncTime    time.Time     // Для инкрементальной синхронизации (updated_at > LastSyncTime)
    PgDumpPath      string        // Путь к pg_dump (если не в PATH)
    ForcePsqlApply  bool          // Если true, применяем DDL через psql, а не Exec
}

// ParseConfigFromFlags — читает конфигурацию из флагов
func ParseConfigFromFlags() *Config {
    cfg := &Config{}

    flag.StringVar(&cfg.MainDSN, "maindsn", getEnvOrDefault("MAIN_DSN", "postgres://user:pass@localhost:5432/main_db?sslmode=disable"),
        "DSN основной БД (или ENV MAIN_DSN)")
    flag.StringVar(&cfg.StandinDSN, "standindsn", getEnvOrDefault("STANDIN_DSN", "postgres://user:pass@localhost:5432/standin_db?sslmode=disable"),
        "DSN резервной БД (или ENV STANDIN_DSN)")

    flag.BoolVar(&cfg.SyncSchema, "sync-schema", true, "Синхронизировать структуру (DDL)")
    flag.BoolVar(&cfg.SyncData, "sync-data", true, "Синхронизировать данные")
    flag.BoolVar(&cfg.CleanExtra, "clean-extra", false, "Удалять объекты, отсутствующие в mainDB")
    flag.BoolVar(&cfg.FDWMode, "fdw-mode", false, "Использовать ли FDW")
    flag.BoolVar(&cfg.UseUpdatedAt, "use-updated-at", false, "Использовать ли столбец updated_at")
    flag.IntVar(&cfg.ChunkSize, "chunk-size", 10000, "Размер порции при чанковой синхронизации")
    flag.StringVar(&cfg.Schema, "schema", "public", "Схема для синхронизации")
    flag.IntVar(&cfg.Workers, "workers", 4, "Число горутин для синхронизации таблиц")

    var lastSync string
    flag.StringVar(&lastSync, "last-sync-time", "", "Время последней синхронизации (YYYY-MM-DD HH:MM:SS), если нужно updated_at")
    flag.StringVar(&cfg.PgDumpPath, "pgdump", "pg_dump", "Путь к pg_dump")
    flag.BoolVar(&cfg.ForcePsqlApply, "force-psql", false, "Применять DDL через psql, а не ExecContext")

    flag.Parse()

    // Если передано lastSyncTime, парсим
    if lastSync != "" {
        t, err := time.Parse("2006-01-02 15:04:05", lastSync)
        if err != nil {
            log.Fatalf("Неверный формат last-sync-time: %v", err)
        }
        cfg.LastSyncTime = t
    }

    return cfg
}

func getEnvOrDefault(envKey, def string) string {
    val := os.Getenv(envKey)
    if val == "" {
        return def
    }
    return val
}
