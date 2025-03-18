package main

import (
    "context"
    "fmt"
    "log"
    "net/url"
    "strings"
)

// setupFDW — создаёт расширение postgres_fdw в резервной БД, настраивает SERVER и USER MAPPING.
func setupFDW(cfg *Config) error {
    ctx := context.Background()

    // 1) Создаём EXTENSION postgres_fdw (если не существует)
    if _, err := standinDB.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS postgres_fdw;`); err != nil {
        return fmt.Errorf("CREATE EXTENSION postgres_fdw: %v", err)
    }
    log.Println("[FDW] Расширение postgres_fdw создано (если не было)")

    // 2) Разбираем cfg.MainDSN, чтобы получить user, pass, host, port, dbname, sslmode и т.д.
    dsnParts, err := parsePostgresDSN(cfg.MainDSN)
    if err != nil {
        return fmt.Errorf("parse DSN err: %v", err)
    }

    // Можно сделать имя сервера настраиваемым, а можно захардкодить. Например:
    serverName := "main_server"

    // 3) CREATE SERVER ... IF NOT EXISTS
    var extraOpts string
    if dsnParts.sslmode != "" {
        extraOpts = fmt.Sprintf(", sslmode '%s'", dsnParts.sslmode)
    }

    createServer := fmt.Sprintf(`
CREATE SERVER IF NOT EXISTS %s
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host '%s', dbname '%s', port '%s' %s);
`,
        serverName, dsnParts.host, dsnParts.dbname, dsnParts.port, extraOpts)

    log.Printf("[FDW] Создаём сервер %q c хостом=%s dbname=%s port=%s sslmode=%s",
        serverName, dsnParts.host, dsnParts.dbname, dsnParts.port, dsnParts.sslmode)

    if _, err := standinDB.ExecContext(ctx, createServer); err != nil {
        return fmt.Errorf("CREATE SERVER: %v", err)
    }

    // 4) CREATE USER MAPPING IF NOT EXISTS
    createUserMap := fmt.Sprintf(`
CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
SERVER %s
OPTIONS (user '%s', password '%s');
`, serverName, dsnParts.user, dsnParts.password)

    log.Printf("[FDW] Создаём USER MAPPING для пользователя %q", dsnParts.user)

    if _, err := standinDB.ExecContext(ctx, createUserMap); err != nil {
        return fmt.Errorf("CREATE USER MAPPING: %v", err)
    }

    // 5) IMPORT FOREIGN SCHEMA
    // По умолчанию копируем все таблицы из cfg.Schema в эту же схему.
    importSQL := fmt.Sprintf(`
IMPORT FOREIGN SCHEMA "%s"
FROM SERVER %s
INTO "%s";
`, cfg.Schema, serverName, cfg.Schema)

    log.Printf("[FDW] IMPORT FOREIGN SCHEMA %q INTO %q", cfg.Schema, cfg.Schema)

    if _, err := standinDB.ExecContext(ctx, importSQL); err != nil {
        // Не всегда критично, поэтому делаем Warning
        log.Printf("[WARN] Не удалось импортировать схему %s: %v", cfg.Schema, err)
    }

    log.Println("[FDW] postgres_fdw настроен, схема импортирована (если удалось).")
    return nil
}

// dsnInfo хранит поля, извлечённые из DSN
type dsnInfo struct {
    user     string
    password string
    host     string
    port     string
    dbname   string
    sslmode  string
}

// parsePostgresDSN — упрощённый парсер для DSN формата postgres://user:pass@host:port/dbname?sslmode=xxx
func parsePostgresDSN(dsn string) (*dsnInfo, error) {
    u, err := url.Parse(dsn)
    if err != nil {
        return nil, err
    }

    info := &dsnInfo{}

    if u.User != nil {
        info.user = u.User.Username()
        pass, _ := u.User.Password()
        info.password = pass
    }

    // Хост и порт
    hostPart := strings.Split(u.Host, ":")
    info.host = hostPart[0]
    if len(hostPart) > 1 {
        info.port = hostPart[1]
    } else {
        info.port = "5432"
    }

    // Имя базы = всё, что после '/' в Path
    info.dbname = strings.TrimPrefix(u.Path, "/")

    // Попробуем забрать sslmode
    q := u.Query()
    if mode := q.Get("sslmode"); mode != "" {
        info.sslmode = mode
    }

    return info, nil
}
