package main

import (
    "bufio"
    "bytes"
    "context"
    "fmt"
    "log"
    "os/exec"
    "strings"
)

// SyncSchema — синхронизирует структуру (DDL) через pg_dump.
func SyncSchema(cfg *Config) error {
    log.Println("[Schema] Синхронизация структуры...")

    // Формируем аргументы для pg_dump
    args := []string{
        "--schema-only",
        "--schema", cfg.Schema,
        "--no-owner",
        "--no-privileges",
        cfg.MainDSN,
    }

    // Выводим debug-информацию (полезно при отладке)
    log.Printf("[DEBUG] Выполняем: %s %v", cfg.PgDumpPath, args)

    // Запускаем pg_dump
    dumpCmd := exec.Command(cfg.PgDumpPath, args...)

    var out bytes.Buffer
    var stderr bytes.Buffer
    dumpCmd.Stdout = &out
    dumpCmd.Stderr = &stderr

    if err := dumpCmd.Run(); err != nil {
        return fmt.Errorf("pg_dump ошибка: %v (stderr=%s)", err, stderr.String())
    }

    ddl := out.String()
    if ddl == "" {
        log.Println("[Schema] pg_dump вернул пустой результат. Возможно, в схеме нет объектов.")
        return nil
    }

    // Логируем общий объём полученного DDL
    log.Printf("[Schema] Получен DDL размером ~%d байт", len(ddl))

    // Выбор способа применения DDL
    if cfg.ForcePsqlApply {
        // Применяем DDL через psql
        log.Println("[Schema] Применяем DDL через psql...")
        if err := applyDDLviaPSQL(cfg, ddl); err != nil {
            return fmt.Errorf("ошибка applyDDLviaPSQL: %v", err)
        }
    } else {
        log.Println("[Schema] Применяем DDL через ExecContext (построчная обработка)...")
        if err := applyDDLToStandin(ddl); err != nil {
            return fmt.Errorf("ошибка applyDDLToStandin: %v", err)
        }
    }

    log.Println("[Schema] Структура синхронизирована.")
    return nil
}

func applyDDLToStandin(ddl string) error {
    ctx := context.Background()
    scanner := bufio.NewScanner(strings.NewReader(ddl))

    var stmtBuilder strings.Builder

    for scanner.Scan() {
        line := scanner.Text()
        trimmed := strings.TrimSpace(line)

        // Накопим строчки, пока не дойдём до ;, оканчивающей SQL-выражение.
        stmtBuilder.WriteString(line + "\n")

        if len(trimmed) > 0 && trimmed[len(trimmed)-1] == ';' {
            statement := stmtBuilder.String()

            _, err := standinDB.ExecContext(ctx, statement)
            if err != nil {
                // Можно сделать return err, если хотим прерывать при ошибке
                log.Printf("[WARN] DDL exec ошибка: %v\nSQL:\n%s\n", err, statement)
            }
            stmtBuilder.Reset()
        }
    }
    return scanner.Err()
}

// applyDDLviaPSQL — альтернатива: запускаем psql -f - и подаём DDL на stdin
func applyDDLviaPSQL(cfg *Config, ddl string) error {

    cmd := exec.Command("psql", cfg.StandinDSN)

    stdin, err := cmd.StdinPipe()
    if err != nil {
        return fmt.Errorf("psql stdin pipe: %v", err)
    }

    // Пишем в stdin содержимое ddl
    go func() {
        defer stdin.Close()
        _, _ = stdin.Write([]byte(ddl))
    }()

    out, err := cmd.CombinedOutput()
    if err != nil {
        return fmt.Errorf("psql ошибка: %v (output=%s)", err, string(out))
    }

    // Логируем вывод psql, если он не пуст
    if len(out) > 0 {
        log.Printf("[psql output]\n%s", string(out))
    }
    return nil
}
