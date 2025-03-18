package main

import (
    "context"
    "crypto/md5"
    "database/sql"
    "encoding/hex"
    "fmt"
    "log"
    "strconv"
    "strings"
)

// syncTableData — главный вход для синхронизации одной таблицы.
func syncTableData(cfg *Config, mainTx *sql.Tx, tableName string) error {
    schema := cfg.Schema

    // 1) Проверяем FDW-режим
    if cfg.FDWMode {
        log.Printf("[syncTableData] Таблица %s: FDW mode включён — пропускаем демонстрационную логику", tableName)
        // Здесь могла бы быть реализация syncTableFDW(...).
        return nil
    }

    // 2) Проверяем режим UpdatedAt
    if cfg.UseUpdatedAt {
        return syncTableByUpdatedAt(cfg, mainTx, tableName)
    }

    // 3) Пытаемся определить PK
    pkCols, numericPK := detectPK(mainTx, schema, tableName)
    if len(pkCols) == 0 {
        log.Printf("[WARN] Таблица %s не имеет PK (или не найдена). Используем полный дифф.", tableName)
        return syncTableFullDiff(cfg, mainTx, tableName, nil)
    }

    // Если PK числовой и единственный, используем chunk-based
    if numericPK && len(pkCols) == 1 {
        return syncTableByChunks(cfg, mainTx, tableName, pkCols[0])
    }

    // Иначе (составной PK, строковый PK и т.д.) — полный дифф
    log.Printf("[INFO] Таблица %s имеет составной/строковый PK или нет chunk-based, переходим на полный дифф.", tableName)
    return syncTableFullDiff(cfg, mainTx, tableName, pkCols)
}

// detectPK — пытается найти все столбцы, входящие в первичный ключ, 
// а также проверяет, единственный ли он и является ли он "numeric" (int/bigint).
func detectPK(tx *sql.Tx, schema, table string) ([]string, bool) {
    query := `
SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE i.indisprimary
  AND n.nspname = $1
  AND c.relname = $2
ORDER BY a.attnum;
`
    rows, err := tx.QueryContext(context.Background(), query, schema, table)
    if err != nil {
        log.Printf("[detectPK] Ошибка при запросе PK для %s.%s: %v", schema, table, err)
        return nil, false
    }
    defer rows.Close()

    var pkCols []string
    for rows.Next() {
        var col string
        if err := rows.Scan(&col); err != nil {
            log.Printf("[detectPK] Ошибка Scan PK: %v", err)
            return nil, false
        }
        pkCols = append(pkCols, col)
    }
    if len(pkCols) == 0 {
        // Нет PK
        return nil, false
    }

    // Если более одного столбца — считаем, что это составной ключ => numericPK = false
    if len(pkCols) > 1 {
        log.Printf("[detectPK] Таблица %s: составной PK = %v, считаем numericPK=false", table, pkCols)
        return pkCols, false
    }

    // Проверим тип единственного столбца, numeric ли он
    colTypeQuery := `
SELECT data_type
FROM information_schema.columns
WHERE table_schema = $1
  AND table_name = $2
  AND column_name = $3
`
    var dataType string
    err = tx.QueryRowContext(context.Background(), colTypeQuery, schema, table, pkCols[0]).Scan(&dataType)
    if err != nil {
        log.Printf("[detectPK] Ошибка при чтении типа столбца %s: %v", pkCols[0], err)
        return pkCols, false
    }
    dataType = strings.ToLower(dataType)
    numeric := false
    if strings.Contains(dataType, "int") {
        numeric = true
    }

    log.Printf("[detectPK] Таблица %s: PK=%v, numeric=%v", table, pkCols, numeric)
    return pkCols, numeric
}

// syncTableByChunks — разбивает PK-диапазон на чанки и синхронизирует каждый участок.
func syncTableByChunks(cfg *Config, mainTx *sql.Tx, tableName, pkCol string) error {
    schema := cfg.Schema
    ctx := context.Background()

    // Получаем динамический список столбцов (из mainTx) для чтения строк
    columns, err := getTableColumns(mainTx, schema, tableName)
    if err != nil {
        return fmt.Errorf("[syncTableByChunks] getTableColumns(%s): %v", tableName, err)
    }
    if len(columns) == 0 {
        log.Printf("[Chunks] Таблица %s не имеет столбцов (?), пропускаем.", tableName)
        return nil
    }

    // Считываем MIN и MAX значений PK
    qMinMax := fmt.Sprintf(`SELECT COALESCE(MIN("%s"),0), COALESCE(MAX("%s"),0) FROM "%s"."%s"`,
        pkCol, pkCol, schema, tableName)
    var minID, maxID int64
    if err := mainTx.QueryRowContext(ctx, qMinMax).Scan(&minID, &maxID); err != nil {
        return fmt.Errorf("[syncTableByChunks] MIN/MAX %s: %v", tableName, err)
    }
    if maxID < minID {
        log.Printf("[Chunks] Таблица %s пуста (min>max), пропускаем.", tableName)
        return nil
    }

    log.Printf("[Chunks] Таблица %s, PK=[%d..%d], chunkSize=%d", tableName, minID, maxID, cfg.ChunkSize)
    chunkSize := int64(cfg.ChunkSize)

    // Идём чанками
    for start := minID; start <= maxID; start += chunkSize {
        end := start + chunkSize - 1
        if end > maxID {
            end = maxID
        }

        // Читаем строки из mainDB
        mainData, rowsMain, err := fetchRowsRange(mainTx, schema, tableName, pkCol, columns, start, end)
        if err != nil {
            log.Printf("[syncTableByChunks] Ошибка fetchRowsRange main %s [%d..%d]: %v", tableName, start, end, err)
            continue
        }
        // Читаем строки из standinDB
        standinData, rowsStandin, err := fetchRowsRange(standinDB, schema, tableName, pkCol, columns, start, end)
        if err != nil {
            log.Printf("[syncTableByChunks] Ошибка fetchRowsRange standin %s [%d..%d]: %v", tableName, start, end, err)
            continue
        }

        // Сравниваем
        toInsert, toUpdate, toDelete := compareData(mainData, standinData)
        if len(toInsert)+len(toUpdate)+len(toDelete) == 0 {
            // В этом чанке нет различий
            continue
        }

        // Применяем
        if err := applyChanges(tableName, schema, pkCol, columns, toInsert, toUpdate, toDelete, rowsMain, rowsStandin); err != nil {
            log.Printf("[syncTableByChunks] Ошибка applyChanges chunk [%d..%d] (%s): %v", start, end, tableName, err)
        } else {
            log.Printf("[Chunks] %s [%d..%d]: +%d / ~%d / -%d",
                tableName, start, end, len(toInsert), len(toUpdate), len(toDelete))
        }
    }
    return nil
}

// getTableColumns — получает список всех столбцов таблицы из information_schema.columns.
func getTableColumns(db *sql.Tx, schema, table string) ([]string, error) {
    query := `
SELECT column_name
FROM information_schema.columns
WHERE table_schema = $1
  AND table_name = $2
ORDER BY ordinal_position;
`
    rows, err := db.QueryContext(context.Background(), query, schema, table)
    if err != nil {
        return nil, fmt.Errorf("getTableColumns: %v", err)
    }
    defer rows.Close()

    var cols []string
    for rows.Next() {
        var colName string
        if err := rows.Scan(&colName); err != nil {
            return nil, err
        }
        cols = append(cols, colName)
    }
    if err := rows.Err(); err != nil {
        return nil, err
    }
    return cols, nil
}

// fetchRowsRange — выбирает строки (все столбцы columns) из таблицы table
func fetchRowsRange(db interface {
    QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
},
    schema, table, pkCol string,
    columns []string,
    start, end int64,
) (
    map[string]string,         // map[pk]->hash
    map[string][]interface{},  // map[pk]->rowValues
    error,
) {
    ctx := context.Background()
    colList := quoteColumns(columns) // "col1","col2",...

    q := fmt.Sprintf(`SELECT %s FROM "%s"."%s" WHERE "%s" BETWEEN $1 AND $2`,
        colList, schema, table, pkCol,
    )
    rows, err := db.QueryContext(ctx, q, start, end)
    if err != nil {
        return nil, nil, err
    }
    defer rows.Close()

    dataHash := make(map[string]string)
    dataRows := make(map[string][]interface{})

    for rows.Next() {
        vals := make([]interface{}, len(columns))
        ptrs := make([]interface{}, len(vals))
        for i := range vals {
            ptrs[i] = &vals[i]
        }
        if err := rows.Scan(ptrs...); err != nil {
            return nil, nil, err
        }

        var pkVal string
        var sb strings.Builder
        for i, c := range columns {
            s := fmt.Sprintf("%v", vals[i])
            if c == pkCol {
                pkVal = s
            }
            sb.WriteString(s)
            sb.WriteString("|")
        }
        h := md5.Sum([]byte(sb.String()))
        dataHash[pkVal] = hex.EncodeToString(h[:])
        dataRows[pkVal] = vals
    }
    return dataHash, dataRows, rows.Err()
}

// compareData — сравнивает mainData vs standinData по pk->hash и возвращает
// списки pk для вставки, обновления, удаления.
func compareData(mainData, standinData map[string]string) (insert, update, del []string) {
    for pk, hash := range mainData {
        stHash, ok := standinData[pk]
        if !ok {
            insert = append(insert, pk)
        } else if stHash != hash {
            update = append(update, pk)
        }
    }
    for pk := range standinData {
        if _, ok := mainData[pk]; !ok {
            del = append(del, pk)
        }
    }
    return
}

// applyChanges — выполняет вставку/обновление (через batch upsert) и удаление
// для списка PK. При этом columns — динамический список столбцов, pkCol — имя PK-столбца,
// rowsMain/rowsStandin содержат сырые данные ( []interface{} ), индексированные по pk.
func applyChanges(
    table, schema, pkCol string,
    columns []string,
    toInsert, toUpdate, toDelete []string,
    rowsMain, rowsStandin map[string][]interface{},
) error {
    ctx := context.Background()
    tx, err := standinDB.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // 1) Подготовим batch upsert: INSERT ... ON CONFLICT DO UPDATE
    upsertRows := make([][]interface{}, 0, len(toInsert)+len(toUpdate))
    for _, pk := range toInsert {
        upsertRows = append(upsertRows, rowsMain[pk])
    }
    for _, pk := range toUpdate {
        upsertRows = append(upsertRows, rowsMain[pk])
    }
    if len(upsertRows) > 0 {
        if err := doBatchUpsertTx(ctx, tx, schema, table, columns, []string{pkCol}, upsertRows); err != nil {
            return err
        }
    }

    // 2) Удаление (batch delete pk IN (...))
    if len(toDelete) > 0 {
        placeholders := make([]string, len(toDelete))
        args := make([]interface{}, len(toDelete))
        for i, pk := range toDelete {
            placeholders[i] = fmt.Sprintf("$%d", i+1)
            args[i] = pk
        }
        delSQL := fmt.Sprintf(
            `DELETE FROM "%s"."%s" WHERE "%s" IN (%s)`,
            schema, table, pkCol, strings.Join(placeholders, ","),
        )
        if _, err := tx.ExecContext(ctx, delSQL, args...); err != nil {
            log.Printf("[DEL] Ошибка DELETE pk IN(...): %v", err)
        }
    }

    if err := tx.Commit(); err != nil {
        return err
    }

    log.Printf("[applyChanges] %s: +%d / ~%d / -%d", table, len(toInsert), len(toUpdate), len(toDelete))

    return nil
}

// doBatchUpsertTx — формирует INSERT ... ON CONFLICT DO UPDATE, подставляя
// все столбцы columns, кроме pkCols, в секцию DO UPDATE SET.
func doBatchUpsertTx(
    ctx context.Context, tx *sql.Tx,
    schema, table string,
    columns, pkCols []string,
    rowValues [][]interface{},
) error {
    if len(rowValues) == 0 {
        return nil
    }

    colList := quoteColumns(columns)      // "col1","col2",...
    pkList := quoteColumns(pkCols)        // "pk"
    placeholders := makePlaceholderMatrix(len(rowValues), len(columns))

    var updateCols []string
    for _, c := range columns {
        if !inSlice(pkCols, c) {
            updateCols = append(updateCols, fmt.Sprintf(`"%s"=EXCLUDED."%s"`, c, c))
        }
    }
    upsert := fmt.Sprintf(`
INSERT INTO "%s"."%s" (%s)
VALUES %s
ON CONFLICT (%s)
DO UPDATE SET %s
`,
        schema, table, colList, placeholders, pkList, strings.Join(updateCols, ", "))

    args := flatten(rowValues)
    _, err := tx.ExecContext(ctx, upsert, args...)
    return err
}

// syncTableByUpdatedAt — заглушка для сценария, когда у каждой строки есть updated_at,
// и мы синхронизируем только те, что обновились с момента cfg.LastSyncTime.
func syncTableByUpdatedAt(cfg *Config, mainTx *sql.Tx, tableName string) error {
    log.Printf("[UpdatedAt] Синхронизация %s c updated_at > %v (заглушка)", tableName, cfg.LastSyncTime)
    return nil
}

// syncTableFullDiff — заглушка полного чтения (без чанков).
func syncTableFullDiff(cfg *Config, mainTx *sql.Tx, tableName string, pkCols []string) error {
    log.Printf("[FullDiff] Таблица %s: полный дифф. PKCols=%v", tableName, pkCols)
    return nil
}

// quoteColumns — оборачивает каждое имя столбца в кавычки "col" и склеивает запятыми.
func quoteColumns(cols []string) string {
    quoted := make([]string, len(cols))
    for i, c := range cols {
        quoted[i] = `"` + c + `"`
    }
    return strings.Join(quoted, ",")
}

// inSlice — проверяет, содержится ли строка s в срезе sl.
func inSlice(sl []string, s string) bool {
    for _, x := range sl {
        if x == s {
            return true
        }
    }
    return false
}

// flatten — превращает [][]interface{} (batch) в []interface{} для ExecContext.
func flatten(rowsData [][]interface{}) []interface{} {
    var out []interface{}
    for _, row := range rowsData {
        out = append(out, row...)
    }
    return out
}

// makePlaceholderMatrix(N,M) -> "($1,$2,...),($3,$4,...)"
func makePlaceholderMatrix(numRows, numCols int) string {
    var sb strings.Builder
    idx := 1
    for r := 0; r < numRows; r++ {
        sb.WriteString("(")
        for c := 0; c < numCols; c++ {
            sb.WriteString("$")
            sb.WriteString(strconv.Itoa(idx))
            if c < numCols-1 {
                sb.WriteString(",")
            }
            idx++
        }
        sb.WriteString(")")
        if r < numRows-1 {
            sb.WriteString(",")
        }
    }
    return sb.String()
}
