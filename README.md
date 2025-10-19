# PGXWrapper

PGXWrapper - это драйвер для PostgreSQL, построенный на основе pgx, который поддерживает архитектуру master-synchronous slave-asynchronous slave. Драйвер предоставляет удобный интерфейс для работы с репликами, автоматическое переключение между ними при ошибках и поддержку транзакций только на мастере.

## Особенности

- Поддержка архитектуры master-synchronous slave-asynchronous slave
- Автоматическое переключение между репликами при ошибках (асинхронная -> синхронная -> мастер)
- Поддержка транзакций только на мастере
- Повторные попытки запросов при сетевых ошибках или таймаутах
- Интеграция с телеметрией для сбора метрик
- Поддержка логирования через slog
- Настраиваемые параметры (число повторов, таймауты, включение/отключение телеметрии)
- Специфичные ошибки для обработки пользователем

## Установка

```bash
go mod init your-project
go get github.com/your-username/pgxwrapper
```

## Использование

### Базовое использование

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/your-username/pgxwrapper"
)

func main() {
    config := pgxwrapper.Config{
        MasterConnString:      "postgres://user:password@master:5432/dbname",
        SyncSlaveConnString:   "postgres://user:password@sync_slave:5432/dbname",
        AsyncSlaveConnString:  "postgres://user:password@async_slave:5432/dbname",
        MaxRetries:            3,
        RetryDelay:            time.Second,
        QueryTimeout:          30 * time.Second,
        EnableTelemetry:       true,
        DisableReplicaFallback: false,
    }
    
    driver, err := pgxwrapper.NewDriver(config)
    if err != nil {
        log.Fatal("Ошибка создания драйвера:", err)
    }
    defer driver.Close(context.Background())
    
    // Работа с мастером
    master := driver.Master()
    
    // Работа с синхронной репликой
    syncSlave := driver.SyncSlave()
    
    // Работа с асинхронной репликой
    asyncSlave := driver.Slave()
}
```

### Выполнение запросов

```go
// Выполнение запроса на мастере
result, err := master.Exec(context.Background(), "INSERT INTO users (name, email) VALUES ($1, $2)", "Иван", "ivan@example.com")
if err != nil {
    log.Printf("Ошибка выполнения запроса: %v", err)
}

// Выполнение запроса на реплике (только чтение)
rows, err := asyncSlave.Query(context.Background(), "SELECT id, name, email FROM users WHERE age > $1", 18)
if err != nil {
    log.Printf("Ошибка выполнения запроса: %v", err)
} else {
    defer rows.Close()
    for rows.Next() {
        var id int
        var name, email string
        err := rows.Scan(&id, &name, &email)
        if err != nil {
            log.Printf("Ошибка сканирования строки: %v", err)
            continue
        }
        log.Printf("Пользователь: %d, %s, %s", id, name, email)
    }
}
```

### Работа с транзакциями

```go
// Транзакции всегда выполняются только на мастере
tx, err := driver.Begin(context.Background())
if err != nil {
    log.Printf("Ошибка начала транзакции: %v", err)
    return
}
defer tx.Rollback(context.Background()) // Откатываем, если транзакция не завершена

// Выполняем операции в транзакции
_, err = tx.Exec(context.Background(), "UPDATE accounts SET balance = balance - 100 WHERE user_id = $1", 1)
if err != nil {
    log.Printf("Ошибка выполнения операции в транзакции: %v", err)
    return
}

_, err = tx.Exec(context.Background(), "UPDATE accounts SET balance = balance + 100 WHERE user_id = $1", 2)
if err != nil {
    log.Printf("Ошибка выполнения операции в транзакции: %v", err)
    return
}

// Фиксируем транзакцию
err = tx.Commit(context.Background())
if err != nil {
    log.Printf("Ошибка фиксации транзакции: %v", err)
    return
}
```

### Выполнение функции в транзакции

```go
// Упрощенное выполнение функции в транзакции
err = driver.ExecuteInTransactionDefault(context.Background(), func(tx pgxwrapper.Tx) error {
    _, err := tx.Exec(context.Background(), "UPDATE accounts SET balance = balance - 100 WHERE user_id = $1", 1)
    if err != nil {
        return err
    }
    
    _, err = tx.Exec(context.Background(), "UPDATE accounts SET balance = balance + 100 WHERE user_id = $1", 2)
    if err != nil {
        return err
    }
    
    return nil
})

if err != nil {
    log.Printf("Ошибка выполнения транзакции: %v", err)
}
```

## Конфигурация

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| MasterConnString | Строка подключения к мастеру | "" |
| SyncSlaveConnString | Строка подключения к синхронной реплике | "" |
| AsyncSlaveConnString | Строка подключения к асинхронной реплике | "" |
| MaxRetries | Максимальное количество повторных попыток при ошибках | 0 |
| RetryDelay | Задержка между повторными попытками | 0 |
| QueryTimeout | Таймаут для выполнения запросов | 0 |
| EnableTelemetry | Включить телеметрию | false |
| DisableReplicaFallback | Отключить переключение между репликами | false |
| Logger | Логгер для драйвера | slog.Default() |

## Специфичные ошибки

- `ErrNoAvailableReplicas` - Нет доступных реплик
- `ErrMasterOnlyOperation` - Операция может быть выполнена только на мастере
- `ErrReplicaTimeout` - Таймаут при работе с репликой
- `ErrConnectionFailed` - Ошибка подключения к базе данных
- `ErrTransactionFailed` - Ошибка выполнения транзакции
- `ErrMaxRetriesExceeded` - Превышено максимальное количество попыток
- `ErrInvalidConfiguration` - Невалидная конфигурация драйвера
- `ErrReplicaNotReady` - Реплика не готова к приему запросов
- `ErrQueryTimeout` - Таймаут выполнения запроса

## Тестирование

Для запуска unit-тестов:

```bash
go test ./...
```

Для запуска интеграционных тестов с Docker:

```bash
docker-compose up -d
go test -run Integration ./...
```

## Docker Compose

Для запуска окружения с PostgreSQL в архитектуре master-synchronous slave-asynchronous slave:

```bash
docker-compose up -d
```

## Телеметрия

Для получения метрик:

```go
metrics := driver.Telemetry.GetMetrics()
log.Printf("Метрики: %+v", metrics)
```

## Логирование

Драйвер использует `slog` для логирования. Вы можете передать свой логгер в конфигурации:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
config := pgxwrapper.Config{
    // ... другие параметры
    Logger: logger,
}
```

## Авторы

- Дмитрий Евгеньевич

## Лицензия

MIT