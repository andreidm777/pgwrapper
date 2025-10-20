# PGXWrapper - Документация

## Обзор

PGXWrapper - это драйвер для PostgreSQL, построенный на основе pgx, который поддерживает архитектуру master-synchronous slave-asynchronous slave. Драйвер предоставляет удобный интерфейс для работы с репликами, автоматическое переключение между ними при ошибках и поддержку транзакций только на мастере.

## Установка

```bash
go get github.com/your-username/pgxwrapper
```

## Основные типы и функции

### func New
```go
func New(ctx context.Context, config Config) (*DB, error)
```
Создает новый экземпляр драйвера.

Параметры:
- `ctx`: контекст выполнения
- `config`: конфигурация драйвера

Возвращает:
- `*DB`: основной драйвер
- `error`: ошибка при создании драйвера

Возможные ошибки:
- `ErrConnectionFailed`: если не удалось подключиться к базе данных

### type Config
```go
type Config struct {
    // MasterConnString строка подключения к мастеру
    MasterConnString string

    // SyncSlaveConnString строка подключения к синхронной реплике
    SyncSlaveConnString string

    // AsyncSlaveConnString строка подключения к асинхронной реплике
    AsyncSlaveConnString string

    // MaxRetries максимальное количество повторных попыток при ошибках
    MaxRetries int

    // RetryDelay задержка между повторными попытками
    RetryDelay time.Duration

    // QueryTimeout таймаут для выполнения запросов
    QueryTimeout time.Duration

    // EnableTelemetry включить телеметрию
    EnableTelemetry bool

    // DisableReplicaFallback отключить переключение между репликами
    DisableReplicaFallback bool

    // Logger логгер для драйвера
    Logger *slog.Logger
}
```
Конфигурация драйвера.

### type DB
```go
type DB struct {
    // ...
}
```
Основной драйвер. Не экспортируется напрямую - создается через функцию New.

#### func (*DB) Master
```go
func (db *DB) Master() Conn
```
Возвращает подключение к мастеру.

#### func (*DB) SyncSlave
```go
func (db *DB) SyncSlave() Conn
```
Возвращает подключение к синхронной реплике. Если синхронная реплика недоступна, возвращает подключение к мастеру.

#### func (*DB) Slave
```go
func (db *DB) Slave() Conn
```
Возвращает подключение к асинхронной реплике. Если асинхронная реплика недоступна, возвращает подключение к синхронной реплике или мастеру.

#### func (*DB) Begin
```go
func (db *DB) Begin(ctx context.Context) (Tx, error)
```
Начинает новую транзакцию на мастере с параметрами по умолчанию. Все транзакции всегда начинаются только на мастере.

Возможные ошибки:
- Возвращает ошибки, связанные с PostgreSQL, через обертку

#### func (*DB) BeginTx
```go
func (db *DB) BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error)
```
Начинает новую транзакцию на мастере с указанными параметрами. Все транзакции всегда начинаются только на мастере.

Возможные ошибки:
- Возвращает ошибки, связанные с PostgreSQL, через обертку

#### func (*DB) ExecuteInTransactionDefault
```go
func (db *DB) ExecuteInTransactionDefault(ctx context.Context, fn func(Tx) error) error
```
Выполняет функцию в транзакции с параметрами по умолчанию. Автоматически фиксирует транзакцию при успешном выполнении функции или откатывает при ошибке.

Параметры:
- `ctx`: контекст выполнения
- `fn`: функция, которая будет выполнена в транзакции

Возможные ошибки:
- Ошибки выполнения функции
- Ошибки фиксации или отката транзакции
- `ErrTransactionFailed`: ошибка выполнения транзакции

#### func (*DB) ExecuteInTransaction
```go
func (db *DB) ExecuteInTransaction(ctx context.Context, txOptions TxOptions, fn func(Tx) error) error
```
Выполняет функцию в транзакции с указанными параметрами. Автоматически фиксирует транзакцию при успешном выполнении функции или откатывает при ошибке.

Параметры:
- `ctx`: контекст выполнения
- `txOptions`: параметры транзакции
- `fn`: функция, которая будет выполнена в транзакции

Возможные ошибки:
- Ошибки выполнения функции
- Ошибки фиксации или отката транзакции
- `ErrTransactionFailed`: ошибка выполнения транзакции

#### func (*DB) Close
```go
func (db *DB) Close(ctx context.Context) error
```
Закрывает все подключения к базе данных (мастер и реплики).

Возможные ошибки:
- Ошибки закрытия отдельных подключений

### type Conn
```go
type Conn interface {
    Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
    Query(ctx context.Context, sql string, args ...any) (Rows, error)
    QueryRow(ctx context.Context, sql string, args ...any) Row
    Begin(ctx context.Context) (Tx, error)
    BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error)
    Ping(ctx context.Context) error
    Close(ctx context.Context) error
}
```
Интерфейс подключения к базе данных.

#### func (Conn) Exec
```go
func (conn Conn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
```
Выполняет SQL команду (INSERT, UPDATE, DELETE и т.д.) и возвращает информацию о выполнении.

Параметры:
- `ctx`: контекст выполнения
- `sql`: SQL запрос
- `arguments`: параметры запроса

Возвращает:
- `pgconn.CommandTag`: информация о выполнении команды
- `error`: ошибка при выполнении

Возможные ошибки:
- Ошибки выполнения SQL
- `ErrMasterOnlyOperation`: при попытке выполнить на реплике операцию, доступную только на мастере

#### func (Conn) Query
```go
func (conn Conn) Query(ctx context.Context, sql string, args ...any) (Rows, error)
```
Выполняет SQL запрос, возвращающий строки данных (SELECT и т.д.).

Параметры:
- `ctx`: контекст выполнения
- `sql`: SQL запрос
- `args`: параметры запроса

Возвращает:
- `Rows`: итератор по результатам запроса
- `error`: ошибка при выполнении

Возможные ошибки:
- Ошибки выполнения SQL

#### func (Conn) QueryRow
```go
func (conn Conn) QueryRow(ctx context.Context, sql string, args ...any) Row
```
Выполняет SQL запрос и возвращает одну строку результата.

Параметры:
- `ctx`: контекст выполнения
- `sql`: SQL запрос
- `args`: параметры запроса

Возвращает:
- `Row`: одну строку результата

#### func (Conn) Begin
```go
func (conn Conn) Begin(ctx context.Context) (Tx, error)
```
Начинает новую транзакцию.

Возможные ошибки:
- `ErrMasterOnlyOperation`: на реплике транзакции не поддерживаются
- Ошибки выполнения

#### func (Conn) Ping
```go
func (conn Conn) Ping(ctx context.Context) error
```
Проверяет соединение с базой данных.

Возможные ошибки:
- Ошибки соединения

### type Tx
```go
type Tx interface {
    Conn
    Commit(ctx context.Context) error
    Rollback(ctx context.Context) error
}
```
Интерфейс транзакции. Расширяет Conn, добавляя методы фиксации и отката транзакции.

#### func (Tx) Commit
```go
func (tx Tx) Commit(ctx context.Context) error
```
Фиксирует транзакцию.

Возможные ошибки:
- Ошибки фиксации транзакции

#### func (Tx) Rollback
```go
func (tx Tx) Rollback(ctx context.Context) error
```
Откатывает транзакцию.

Возможные ошибки:
- Ошибки отката транзакции

### type Rows
```go
type Rows interface {
    Close()
    Err() error
    Next() bool
    Scan(dest ...any) error
    Values() ([]any, error)
    ColumnTypes() []any
}
```
Интерфейс для результата запроса (множество строк).

#### func (Rows) Close
```go
func (rows Rows) Close()
```
Закрывает итератор результатов. Должен быть вызван после завершения работы с результатами.

#### func (Rows) Next
```go
func (rows Rows) Next() bool
```
Проверяет, есть ли следующая строка в результатах. Используется для итерации по результатам.

#### func (Rows) Scan
```go
func (rows Rows) Scan(dest ...any) error
```
Сканирует значения текущей строки в переменные.

Параметры:
- `dest`: переменные для сканирования значений

Возможные ошибки:
- Ошибки преобразования типов
- Ошибки сопоставления столбцов

### type Row
```go
type Row interface {
    Scan(dest ...any) error
}
```
Интерфейс для одной строки результата.

#### func (Row) Scan
```go
func (row Row) Scan(dest ...any) error
```
Сканирует значения строки в переменные.

Параметры:
- `dest`: переменные для сканирования значений

Возможные ошибки:
- Ошибки преобразования типов
- Ошибки сопоставления столбцов

## Телеметрия

### type Telemetry
```go
type Telemetry struct {
    // ...
}
```
Структура для сбора метрик использования драйвера.

#### func NewTelemetry
```go
func NewTelemetry() *Telemetry
```
Создает новый экземпляр телеметрии с включением по умолчанию и стандартным логгером.

#### func NewTelemetryWithLogger
```go
func NewTelemetryWithLogger(logger *slog.Logger) *Telemetry
```
Создает новый экземпляр телеметрии с указанным логгером.

#### func (*Telemetry) GetMetrics
```go
func (t *Telemetry) GetMetrics() map[string]any
```
Возвращает текущие метрики.

Возвращаемые метрики:
- `total_queries` - общее количество запросов
- `total_errors` - общее количество ошибок
- `total_retries` - общее количество повторов
- `average_duration` - средняя продолжительность запросов
- `connection_errors` - количество ошибок подключения
- `enabled` - включена ли телеметрия

## Типы реплик

### type ReplicaType
```go
type ReplicaType int
```
Тип реплики.

Константы:
- `AsyncReplica` - асинхронная реплика
- `SyncReplica` - синхронная реплика

## Специфичные ошибки

- `ErrNoAvailableReplicas`: Нет доступных реплик
- `ErrMasterOnlyOperation`: Операция может быть выполнена только на мастере
- `ErrReplicaTimeout`: Таймаут при работе с репликой
- `ErrConnectionFailed`: Ошибка подключения к базе данных
- `ErrTransactionFailed`: Ошибка выполнения транзакции
- `ErrMaxRetriesExceeded`: Превышено максимальное количество попыток
- `ErrInvalidConfiguration`: Невалидная конфигурация драйвера
- `ErrReplicaNotReady`: Реплика не готова к приему запросов
- `ErrQueryTimeout`: Таймаут выполнения запроса

## Примеры использования

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
    
    db, err := pgxwrapper.New(ctx.Background(), config)
    if err != nil {
        log.Fatal("Ошибка создания драйвера:", err)
    }
    defer db.Close(context.Background())
    
    // Работа с мастером
    master := db.Master()
    
    // Работа с синхронной репликой
    syncSlave := db.SyncSlave()
    
    // Работа с асинхронной репликой
    asyncSlave := db.Slave()
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
tx, err := db.Begin(context.Background())
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
err = db.ExecuteInTransactionDefault(context.Background(), func(tx pgxwrapper.Tx) error {
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