package pgxwrapper

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Config конфигурация драйвера
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

// Conn интерфейс подключения к базе данных
type Conn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Begin(ctx context.Context) (Tx, error)
	BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error)
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}

// Rows интерфейс для результата запроса
type Rows interface {
	Close()
	Err() error
	Next() bool
	Scan(dest ...any) error
	Values() ([]any, error)
	ColumnTypes() []any
}

// Row интерфейс для одной строки результата
type Row interface {
	Scan(dest ...any) error
}

// TxOptions параметры транзакции
type TxOptions struct {
	// TODO: определить параметры транзакции
	pgx.TxOptions
}

// Tx интерфейс транзакции
type Tx interface {
	Conn
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// DB основной драйвер
type DB struct {
	master     *pgx.Conn
	syncSlave  *pgx.Conn
	asyncSlave *pgx.Conn

	config Config

	// telemetry телеметрия
	telemetry *Telemetry

	// replicaFallback отключить переключение между репликами
	replicaFallback bool

	// logger логгер
	logger *slog.Logger
}
