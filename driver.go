package pgxwrapper

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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

// Driver основной драйвер
type Driver struct {
	master     *pgx.Conn
	syncSlave  *pgx.Conn
	asyncSlave *pgx.Conn

	config Config

	// connections мьютекс для управления подключениями
	connections sync.RWMutex

	// telemetry телеметрия
	telemetry *Telemetry

	// replicaFallback отключить переключение между репликами
	replicaFallback bool

	// logger логгер
	logger *slog.Logger
}

// NewDriver создает новый экземпляр драйвера
func NewDriver(ctx context.Context, config Config) (*Driver, error) {
	driver := &Driver{
		config: config,
	}

	// Инициализируем логгер
	if config.Logger != nil {
		driver.logger = config.Logger
	} else {
		driver.logger = slog.Default()
	}

	// Инициализируем телеметрию
	if config.EnableTelemetry {
		driver.telemetry = NewTelemetry()
	}

	// Подключаемся к мастеру
	var err error
	driver.master, err = pgx.Connect(ctx, config.MasterConnString)
	if err != nil {
		return nil, fmt.Errorf("%w: ошибка подключения к мастеру: %v", ErrConnectionFailed, err)
	}

	// Подключаемся к синхронной реплике
	if config.SyncSlaveConnString != "" {
		driver.syncSlave, err = pgx.Connect(ctx, config.SyncSlaveConnString)
		if err != nil {
			return nil, fmt.Errorf("%w: ошибка подключения к синхронной реплике: %v", ErrConnectionFailed, err)
		}
	}

	// Подключаемся к асинхронной реплике
	if config.AsyncSlaveConnString != "" {
		driver.asyncSlave, err = pgx.Connect(ctx, config.AsyncSlaveConnString)
		if err != nil {
			return nil, fmt.Errorf("%w: ошибка подключения к асинхронной реплике: %v", ErrConnectionFailed, err)
		}
	}

	// Устанавливаем флаг переключения между репликами
	driver.replicaFallback = !config.DisableReplicaFallback

	return driver, nil
}

// Master возвращает подключение к мастеру
func (d *Driver) Master() Conn {
	return &masterConn{conn: d.master, driver: d}
}

// SyncSlave возвращает подключение к синхронной реплике
func (d *Driver) SyncSlave() Conn {
	if d.syncSlave == nil {
		d.logger.Info("SyncSlave недоступен")
		return nil
	}
	conn := &replicaConn{conn: d.syncSlave, driver: d, replicaType: SyncReplica}
	// Оборачиваем в ReplicaManager для поддержки повторных попыток и переключения
	rm := NewReplicaManager(d)
	return &retryableConn{conn: conn, manager: rm}
}

// Slave возвращает подключение к асинхронной реплике
func (d *Driver) Slave() Conn {
	if d.asyncSlave == nil {
		d.logger.Info("Slave недоступен")
		return nil
	}
	conn := &replicaConn{conn: d.asyncSlave, driver: d, replicaType: AsyncReplica}
	// Оборачиваем в ReplicaManager для поддержки повторных попыток и переключения
	rm := NewReplicaManager(d)
	return &retryableConn{conn: conn, manager: rm}
}

// Close закрывает все подключения
func (d *Driver) Close(ctx context.Context) error {
	var errs []error

	if d.master != nil {
		if err := d.master.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("ошибка закрытия мастер-подключения: %w", err))
		}
	}

	if d.syncSlave != nil {
		if err := d.syncSlave.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("ошибка закрытия синхронной реплики: %w", err))
		}
	}

	if d.asyncSlave != nil {
		if err := d.asyncSlave.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("ошибка закрытия асинхронной реплики: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("ошибки при закрытии подключений: %v", errs)
	}

	return nil
}

// ReplicaType тип реплики
type ReplicaType int

const (
	// AsyncReplica асинхронная реплика
	AsyncReplica ReplicaType = iota

	// SyncReplica синхронная реплика
	SyncReplica
)

// Удаляем дублирующиеся объявления структур

// replicaSelector определяет порядок переключения между репликами
type replicaSelector struct {
	current ReplicaType
	driver  *Driver
}

// newReplicaSelector создает новый селектор реплик
func newReplicaSelector(driver *Driver) *replicaSelector {
	return &replicaSelector{
		current: AsyncReplica,
		driver:  driver,
	}
}

// getNextReplica возвращает следующую реплику в порядке: асинхронная -> синхронная -> мастер
func (rs *replicaSelector) getNextReplica() (Conn, ReplicaType, error) {
	if !rs.driver.replicaFallback {
		// Если отключено переключение между репликами, возвращаем ошибку
		return nil, -1, ErrNoAvailableReplicas
	}

	switch rs.current {
	case AsyncReplica:
		rs.current = SyncReplica
		if rs.driver.syncSlave != nil {
			return &replicaConn{conn: rs.driver.syncSlave, driver: rs.driver, replicaType: SyncReplica}, SyncReplica, nil
		}
		fallthrough
	case SyncReplica:
		rs.current = -1 // означает мастер
		return &masterConn{conn: rs.driver.master, driver: rs.driver}, -1, nil
	default:
		return nil, -1, ErrNoAvailableReplicas
	}
}

// executeWithRetry выполняет операцию с повторными попытками
func executeWithRetry(ctx context.Context, driver *Driver, operation func(Conn) error) error {
	rm := NewReplicaManager(driver)
	return rm.ExecuteQueryWithRetry(ctx, operation)
}
