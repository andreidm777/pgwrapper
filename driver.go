package pgxwrapper

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// New создает новый экземпляр драйвера
func New(ctx context.Context, config Config) (*DB, error) {
	db := &DB{
		config: config,
	}

	// Инициализируем логгер
	if config.Logger != nil {
		db.logger = config.Logger
	} else {
		db.logger = slog.Default()
	}

	// Инициализируем телеметрию
	if config.EnableTelemetry {
		db.telemetry = NewTelemetry()
	}

	// Подключаемся к мастеру
	var err error
	db.master, err = pgx.Connect(ctx, config.MasterConnString)
	if err != nil {
		return nil, fmt.Errorf("%w: master connection error: %v", ErrConnectionFailed, err)
	}

	// Подключаемся к синхронной реплике
	if config.SyncSlaveConnString != "" {
		db.syncSlave, err = pgx.Connect(ctx, config.SyncSlaveConnString)
		if err != nil {
			return nil, fmt.Errorf("%w: synchronous replica connection error: %v", ErrConnectionFailed, err)
		}
	}

	// Подключаемся к асинхронной реплике
	if config.AsyncSlaveConnString != "" {
		db.asyncSlave, err = pgx.Connect(ctx, config.AsyncSlaveConnString)
		if err != nil {
			return nil, fmt.Errorf("%w: asynchronous replica connection error: %v", ErrConnectionFailed, err)
		}
	}

	// Устанавливаем флаг переключения между репликами
	db.replicaFallback = !config.DisableReplicaFallback

	return db, nil
}

// Master возвращает подключение к мастеру
func (db *DB) Master() Conn {
	return &masterConn{conn: db.master, db: db}
}

// SyncSlave возвращает подключение к синхронной реплике
func (db *DB) SyncSlave() Conn {
	if db.syncSlave == nil {
		db.logger.Info("SyncSlave недоступен")
		return db.Master()
	}
	conn := &replicaConn{masterConn{conn: db.syncSlave, db: db}, SyncReplica}
	// Оборачиваем в ReplicaManager для поддержки повторных попыток и переключения
	rm := NewReplicaManager(db)
	return &retryableConn{conn: conn, manager: rm}
}

// Slave возвращает подключение к асинхронной реплике
func (db *DB) Slave() Conn {
	if db.asyncSlave == nil {
		db.logger.Info("Slave недоступен")
		return db.SyncSlave()
	}
	conn := &replicaConn{masterConn{conn: db.asyncSlave, db: db}, AsyncReplica}
	// Оборачиваем в ReplicaManager для поддержки повторных попыток и переключения
	rm := NewReplicaManager(db)
	return &retryableConn{conn: conn, manager: rm}
}

// Close закрывает все подключения
func (db *DB) Close(ctx context.Context) error {
	var errs []error

	if db.master != nil {
		if err := db.master.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("master connection close error: %w", err))
		}
	}

	if db.syncSlave != nil {
		if err := db.syncSlave.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("synchronous replica close error: %w", err))
		}
	}

	if db.asyncSlave != nil {
		if err := db.asyncSlave.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("asynchronous replica close error: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs)
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
