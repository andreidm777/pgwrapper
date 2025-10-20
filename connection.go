package pgxwrapper

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// masterConn структура для подключения к мастеру
type masterConn struct {
	conn *pgx.Conn
	db   *DB
}

// Exec выполняет SQL команду на мастере
func (mc *masterConn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	// Применяем таймаут из конфигурации, если он задан
	if mc.db.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, mc.db.config.QueryTimeout)
		defer cancel()
	}

	if mc.db.telemetry != nil && mc.db.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			mc.db.telemetry.RecordQuery(duration)
		}()
	}

	result, err := mc.conn.Exec(ctx, sql, arguments...)
	if err != nil {
		if mc.db.telemetry != nil {
			mc.db.telemetry.RecordError()
		}
		mc.db.logger.ErrorContext(ctx, "Ошибка выполнения Exec на мастере", "error", err, "sql", sql)
		return result, fmt.Errorf("error executing query on master: %w", err)
	}

	mc.db.logger.DebugContext(ctx, "Выполнен Exec на мастере", "sql", sql)
	return result, nil
}

// Query выполняет SQL запрос на мастере
func (mc *masterConn) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	// Применяем таймаут из конфигурации, если он задан
	if mc.db.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, mc.db.config.QueryTimeout)
		defer cancel()
	}

	if mc.db.telemetry != nil && mc.db.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			mc.db.telemetry.RecordQuery(duration)
		}()
	}

	rows, err := mc.conn.Query(ctx, sql, args...)
	if err != nil {
		if mc.db.telemetry != nil {
			mc.db.telemetry.RecordError()
		}
		mc.db.logger.ErrorContext(ctx, "Ошибка выполнения Query на мастере", "error", err, "sql", sql)
		return nil, fmt.Errorf("error executing query on master: %w", err)
	}

	mc.db.logger.DebugContext(ctx, "Выполнен Query на мастере", "sql", sql)
	return &rowsWrapper{rows: rows}, nil
}

// QueryRow выполняет SQL запрос и возвращает одну строку на мастере
func (mc *masterConn) QueryRow(ctx context.Context, sql string, args ...any) Row {
	// Применяем таймаут из конфигурации, если он задан
	if mc.db.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, mc.db.config.QueryTimeout)
		defer cancel()
	}

	if mc.db.telemetry != nil && mc.db.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			mc.db.telemetry.RecordQuery(duration)
		}()
	}

	row := mc.conn.QueryRow(ctx, sql, args...)
	return &rowWrapper{row: row}
}

// Begin начинает транзакцию на мастере
func (mc *masterConn) Begin(ctx context.Context) (Tx, error) {
	// Применяем таймаут из конфигурации, если он задан
	if mc.db.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, mc.db.config.QueryTimeout)
		defer cancel()
	}

	if mc.db.telemetry != nil && mc.db.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			mc.db.telemetry.RecordQuery(duration)
		}()
	}

	tx, err := mc.conn.Begin(ctx)
	if err != nil {
		if mc.db.telemetry != nil {
			mc.db.telemetry.RecordError()
		}
		return nil, fmt.Errorf("error starting transaction on master: %w", err)
	}

	return &txWrapper{
		tx: tx,
		db: mc.db,
	}, nil
}

// BeginTx начинает транзакцию с опциями на мастере
func (mc *masterConn) BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error) {
	// Применяем таймаут из конфигурации, если он задан
	if mc.db.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, mc.db.config.QueryTimeout)
		defer cancel()
	}

	if mc.db.telemetry != nil && mc.db.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			mc.db.telemetry.RecordQuery(duration)
		}()
	}

	tx, err := mc.conn.BeginTx(ctx, txOptions.TxOptions)
	if err != nil {
		if mc.db.telemetry != nil {
			mc.db.telemetry.RecordError()
		}
		return nil, fmt.Errorf("error starting transaction on master: %w", err)
	}

	return &txWrapper{
		tx: tx,
		db: mc.db,
	}, nil
}

// Ping проверяет соединение с мастером
func (mc *masterConn) Ping(ctx context.Context) error {
	// Применяем таймаут из конфигурации, если он задан
	if mc.db.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, mc.db.config.QueryTimeout)
		defer cancel()
	}

	err := mc.conn.Ping(ctx)
	if err != nil {
		if mc.db.telemetry != nil {
			mc.db.telemetry.RecordConnectionError()
		}
		return fmt.Errorf("error pinging master: %w", err)
	}

	return nil
}

// Close закрывает соединение с мастером
func (mc *masterConn) Close(ctx context.Context) error {
	return mc.conn.Close(ctx)
}

// replicaConn структура для подключения к реплике
type replicaConn struct {
	masterConn
	replicaType ReplicaType
}

// Exec выполняет SQL команду на реплике (только для мастера)
func (rc *replicaConn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, ErrMasterOnlyOperation
}

// QueryRow выполняет SQL запрос и возвращает одну строку на реплике
func (rc *replicaConn) QueryRow(ctx context.Context, sql string, args ...any) Row {
	// Применяем таймаут из конфигурации, если он задан
	if rc.db.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, rc.db.config.QueryTimeout)
		defer cancel()
	}

	if rc.db.telemetry != nil && rc.db.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			rc.db.telemetry.RecordQuery(duration)
		}()
	}

	row := rc.conn.QueryRow(ctx, sql, args...)
	return &rowWrapper{row: row}
}

// Begin начинает транзакцию на реплике (не поддерживается)
func (rc *replicaConn) Begin(ctx context.Context) (Tx, error) {
	if rc.db.telemetry != nil {
		rc.db.telemetry.RecordError()
	}
	return nil, ErrMasterOnlyOperation
}

// BeginTx начинает транзакцию с опциями на реплике (не поддерживается)
func (rc *replicaConn) BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error) {
	if rc.db.telemetry != nil {
		rc.db.telemetry.RecordError()
	}
	return nil, ErrMasterOnlyOperation
}
