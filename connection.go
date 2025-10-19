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
	conn   *pgx.Conn
	driver *Driver
}

// Exec выполняет SQL команду на мастере
func (mc *masterConn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	// Применяем таймаут из конфигурации, если он задан
	if mc.driver.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, mc.driver.config.QueryTimeout)
		defer cancel()
	}

	if mc.driver.telemetry != nil && mc.driver.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			mc.driver.telemetry.RecordQuery(duration)
		}()
	}

	result, err := mc.conn.Exec(ctx, sql, arguments...)
	if err != nil {
		if mc.driver.telemetry != nil {
			mc.driver.telemetry.RecordError()
		}
		mc.driver.logger.ErrorContext(ctx, "Ошибка выполнения Exec на мастере", "error", err, "sql", sql)
		return result, fmt.Errorf("ошибка выполнения запроса на мастере: %w", err)
	}

	mc.driver.logger.DebugContext(ctx, "Выполнен Exec на мастере", "sql", sql)
	return result, nil
}

// Query выполняет SQL запрос на мастере
func (mc *masterConn) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	// Применяем таймаут из конфигурации, если он задан
	if mc.driver.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, mc.driver.config.QueryTimeout)
		defer cancel()
	}

	if mc.driver.telemetry != nil && mc.driver.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			mc.driver.telemetry.RecordQuery(duration)
		}()
	}

	rows, err := mc.conn.Query(ctx, sql, args...)
	if err != nil {
		if mc.driver.telemetry != nil {
			mc.driver.telemetry.RecordError()
		}
		mc.driver.logger.ErrorContext(ctx, "Ошибка выполнения Query на мастере", "error", err, "sql", sql)
		return nil, fmt.Errorf("ошибка выполнения запроса на мастере: %w", err)
	}

	mc.driver.logger.DebugContext(ctx, "Выполнен Query на мастере", "sql", sql)
	return &rowsWrapper{rows: rows}, nil
}

// QueryRow выполняет SQL запрос и возвращает одну строку на мастере
func (mc *masterConn) QueryRow(ctx context.Context, sql string, args ...any) Row {
	// Применяем таймаут из конфигурации, если он задан
	if mc.driver.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, mc.driver.config.QueryTimeout)
		defer cancel()
	}

	if mc.driver.telemetry != nil && mc.driver.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			mc.driver.telemetry.RecordQuery(duration)
		}()
	}

	row := mc.conn.QueryRow(ctx, sql, args...)
	return &rowWrapper{row: row}
}

// Begin начинает транзакцию на мастере
func (mc *masterConn) Begin(ctx context.Context) (Tx, error) {
	// Применяем таймаут из конфигурации, если он задан
	if mc.driver.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, mc.driver.config.QueryTimeout)
		defer cancel()
	}

	if mc.driver.telemetry != nil && mc.driver.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			mc.driver.telemetry.RecordQuery(duration)
		}()
	}

	tx, err := mc.conn.Begin(ctx)
	if err != nil {
		if mc.driver.telemetry != nil {
			mc.driver.telemetry.RecordError()
		}
		return nil, fmt.Errorf("ошибка начала транзакции на мастере: %w", err)
	}

	return &txWrapper{
		tx:     tx,
		driver: mc.driver,
	}, nil
}

// BeginTx начинает транзакцию с опциями на мастере
func (mc *masterConn) BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error) {
	// Применяем таймаут из конфигурации, если он задан
	if mc.driver.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, mc.driver.config.QueryTimeout)
		defer cancel()
	}

	if mc.driver.telemetry != nil && mc.driver.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			mc.driver.telemetry.RecordQuery(duration)
		}()
	}

	tx, err := mc.conn.BeginTx(ctx, txOptions.TxOptions)
	if err != nil {
		if mc.driver.telemetry != nil {
			mc.driver.telemetry.RecordError()
		}
		return nil, fmt.Errorf("ошибка начала транзакции на мастере: %w", err)
	}

	return &txWrapper{
		tx:     tx,
		driver: mc.driver,
	}, nil
}

// Ping проверяет соединение с мастером
func (mc *masterConn) Ping(ctx context.Context) error {
	// Применяем таймаут из конфигурации, если он задан
	if mc.driver.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, mc.driver.config.QueryTimeout)
		defer cancel()
	}

	err := mc.conn.Ping(ctx)
	if err != nil {
		if mc.driver.telemetry != nil {
			mc.driver.telemetry.RecordConnectionError()
		}
		return fmt.Errorf("ошибка пинга мастера: %w", err)
	}

	return nil
}

// Close закрывает соединение с мастером
func (mc *masterConn) Close(ctx context.Context) error {
	return mc.conn.Close(ctx)
}

// replicaConn структура для подключения к реплике
type replicaConn struct {
	conn        *pgx.Conn
	driver      *Driver
	replicaType ReplicaType
}

// Exec выполняет SQL команду на реплике (только для мастера)
func (rc *replicaConn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, ErrMasterOnlyOperation
}

// Query выполняет SQL запрос на реплике
func (rc *replicaConn) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	// Применяем таймаут из конфигурации, если он задан
	if rc.driver.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, rc.driver.config.QueryTimeout)
		defer cancel()
	}

	if rc.driver.telemetry != nil && rc.driver.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			rc.driver.telemetry.RecordQuery(duration)
		}()
	}

	rows, err := rc.conn.Query(ctx, sql, args...)
	if err != nil {
		if rc.driver.telemetry != nil {
			rc.driver.telemetry.RecordError()
		}
		rc.driver.logger.ErrorContext(ctx, "Ошибка выполнения Query на реплике", "error", err, "sql", sql, "replica_type", rc.replicaType)
		return nil, fmt.Errorf("ошибка выполнения запроса на реплике: %w", err)
	}

	rc.driver.logger.DebugContext(ctx, "Выполнен Query на реплике", "sql", sql, "replica_type", rc.replicaType)
	return &rowsWrapper{rows: rows}, nil
}

// QueryRow выполняет SQL запрос и возвращает одну строку на реплике
func (rc *replicaConn) QueryRow(ctx context.Context, sql string, args ...any) Row {
	// Применяем таймаут из конфигурации, если он задан
	if rc.driver.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, rc.driver.config.QueryTimeout)
		defer cancel()
	}

	if rc.driver.telemetry != nil && rc.driver.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			rc.driver.telemetry.RecordQuery(duration)
		}()
	}

	row := rc.conn.QueryRow(ctx, sql, args...)
	return &rowWrapper{row: row}
}

// Begin начинает транзакцию на реплике (не поддерживается)
func (rc *replicaConn) Begin(ctx context.Context) (Tx, error) {
	if rc.driver.telemetry != nil {
		rc.driver.telemetry.RecordError()
	}
	return nil, ErrMasterOnlyOperation
}

// BeginTx начинает транзакцию с опциями на реплике (не поддерживается)
func (rc *replicaConn) BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error) {
	if rc.driver.telemetry != nil {
		rc.driver.telemetry.RecordError()
	}
	return nil, ErrMasterOnlyOperation
}

// Ping проверяет соединение с репликой
func (rc *replicaConn) Ping(ctx context.Context) error {
	// Применяем таймаут из конфигурации, если он задан
	if rc.driver.config.QueryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, rc.driver.config.QueryTimeout)
		defer cancel()
	}

	err := rc.conn.Ping(ctx)
	if err != nil {
		if rc.driver.telemetry != nil {
			rc.driver.telemetry.RecordConnectionError()
		}
		return fmt.Errorf("ошибка пинга реплики: %w", err)
	}

	return nil
}

// Close закрывает соединение с репликой
func (rc *replicaConn) Close(ctx context.Context) error {
	return rc.conn.Close(ctx)
}
