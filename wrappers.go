package pgxwrapper

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// rowsWrapper обертка для Rows
type rowsWrapper struct {
	rows pgx.Rows
}

// Close закрывает Rows
func (r *rowsWrapper) Close() {
	r.rows.Close()
}

// Err возвращает ошибку
func (r *rowsWrapper) Err() error {
	return r.rows.Err()
}

// Next проверяет, есть ли следующая строка
func (r *rowsWrapper) Next() bool {
	return r.rows.Next()
}

// Scan сканирует значения в переменные
func (r *rowsWrapper) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

// Values возвращает значения текущей строки
func (r *rowsWrapper) Values() ([]any, error) {
	return r.rows.Values()
}

// ColumnTypes возвращает типы колонок
func (r *rowsWrapper) ColumnTypes() []any {
	descs := r.rows.FieldDescriptions()
	result := make([]any, len(descs))
	for i, desc := range descs {
		result[i] = desc
	}
	return result
}

// rowWrapper обертка для Row
type rowWrapper struct {
	row pgx.Row
}

// Scan сканирует значения в переменные
func (r *rowWrapper) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}

// txWrapper обертка для транзакции
type txWrapper struct {
	tx pgx.Tx
	db *DB
}

// Exec выполняет SQL команду в транзакции
func (t *txWrapper) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if t.db.telemetry != nil && t.db.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			t.db.telemetry.RecordQuery(duration)
		}()
	}

	result, err := t.tx.Exec(ctx, sql, arguments...)
	if err != nil {
		if t.db.telemetry != nil {
			t.db.telemetry.RecordError()
		}
		return result, fmt.Errorf("error executing query in transaction: %w", err)
	}

	return result, nil
}

// Query выполняет SQL запрос в транзакции
func (t *txWrapper) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	if t.db.telemetry != nil && t.db.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			t.db.telemetry.RecordQuery(duration)
		}()
	}

	rows, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		if t.db.telemetry != nil {
			t.db.telemetry.RecordError()
		}
		return nil, fmt.Errorf("ошибка выполнения запроса в транзакции: %w", err)
	}

	return &rowsWrapper{rows: rows}, nil
}

// QueryRow выполняет SQL запрос и возвращает одну строку в транзакции
func (t *txWrapper) QueryRow(ctx context.Context, sql string, args ...any) Row {
	if t.db.telemetry != nil && t.db.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			t.db.telemetry.RecordQuery(duration)
		}()
	}

	row := t.tx.QueryRow(ctx, sql, args...)
	return &rowWrapper{row: row}
}

// Begin не поддерживается в транзакции
func (t *txWrapper) Begin(ctx context.Context) (Tx, error) {
	return nil, errors.New("nested transactions are not supported")
}

// BeginTx не поддерживается в транзакции
func (t *txWrapper) BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error) {
	return nil, errors.New("nested transactions are not supported")
}

// Ping не поддерживается в транзакции
func (t *txWrapper) Ping(ctx context.Context) error {
	return errors.New("ping is not supported in transaction")
}

// Close закрывает транзакцию (на самом деле нет, т.к. это транзакция)
func (t *txWrapper) Close(ctx context.Context) error {
	return nil // Не закрываем транзакцию при вызове Close, только через Commit или Rollback
}

// Commit фиксирует транзакцию
func (t *txWrapper) Commit(ctx context.Context) error {
	err := t.tx.Commit(ctx)
	if err != nil {
		if t.db.telemetry != nil {
			t.db.telemetry.RecordError()
		}
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

// Rollback откатывает транзакцию
func (t *txWrapper) Rollback(ctx context.Context) error {
	err := t.tx.Rollback(ctx)
	if err != nil {
		if t.db.telemetry != nil {
			t.db.telemetry.RecordError()
		}
		return fmt.Errorf("error rolling back transaction: %w", err)
	}

	return nil
}
