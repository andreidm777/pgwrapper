package pgxwrapper

import (
	"context"
	"fmt"
	"time"
)

// BeginTx начинает новую транзакцию на мастере
func (db *DB) BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error) {
	if db.telemetry != nil && db.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			db.telemetry.RecordQuery(duration)
		}()
	}

	// Все транзакции начинаются только на мастере
	tx, err := db.master.BeginTx(ctx, txOptions.TxOptions)
	if err != nil {
		if db.telemetry != nil {
			db.telemetry.RecordError()
		}
		return nil, fmt.Errorf("transaction begin error on master: %w", err)
	}

	return &txWrapper{
		tx: tx,
		db: db,
	}, nil
}

// Begin начинает новую транзакцию на мастере с параметрами по умолчанию
func (db *DB) Begin(ctx context.Context) (Tx, error) {
	if db.telemetry != nil && db.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			db.telemetry.RecordQuery(duration)
		}()
	}

	// Все транзакции начинаются только на мастере
	tx, err := db.master.Begin(ctx)
	if err != nil {
		if db.telemetry != nil {
			db.telemetry.RecordError()
		}
		return nil, fmt.Errorf("transaction begin error on master: %w", err)
	}

	return &txWrapper{
		tx: tx,
		db: db,
	}, nil
}

// ExecuteInTransaction выполняет функцию в транзакции с автоматическим коммитом или откатом
func (db *DB) ExecuteInTransaction(ctx context.Context, txOptions TxOptions, fn func(Tx) error) error {
	tx, err := db.BeginTx(ctx, txOptions)
	if err != nil {
		return fmt.Errorf("transaction begin error: %w", err)
	}
	defer func() {
		if tx != nil {
			// Если транзакция не завершена (не зафиксирована и не откачена), откатываем
			tx.Rollback(ctx)
		}
	}()

	err = fn(tx)
	if err != nil {
		rbErr := tx.Rollback(ctx)
		if rbErr != nil {
			return fmt.Errorf("function execution error in transaction: %v, rollback error: %w", err, rbErr)
		}
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("transaction commit error: %w", err)
	}

	// Устанавливаем tx в nil, чтобы избежать двойного отката в defer
	tx = nil
	return nil
}

// ExecuteInTransactionDefault выполняет функцию в транзакции с параметрами по умолчанию
func (db *DB) ExecuteInTransactionDefault(ctx context.Context, fn func(Tx) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("transaction begin error: %w", err)
	}
	defer func() {
		if tx != nil {
			// Если транзакция не завершена (не зафиксирована и не откачена), откатываем
			tx.Rollback(ctx)
		}
	}()

	err = fn(tx)
	if err != nil {
		rbErr := tx.Rollback(ctx)
		if rbErr != nil {
			return fmt.Errorf("function execution error in transaction: %v, rollback error: %w", err, rbErr)
		}
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("transaction commit error: %w", err)
	}

	// Устанавливаем tx в nil, чтобы избежать двойного отката в defer
	tx = nil
	return nil
}
