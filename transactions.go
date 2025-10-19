package pgxwrapper

import (
	"context"
	"fmt"
	"time"
)

// BeginTx начинает новую транзакцию на мастере
func (d *Driver) BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error) {
	if d.telemetry != nil && d.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			d.telemetry.RecordQuery(duration)
		}()
	}

	// Все транзакции начинаются только на мастере
	tx, err := d.master.BeginTx(ctx, txOptions.TxOptions)
	if err != nil {
		if d.telemetry != nil {
			d.telemetry.RecordError()
		}
		return nil, fmt.Errorf("ошибка начала транзакции на мастере: %w", err)
	}

	return &txWrapper{
		tx:     tx,
		driver: d,
	}, nil
}

// Begin начинает новую транзакцию на мастере с параметрами по умолчанию
func (d *Driver) Begin(ctx context.Context) (Tx, error) {
	if d.telemetry != nil && d.telemetry.IsEnabled() {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			d.telemetry.RecordQuery(duration)
		}()
	}

	// Все транзакции начинаются только на мастере
	tx, err := d.master.Begin(ctx)
	if err != nil {
		if d.telemetry != nil {
			d.telemetry.RecordError()
		}
		return nil, fmt.Errorf("ошибка начала транзакции на мастере: %w", err)
	}

	return &txWrapper{
		tx:     tx,
		driver: d,
	}, nil
}

// ExecuteInTransaction выполняет функцию в транзакции с автоматическим коммитом или откатом
func (d *Driver) ExecuteInTransaction(ctx context.Context, txOptions TxOptions, fn func(Tx) error) error {
	tx, err := d.BeginTx(ctx, txOptions)
	if err != nil {
		return fmt.Errorf("ошибка начала транзакции: %w", err)
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
			return fmt.Errorf("ошибка выполнения функции в транзакции: %v, ошибка отката транзакции: %w", err, rbErr)
		}
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("ошибка фиксации транзакции: %w", err)
	}

	// Устанавливаем tx в nil, чтобы избежать двойного отката в defer
	tx = nil
	return nil
}

// ExecuteInTransactionDefault выполняет функцию в транзакции с параметрами по умолчанию
func (d *Driver) ExecuteInTransactionDefault(ctx context.Context, fn func(Tx) error) error {
	tx, err := d.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ошибка начала транзакции: %w", err)
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
			return fmt.Errorf("ошибка выполнения функции в транзакции: %v, ошибка отката транзакции: %w", err, rbErr)
		}
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("ошибка фиксации транзакции: %w", err)
	}

	// Устанавливаем tx в nil, чтобы избежать двойного отката в defer
	tx = nil
	return nil
}
