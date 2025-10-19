package pgxwrapper

import (
	"context"
)

// retryableRow обертка для Row с поддержкой повторных попыток
type retryableRow struct {
	ctx     context.Context
	conn    Conn
	manager *ReplicaManager
	sql     string
	args    []any
	row     Row
	err     error
}

// Scan сканирует значения в переменные с повторными попытками
func (rr *retryableRow) Scan(dest ...any) error {
	if rr.row != nil {
		return rr.row.Scan(dest...)
	}

	// Если row еще не установлен, выполняем запрос с повторными попытками
	err := rr.manager.ExecuteQueryWithRetry(rr.ctx, func(conn Conn) error {
		row := conn.QueryRow(rr.ctx, rr.sql, rr.args...)
		rr.row = row
		rr.err = row.Scan(dest...)
		return rr.err
	})

	return err
}
