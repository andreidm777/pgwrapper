package pgxwrapper

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
)

// retryableConn обертка для подключения с поддержкой повторных попыток
type retryableConn struct {
	conn    Conn
	manager *ReplicaManager
}

// Exec выполняет SQL команду с повторными попытками
func (rc *retryableConn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	var result pgconn.CommandTag
	var err error

	err = rc.manager.ExecuteQueryWithRetry(ctx, func(conn Conn) error {
		result, err = conn.Exec(ctx, sql, arguments...)
		return err
	})

	return result, err
}

// Query выполняет SQL запрос с повторными попытками
func (rc *retryableConn) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	var result Rows
	var err error

	err = rc.manager.ExecuteQueryWithRetry(ctx, func(conn Conn) error {
		result, err = conn.Query(ctx, sql, args...)
		return err
	})

	return result, err
}

// QueryRow выполняет SQL запрос и возвращает одну строку с повторными попытками
func (rc *retryableConn) QueryRow(ctx context.Context, sql string, args ...any) Row {
	// Создаем обертку для Row, которая будет использовать повторные попытки
	return &retryableRow{
		ctx:     ctx,
		conn:    rc.conn,
		manager: rc.manager,
		sql:     sql,
		args:    args,
	}
}

// Begin начинает транзакцию
func (rc *retryableConn) Begin(ctx context.Context) (Tx, error) {
	// Для транзакций используем только мастер, иначе возвращаем ошибку
	return rc.conn.Begin(ctx)
}

// BeginTx начинает транзакцию с опциями
func (rc *retryableConn) BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error) {
	// Для транзакций используем только мастер, иначе возвращаем ошибку
	return rc.conn.BeginTx(ctx, txOptions)
}

// Ping проверяет соединение
func (rc *retryableConn) Ping(ctx context.Context) error {
	return rc.manager.ExecuteQueryWithRetry(ctx, func(conn Conn) error {
		return conn.Ping(ctx)
	})
}

// Close закрывает соединение
func (rc *retryableConn) Close(ctx context.Context) error {
	return rc.conn.Close(ctx)
}
