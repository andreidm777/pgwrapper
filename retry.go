package pgxwrapper

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// ReplicaManager менеджер реплик для обработки запросов с переключением между ними
type ReplicaManager struct {
	db *DB
}

// NewReplicaManager создает новый менеджер реплик
func NewReplicaManager(db *DB) *ReplicaManager {
	return &ReplicaManager{
		db: db,
	}
}

// ExecuteWithFallback выполняет операцию с переключением между репликами при ошибках
func (rm *ReplicaManager) ExecuteWithFallback(ctx context.Context, operation func(Conn) error) error {
	if !rm.db.replicaFallback {
		// Если отключено переключение между репликами, используем только асинхронную реплику
		if rm.db.asyncSlave != nil {
			conn := &replicaConn{masterConn{conn: rm.db.asyncSlave, db: rm.db}, AsyncReplica}
			return operation(conn)
		}
		return ErrNoAvailableReplicas
	}

	// Порядок попыток: AsyncSlave -> SyncSlave -> Master
	connections := []struct {
		conn        Conn
		replicaType ReplicaType
		name        string
	}{
		{&replicaConn{masterConn{conn: rm.db.asyncSlave, db: rm.db}, AsyncReplica}, AsyncReplica, "async slave"},
		{&replicaConn{masterConn{conn: rm.db.syncSlave, db: rm.db}, SyncReplica}, SyncReplica, "sync slave"},
		{&masterConn{conn: rm.db.master, db: rm.db}, -1, "master"},
	}

	var lastErr error
	for _, connInfo := range connections {
		if connInfo.conn == nil {
			continue // Skipping unavailable connections
		}

		err := operation(connInfo.conn)
		if err == nil {
			return nil // Операция выполнена успешно
		}

		// Записываем ошибку в телеметрию
		if rm.db.telemetry != nil {
			rm.db.telemetry.RecordError()
		}

		// Если ошибка не связана с подключением или таймаутом, не пытаемся на других репликах
		if !isConnectionError(err) {
			return err
		}

		lastErr = err
		rm.db.logger.InfoContext(ctx, fmt.Sprintf("Operation failed on %s, trying the next replica", connInfo.name), "error", err)
	}

	if lastErr != nil {
		return fmt.Errorf("operation not performed on any replica: %w", lastErr)
	}

	return ErrNoAvailableReplicas
}

// ExecuteQueryWithRetry выполняет запрос с повторными попытками и переключением между репликами
func (rm *ReplicaManager) ExecuteQueryWithRetry(ctx context.Context, operation func(Conn) error) error {
	var lastErr error

	for attempt := 0; attempt <= rm.db.config.MaxRetries; attempt++ {
		err := rm.ExecuteWithFallback(ctx, operation)
		if err == nil {
			return nil // Операция выполнена успешно
		}

		// Если ошибка не связана с подключением или таймаутом, не повторяем
		if !isConnectionError(err) {
			return err
		}

		lastErr = err

		// Увеличиваем счетчик повторных попыток в телеметрии
		if rm.db.telemetry != nil {
			rm.db.telemetry.RecordRetry()
		}

		// Если это не последняя попытка, ждем перед следующей
		if attempt < rm.db.config.MaxRetries {
			time.Sleep(rm.db.config.RetryDelay)
		}
	}

	if lastErr != nil {
		return fmt.Errorf("operation not performed after %d attempts: %w", rm.db.config.MaxRetries+1, lastErr)
	}

	return ErrMaxRetriesExceeded
}

// ExecuteReadQueryWithFallback выполняет запрос на чтение с переключением между репликами
func (rm *ReplicaManager) ExecuteReadQueryWithFallback(ctx context.Context, query string, args ...any) (Rows, error) {
	var result Rows
	var err error

	err = rm.ExecuteWithFallback(ctx, func(conn Conn) error {
		result, err = conn.Query(ctx, query, args...)
		return err
	})

	return result, err
}

// ExecuteReadQueryWithRetry выполняет запрос на чтение с повторными попытками
func (rm *ReplicaManager) ExecuteReadQueryWithRetry(ctx context.Context, query string, args ...any) (Rows, error) {
	var result Rows
	var err error

	for attempt := 0; attempt <= rm.db.config.MaxRetries; attempt++ {
		result, err = rm.ExecuteReadQueryWithFallback(ctx, query, args...)
		if err == nil {
			return result, nil // Запрос выполнен успешно
		}

		// Если ошибка не связана с подключением или таймаутом, не повторяем
		if !isConnectionError(err) {
			return nil, err
		}

		// Увеличиваем счетчик повторных попыток в телеметрии
		if rm.db.telemetry != nil {
			rm.db.telemetry.RecordRetry()
		}

		// Если это не последняя попытка, ждем перед следующей
		if attempt < rm.db.config.MaxRetries {
			time.Sleep(rm.db.config.RetryDelay)
		}
	}

	return nil, fmt.Errorf("%w: read query not performed after %d attempts: %v", ErrMaxRetriesExceeded, rm.db.config.MaxRetries+1, err)
}

// isConnectionError проверяет, связана ли ошибка с подключением
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	if err.Error() == "connection refused" ||
		err.Error() == "connection reset by peer" {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr != nil {
		// Определенные коды ошибок PostgreSQL могут указывать на проблемы с подключением
		switch pgErr.Code {
		case "08001", // SQLSTATE connection failure
			"08003", // SQLSTATE connection does not exist
			"08006": // SQLSTATE connection failure
			return true
		}
	}

	return false
}
