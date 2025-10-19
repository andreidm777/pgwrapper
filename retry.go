package pgxwrapper

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// ReplicaManager менеджер реплик для обработки запросов с переключением между ними
type ReplicaManager struct {
	driver *Driver
}

// NewReplicaManager создает новый менеджер реплик
func NewReplicaManager(driver *Driver) *ReplicaManager {
	return &ReplicaManager{
		driver: driver,
	}
}

// ExecuteWithFallback выполняет операцию с переключением между репликами при ошибках
func (rm *ReplicaManager) ExecuteWithFallback(ctx context.Context, operation func(Conn) error) error {
	if !rm.driver.replicaFallback {
		// Если отключено переключение между репликами, используем только асинхронную реплику
		if rm.driver.asyncSlave != nil {
			conn := &replicaConn{conn: rm.driver.asyncSlave, driver: rm.driver, replicaType: AsyncReplica}
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
		{&replicaConn{conn: rm.driver.asyncSlave, driver: rm.driver, replicaType: AsyncReplica}, AsyncReplica, "async slave"},
		{&replicaConn{conn: rm.driver.syncSlave, driver: rm.driver, replicaType: SyncReplica}, SyncReplica, "sync slave"},
		{&masterConn{conn: rm.driver.master, driver: rm.driver}, -1, "master"},
	}

	var lastErr error
	for _, connInfo := range connections {
		if connInfo.conn == nil {
			continue // Пропускаем недоступные подключения
		}

		err := operation(connInfo.conn)
		if err == nil {
			return nil // Операция выполнена успешно
		}

		// Записываем ошибку в телеметрию
		if rm.driver.telemetry != nil {
			rm.driver.telemetry.RecordError()
		}

		// Если ошибка не связана с подключением или таймаутом, не пытаемся на других репликах
		if !isConnectionError(err) {
			return err
		}

		lastErr = err
		rm.driver.logger.InfoContext(ctx, fmt.Sprintf("Ошибка при выполнении операции на %s, пробуем следующую реплику", connInfo.name), "error", err)
	}

	if lastErr != nil {
		return fmt.Errorf("операция не выполнена ни на одной реплике: %w", lastErr)
	}

	return ErrNoAvailableReplicas
}

// ExecuteQueryWithRetry выполняет запрос с повторными попытками и переключением между репликами
func (rm *ReplicaManager) ExecuteQueryWithRetry(ctx context.Context, operation func(Conn) error) error {
	var lastErr error

	for attempt := 0; attempt <= rm.driver.config.MaxRetries; attempt++ {
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
		if rm.driver.telemetry != nil {
			rm.driver.telemetry.RecordRetry()
		}

		// Если это не последняя попытка, ждем перед следующей
		if attempt < rm.driver.config.MaxRetries {
			time.Sleep(rm.driver.config.RetryDelay)
		}
	}

	if lastErr != nil {
		return fmt.Errorf("операция не выполнена после %d попыток: %w", rm.driver.config.MaxRetries+1, lastErr)
	}

	return ErrMaxRetriesExceeded
}

// ExecuteReadQueryWithFallback выполняет запрос на чтение с переключением между репликами
func (rm *ReplicaManager) ExecuteReadQueryWithFallback(ctx context.Context, query string, args ...any) (Rows, error) {
	var result Rows
	var err error

	err = rm.ExecuteWithFallback(ctx, func(conn Conn) error {
		// Проверяем, является ли это репликой
		if _, ok := conn.(*masterConn); !ok {
			// Для реплик выполняем запрос на чтение
			result, err = conn.Query(ctx, query, args...)
			return err
		} else {
			// Если мы на мастере, это ошибка, так как чтение на мастере не предполагается
			rm.driver.logger.ErrorContext(ctx, "Попытка выполнить операцию чтения на мастере", "query", query)
			return ErrMasterOnlyOperation
		}
	})

	return result, err
}

// ExecuteReadQueryWithRetry выполняет запрос на чтение с повторными попытками
func (rm *ReplicaManager) ExecuteReadQueryWithRetry(ctx context.Context, query string, args ...any) (Rows, error) {
	var result Rows
	var err error

	for attempt := 0; attempt <= rm.driver.config.MaxRetries; attempt++ {
		result, err = rm.ExecuteReadQueryWithFallback(ctx, query, args...)
		if err == nil {
			return result, nil // Запрос выполнен успешно
		}

		// Если ошибка не связана с подключением или таймаутом, не повторяем
		if !isConnectionError(err) {
			return nil, err
		}

		// Увеличиваем счетчик повторных попыток в телеметрии
		if rm.driver.telemetry != nil {
			rm.driver.telemetry.RecordRetry()
		}

		// Если это не последняя попытка, ждем перед следующей
		if attempt < rm.driver.config.MaxRetries {
			time.Sleep(rm.driver.config.RetryDelay)
		}
	}

	return nil, fmt.Errorf("%w: запрос на чтение не выполнен после %d попыток: %v", ErrMaxRetriesExceeded, rm.driver.config.MaxRetries+1, err)
}

// isConnectionError проверяет, связана ли ошибка с подключением
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	if err.Error() == "context deadline exceeded" ||
		err.Error() == "connection refused" ||
		err.Error() == "connection reset by peer" {
		return true
	}

	var pgErr *pgconn.PgError
	if pgErr != nil {
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
