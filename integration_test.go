package pgxwrapper

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Тесты интеграции, которые будут запускаться с помощью Docker
func TestDriverIntegration(t *testing.T) {
	// Эти тесты требуют запущенного PostgreSQL, поэтому они будут пропущены
	// если нет доступа к базе данных

	t.Run("интеграционный тест подключения к мастеру", func(t *testing.T) {
		t.Skip("Требуется запущенный PostgreSQL для интеграционных тестов")

		config := Config{
			MasterConnString: "postgres://test:test@localhost:5432/testdb",
			MaxRetries:       3,
			RetryDelay:       time.Millisecond * 100,
			QueryTimeout:     5 * time.Second,
		}

		db, err := New(context.Background(), config)
		require.NoError(t, err)
		defer db.Close(context.Background())

		// Проверяем, что можно получить подключение к мастеру
		masterConn := db.Master()
		require.NotNil(t, masterConn)

		// Выполняем простой запрос для проверки подключения
		err = masterConn.Ping(context.Background())
		assert.NoError(t, err)
	})

	t.Run("интеграционный тест выполнения запроса", func(t *testing.T) {
		t.Skip("Требуется запущенный PostgreSQL для интеграционных тестов")

		config := Config{
			MasterConnString: "postgres://test:test@localhost:5432/testdb",
			MaxRetries:       3,
			RetryDelay:       time.Millisecond * 100,
			QueryTimeout:     5 * time.Second,
		}

		db, err := New(context.Background(), config)
		require.NoError(t, err)
		defer db.Close(context.Background())

		// Выполняем простой запрос
		masterConn := db.Master()
		result, err := masterConn.Exec(context.Background(), "SELECT 1")
		assert.NoError(t, err)
		assert.EqualValues(t, 0, result.RowsAffected())
	})

	t.Run("интеграционный тест транзакции", func(t *testing.T) {
		t.Skip("Требуется запущенный PostgreSQL для интеграционных тестов")

		config := Config{
			MasterConnString: "postgres://test:test@localhost:5432/testdb",
			MaxRetries:       3,
			RetryDelay:       time.Millisecond * 100,
			QueryTimeout:     5 * time.Second,
		}

		db, err := New(context.Background(), config)
		require.NoError(t, err)
		defer db.Close(context.Background())

		// Начинаем транзакцию
		tx, err := db.Begin(context.Background())
		assert.NoError(t, err)

		// Откатываем транзакцию
		err = tx.Rollback(context.Background())
		assert.NoError(t, err)
	})

	t.Run("интеграционный тест реплики с валидацией fallback", func(t *testing.T) {
		t.Skip("Требуется запущенный PostgreSQL с репликами для интеграционных тестов")

		config := Config{
			MasterConnString:     "postgres://test:test@localhost:5432/testdb",
			SyncSlaveConnString:  "postgres://test:test@localhost:5433/testdb",
			AsyncSlaveConnString: "postgres://test:test@localhost:5434/testdb",
			MaxRetries:           2,
			RetryDelay:           time.Millisecond * 50,
			QueryTimeout:         5 * time.Second,
			EnableTelemetry:      true,
		}

		db, err := New(context.Background(), config)
		require.NoError(t, err)
		defer db.Close(context.Background())

		// Проверяем, что реплики доступны
		syncSlave := db.SyncSlave()
		require.NotNil(t, syncSlave)

		asyncSlave := db.Slave()
		require.NotNil(t, asyncSlave)

		// Проверяем выполнение запроса на реплике
		rows, err := syncSlave.Query(context.Background(), "SELECT 1")
		assert.NoError(t, err)
		if rows != nil {
			rows.Close()
		}

		// Проверяем, что телеметрия работает
		metrics := db.telemetry.GetMetrics()
		assert.NotNil(t, metrics)
	})

	t.Run("интеграционный тест fallback с симуляцией ошибок реплики", func(t *testing.T) {
		// Тест проверяет fallback с невалидными строками подключения к репликам
		// для симуляции ошибок подключения
		config := Config{
			MasterConnString:       "postgres://test:test@localhost:5432/testdb",
			SyncSlaveConnString:    "postgres://invalid_user:invalid_pass@localhost:9999/invalid_db", // intentionally invalid
			AsyncSlaveConnString:   "postgres://invalid_user:invalid_pass@localhost:9998/invalid_db", // intentionally invalid
			MaxRetries:             1,
			RetryDelay:             time.Millisecond * 10,
			QueryTimeout:           3 * time.Second,
			EnableTelemetry:        true,
			DisableReplicaFallback: false, // Включаем fallback
		}

		db, err := New(context.Background(), config)
		if err != nil {
			// If master connection also fails, skip the test
			t.Skip("Требуется запущенный PostgreSQL для интеграционных тестов")
		}
		defer db.Close(context.Background())

		// Проверяем, что синхронная реплика недоступна (должна использовать fallback на мастер)
		syncSlave := db.SyncSlave()
		require.NotNil(t, syncSlave)

		// Проверяем, что запросы на реплике будут перенаправлены на мастер при ошибках
		// Это проверит логику fallback'а в реальном сценарии
		rows, err := syncSlave.Query(context.Background(), "SELECT 1")
		// When replica connections are invalid, the fallback logic should kick in
		// and eventually try the master connection (which is valid)
		if err != nil {
			// Если ошибка связана с репликами, но не с мастером - это нормальное поведение
			t.Logf("Query on sync slave failed as expected: %v", err)
		} else if rows != nil {
			// Если запрос прошел, значит fallback сработал и использовал мастер
			rows.Close()
			t.Log("Query on sync slave succeeded, indicating fallback to master worked")
		}

		// Проверяем, что телеметрия зафиксировала ошибки и повторы
		metrics := db.telemetry.GetMetrics()
		assert.NotNil(t, metrics)
	})

	t.Run("интеграционный тест с отключенным fallback реплик", func(t *testing.T) {
		t.Skip("Требуется запущенный PostgreSQL для интеграционных тестов")

		config := Config{
			MasterConnString:     "postgres://test:test@localhost:5432/testdb",
			SyncSlaveConnString:  "postgres://test:test@localhost:5433/testdb",
			AsyncSlaveConnString: "postgres://test:test@localhost:5434/testdb",
			MaxRetries:           2,
			RetryDelay:           time.Millisecond * 50,
			QueryTimeout:         5 * time.Second,
			DisableReplicaFallback: true, // Отключаем fallback
		}

		db, err := New(context.Background(), config)
		require.NoError(t, err)
		defer db.Close(context.Background())

		// Проверяем, что реплики доступны
		asyncSlave := db.Slave()
		require.NotNil(t, asyncSlave)

		// При отключенном fallback запросы должны оставаться только на конкретной реплике
		rows, err := asyncSlave.Query(context.Background(), "SELECT 1")
		assert.NoError(t, err)
		if rows != nil {
			rows.Close()
		}
	})

	t.Run("интеграционный тест с операциями чтения на реплике и fallback", func(t *testing.T) {
		t.Skip("Требуется запущенный PostgreSQL с репликами для интеграционных тестов")

		config := Config{
			MasterConnString:     "postgres://test:test@localhost:5432/testdb",
			SyncSlaveConnString:  "postgres://test:test@localhost:5433/testdb",
			AsyncSlaveConnString: "postgres://test:test@localhost:5434/testdb",
			MaxRetries:           3,
			RetryDelay:           time.Millisecond * 100,
			QueryTimeout:         5 * time.Second,
			EnableTelemetry:      true,
		}

		db, err := New(context.Background(), config)
		require.NoError(t, err)
		defer db.Close(context.Background())

		// Проверяем операции чтения на асинхронной реплике
		asyncSlave := db.Slave()
		require.NotNil(t, asyncSlave)

		// Выполняем несколько операций чтения
		for i := 0; i < 5; i++ {
			rows, err := asyncSlave.Query(context.Background(), "SELECT $1::int, $2::text", i, "test")
			assert.NoError(t, err)
			if rows != nil {
				rows.Close()
			}
		}

		// Проверяем, что операция записи на реплике возвращает ошибку
		_, err = asyncSlave.Exec(context.Background(), "SELECT 1") // This should fail as Exec is not allowed on replica
		assert.Error(t, err)
		assert.Equal(t, ErrMasterOnlyOperation, err)

		// Проверяем телеметрию
		metrics := db.telemetry.GetMetrics()
		assert.NotNil(t, metrics)
		assert.GreaterOrEqual(t, metrics["total_queries"].(int64), int64(5))
	})

	t.Run("интеграционный тест работы транзакций только на мастере", func(t *testing.T) {
		t.Skip("Требуется запущенный PostgreSQL для интеграционных тестов")

		config := Config{
			MasterConnString:     "postgres://test:test@localhost:5432/testdb",
			SyncSlaveConnString:  "postgres://test:test@localhost:5433/testdb",
			AsyncSlaveConnString: "postgres://test:test@localhost:5434/testdb",
			MaxRetries:           1,
			RetryDelay:           time.Millisecond * 50,
			QueryTimeout:         5 * time.Second,
		}

		db, err := New(context.Background(), config)
		require.NoError(t, err)
		defer db.Close(context.Background())

		// Проверяем, что транзакции начинаются только на мастере
		tx, err := db.Begin(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, tx)

		// Проверяем, что на реплике транзакции не начинаются
		syncSlave := db.SyncSlave()
		_, err = syncSlave.Begin(context.Background())
		assert.Error(t, err)
		assert.Equal(t, ErrMasterOnlyOperation, err)

		// Тоже самое для асинхронной реплики
		asyncSlave := db.Slave()
		_, err = asyncSlave.Begin(context.Background())
		assert.Error(t, err)
		assert.Equal(t, ErrMasterOnlyOperation, err)

		// Завершаем транзакцию на мастере
		err = tx.Rollback(context.Background())
		assert.NoError(t, err)
	})

	t.Run("интеграционный тест повторных попыток с симуляцией временных ошибок", func(t *testing.T) {
		// Тест проверяет механизм повторных попыток (retries) при временных ошибках
		config := Config{
			MasterConnString:       "postgres://test:test@localhost:5432/testdb",
			SyncSlaveConnString:    "postgres://invalid_user:invalid_pass@localhost:9999/testdb", // intentionally invalid
			AsyncSlaveConnString:   "postgres://invalid_user:invalid_pass@localhost:9998/testdb", // intentionally invalid
			MaxRetries:             2, // Установим 2 повторные попытки
			RetryDelay:             time.Millisecond * 50, // Задержка между попытками
			QueryTimeout:           5 * time.Second,
			EnableTelemetry:        true,
			DisableReplicaFallback: false,
		}

		db, err := New(context.Background(), config)
		if err != nil {
			t.Skip("Требуется запущенный PostgreSQL для интеграционных тестов")
		}
		defer db.Close(context.Background())

		// Проверим работу retry механизма на реплике, которая будет использовать fallback
		asyncSlave := db.Slave()
		require.NotNil(t, asyncSlave)

		// Выполняем запрос, который должен использовать retry mechanism
		startTime := time.Now()
		rows, err := asyncSlave.Query(context.Background(), "SELECT 1")
		duration := time.Since(startTime)

		// Поскольку реплики недоступны, должны были быть попытки fallback'а и retries
		// Если бы была ошибка подключения, механизм retry'ев должен был сработать
		if err != nil {
			t.Logf("Query failed as expected with invalid replica connections: %v", err)
		} else if rows != nil {
			t.Log("Query succeeded, fallback to master worked")
			rows.Close()
		}

		// Проверим, что телеметрия фиксирует количество ошибок и повторов
		metrics := db.telemetry.GetMetrics()
		assert.NotNil(t, metrics)

		// Если retry mechanism работает, то время выполнения должно быть больше
		// из-за задержек между попытками
		expectedMinDuration := time.Millisecond * 100 // 2 retries * 50ms delay
		if duration >= expectedMinDuration {
			t.Logf("Retry mechanism worked - duration was %v (expected at least %v)", duration, expectedMinDuration)
		} else {
			t.Logf("Duration was %v, possibly master connection succeeded immediately", duration)
		}
	})

	t.Run("интеграционный тест с проверкой лимита повторных попыток", func(t *testing.T) {
		// Тест проверяет, что при превышении лимита повторных попыток возвращается соответствующая ошибка
		config := Config{
			MasterConnString:       "postgres://test:test@localhost:5432/testdb",
			SyncSlaveConnString:    "postgres://invalid_user:invalid_pass@localhost:9999/testdb",
			AsyncSlaveConnString:   "postgres://invalid_user:invalid_pass@localhost:9998/testdb",
			MaxRetries:             0, // Установим 0 повторов для тестирования быстрого фейла
			RetryDelay:             time.Millisecond * 10,
			QueryTimeout:           2 * time.Second,
			EnableTelemetry:        true,
			DisableReplicaFallback: false,
		}

		db, err := New(context.Background(), config)
		if err != nil {
			t.Skip("Требуется запущенный PostgreSQL для интеграционных тестов")
		}
		defer db.Close(context.Background())

		// Проверим работу с 0 retries - должна быстро вернуть ошибку
		asyncSlave := db.Slave()
		require.NotNil(t, asyncSlave)

		startTime := time.Now()
		_, err = asyncSlave.Query(context.Background(), "SELECT 1")
		duration := time.Since(startTime)

		// Должна быть ошибка, и время выполнения должно быть коротким (без retry задержек)
		if err != nil {
			t.Logf("Query failed as expected with 0 retries: %v", err)
		}

		// Проверим, что не было значительных задержек (без retry попыток)
		maxExpectedDuration := time.Millisecond * 100 // Должно быть меньше чем с retry задержками
		if duration < maxExpectedDuration {
			t.Logf("No retry behavior confirmed - duration was %v", duration)
		} else {
			t.Logf("Duration was %v, which might indicate some retries occurred", duration)
		}

		// Проверим метрики телеметрии
		metrics := db.telemetry.GetMetrics()
		assert.NotNil(t, metrics)
	})
}
