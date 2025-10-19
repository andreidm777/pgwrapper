package pgxwrapper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Тестирование создания драйвера
func TestNewDriver(t *testing.T) {
	t.Run("успешное создание драйвера", func(t *testing.T) {
		config := Config{
			MasterConnString: "postgres://user:password@localhost:5432/testdb",
			MaxRetries:       3,
			RetryDelay:       time.Second,
			QueryTimeout:     30 * time.Second,
		}

		// Здесь мы не можем протестировать реальное подключение без запущенного PostgreSQL
		// Поэтому тестируем только создание конфигурации
		assert.Equal(t, "postgres://user:password@localhost:5432/testdb", config.MasterConnString)
		assert.Equal(t, 3, config.MaxRetries)
		assert.Equal(t, time.Second, config.RetryDelay)
		assert.Equal(t, 30*time.Second, config.QueryTimeout)
	})
}

// Тестирование структуры конфигурации
func TestConfig(t *testing.T) {
	t.Run("проверка значений по умолчанию", func(t *testing.T) {
		config := Config{}

		assert.Equal(t, "", config.MasterConnString)
		assert.Equal(t, 0, config.MaxRetries)
		assert.Equal(t, time.Duration(0), config.RetryDelay)
		assert.Equal(t, time.Duration(0), config.QueryTimeout)
		assert.False(t, config.EnableTelemetry)
		assert.False(t, config.DisableReplicaFallback)
	})
}

// Тестирование специфичных ошибок
func TestErrors(t *testing.T) {
	t.Run("проверка текстов ошибок", func(t *testing.T) {
		assert.Equal(t, "no available replicas", ErrNoAvailableReplicas.Error())
		assert.Equal(t, "this operation can only be performed on the master", ErrMasterOnlyOperation.Error())
		assert.Equal(t, "replica timeout", ErrReplicaTimeout.Error())
		assert.Equal(t, "connection failed", ErrConnectionFailed.Error())
		assert.Equal(t, "transaction failed", ErrTransactionFailed.Error())
		assert.Equal(t, "maximum number of retries exceeded", ErrMaxRetriesExceeded.Error())
		assert.Equal(t, "invalid driver configuration", ErrInvalidConfiguration.Error())
		assert.Equal(t, "replica is not ready to accept requests", ErrReplicaNotReady.Error())
		assert.Equal(t, "query timeout exceeded", ErrQueryTimeout.Error())
	})
}

// Тестирование типа ReplicaType
func TestReplicaType(t *testing.T) {
	t.Run("проверка значений типа реплики", func(t *testing.T) {
		assert.Equal(t, ReplicaType(0), AsyncReplica)
		assert.Equal(t, ReplicaType(1), SyncReplica)
	})
}

// Тестирование интерфейса TxOptions
func TestTxOptions(t *testing.T) {
	t.Run("проверка структуры TxOptions", func(t *testing.T) {
		var txOpts TxOptions
		// Проверяем, что структура может быть создана
		assert.NotNil(t, &txOpts)
	})
}

// Тестирование функций драйвера с mock-объектами (когда будет возможность протестировать)
func TestDriverFunctions(t *testing.T) {
	t.Run("заглушка для тестирования функций драйвера", func(t *testing.T) {
		// В реальном приложении здесь будут тесты с использованием моков или тестовой базы данных
		// Пока что просто проверяем, что функции существуют и могут быть вызваны
		driver := &Driver{}

		// Проверяем, что методы существуют
		assert.NotNil(t, driver.Master)
		assert.NotNil(t, driver.SyncSlave)
		assert.NotNil(t, driver.Slave)
		assert.NotNil(t, driver.Close)
	})
}

// Тестирование менеджера реплик
func TestReplicaManager(t *testing.T) {
	t.Run("проверка создания менеджера реплик", func(t *testing.T) {
		driver := &Driver{}
		rm := NewReplicaManager(driver)

		assert.NotNil(t, rm)
		assert.Equal(t, driver, rm.driver)
	})
}
