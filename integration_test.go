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

		driver, err := NewDriver(context.Background(), config)
		require.NoError(t, err)
		defer driver.Close(context.Background())

		// Проверяем, что можно получить подключение к мастеру
		masterConn := driver.Master()
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

		driver, err := NewDriver(context.Background(), config)
		require.NoError(t, err)
		defer driver.Close(context.Background())

		// Выполняем простой запрос
		masterConn := driver.Master()
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

		driver, err := NewDriver(context.Background(), config)
		require.NoError(t, err)
		defer driver.Close(context.Background())

		// Начинаем транзакцию
		tx, err := driver.Begin(context.Background())
		assert.NoError(t, err)

		// Откатываем транзакцию
		err = tx.Rollback(context.Background())
		assert.NoError(t, err)
	})
}
