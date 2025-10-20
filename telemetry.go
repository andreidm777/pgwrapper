package pgxwrapper

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Telemetry структура для телеметрии
type Telemetry struct {
	enabled bool
	logger  *slog.Logger
	mu      sync.RWMutex

	// Метрики
	totalQueries     int64
	totalErrors      int64
	totalRetries     int64
	queryDuration    time.Duration
	connectionErrors int64
}

// NewTelemetry создает новый экземпляр телеметрии
func NewTelemetry() *Telemetry {
	return &Telemetry{
		enabled: true,
		logger:  slog.Default(),
	}
}

// NewTelemetryWithLogger создает новый экземпляр телеметрии с указанным логгером
func NewTelemetryWithLogger(logger *slog.Logger) *Telemetry {
	return &Telemetry{
		enabled: true,
		logger:  logger,
	}
}

// Disable отключает телеметрию
func (t *Telemetry) Disable() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.enabled = false
}

// Enable включает телеметрию
func (t *Telemetry) Enable() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.enabled = true
}

// IsEnabled проверяет, включена ли телеметрия
func (t *Telemetry) IsEnabled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.enabled
}

// RecordQuery записывает информацию о запросе
func (t *Telemetry) RecordQuery(duration time.Duration) {
	if !t.IsEnabled() {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.totalQueries++
	t.queryDuration += duration
}

// RecordError записывает информацию об ошибке
func (t *Telemetry) RecordError() {
	if !t.IsEnabled() {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.totalErrors++
}

// RecordRetry записывает информацию о повторной попытке
func (t *Telemetry) RecordRetry() {
	if !t.IsEnabled() {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.totalRetries++
}

// RecordConnectionError записывает информацию об ошибке подключения
func (t *Telemetry) RecordConnectionError() {
	if !t.IsEnabled() {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.connectionErrors++
}

// GetMetrics возвращает текущие метрики
func (t *Telemetry) GetMetrics() map[string]any {
	t.mu.RLock()
	defer t.mu.RUnlock()

	avgDuration := time.Duration(0)
	if t.totalQueries > 0 {
		avgDuration = t.queryDuration / time.Duration(t.totalQueries)
	}

	return map[string]any{
		"total_queries":     t.totalQueries,
		"total_errors":      t.totalErrors,
		"total_retries":     t.totalRetries,
		"average_duration":  avgDuration,
		"connection_errors": t.connectionErrors,
		"enabled":           t.enabled,
	}
}

// LogMetrics логирует текущие метрики
func (t *Telemetry) LogMetrics(ctx context.Context) {
	if !t.IsEnabled() {
		return
	}

	metrics := t.GetMetrics()
	t.logger.InfoContext(ctx, "Telemetry metrics", slog.Any("data", metrics))
}
