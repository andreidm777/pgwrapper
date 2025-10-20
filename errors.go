package pgxwrapper

import "errors"

// Определение специфичных ошибок для драйвера

// ErrNoAvailableReplicas ошибка, когда нет доступных реплик
var ErrNoAvailableReplicas = errors.New("no available replicas")

// ErrMasterOnlyOperation ошибка, когда операция может быть выполнена только на мастере
var ErrMasterOnlyOperation = errors.New("this operation can only be performed on the master")

// ErrReplicaTimeout ошибка таймаута при работе с репликой
var ErrReplicaTimeout = errors.New("replica timeout")

// ErrConnectionFailed ошибка подключения к базе данных
var ErrConnectionFailed = errors.New("connection failed")

// ErrTransactionFailed ошибка выполнения транзакции
var ErrTransactionFailed = errors.New("transaction failed")

// ErrMaxRetriesExceeded ошибка, когда превышено максимальное количество попыток
var ErrMaxRetriesExceeded = errors.New("maximum number of retries exceeded")

// ErrInvalidConfiguration ошибка, когда конфигурация драйвера невалидна
var ErrInvalidConfiguration = errors.New("invalid db configuration")

// ErrReplicaNotReady ошибка, когда реплика не готова к приему запросов
var ErrReplicaNotReady = errors.New("replica is not ready to accept requests")

// ErrQueryTimeout ошибка таймаута выполнения запроса
var ErrQueryTimeout = errors.New("query timeout exceeded")
