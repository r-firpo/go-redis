// internal/store/store.go

package store

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"go-redis/redis-server/internal/persistence"
)

// DataStore represents our in-memory Redis-compatible data store
type DataStore struct {
	mu         sync.RWMutex
	data       map[string]string
	expires    map[string]time.Time
	logger     *zap.Logger
	rdbHandler *persistence.RDBHandler
	stopChan   chan struct{} // For clean shutdown
	shutdown   bool
}

// NewDataStore creates a new DataStore instance
func NewDataStore(logger *zap.Logger, rdbDir, rdbFile string) *DataStore {
	ds := &DataStore{
		data:       make(map[string]string),
		expires:    make(map[string]time.Time),
		logger:     logger,
		rdbHandler: persistence.NewRDBHandler(rdbDir, rdbFile, logger),
		stopChan:   make(chan struct{}),
	}

	// Load existing data from RDB
	if err := ds.loadFromRDB(); err != nil {
		logger.Error("Failed to load RDB file", zap.Error(err))
	}

	// Start background tasks
	go ds.periodicSave()
	go ds.periodicCleanup()

	return ds
}

// loadFromRDB loads data from the RDB file
func (ds *DataStore) loadFromRDB() error {
	items, err := ds.rdbHandler.Load()
	if err != nil {
		return err
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Clear existing data
	ds.data = make(map[string]string)
	ds.expires = make(map[string]time.Time)

	// Load data from RDB items
	for key, item := range items {
		ds.data[key] = item.Value
		if item.ExpireAt != nil {
			ds.expires[key] = time.UnixMilli(*item.ExpireAt)
		}
	}

	ds.logger.Info("Loaded data from RDB",
		zap.Int("keys", len(ds.data)),
		zap.Int("expires", len(ds.expires)),
	)

	return nil
}

// saveToRDB saves the current dataset to RDB file
func (ds *DataStore) saveToRDB() error {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	items := make(map[string]persistence.Item)
	for key, value := range ds.data {
		item := persistence.Item{Value: value}
		if expireTime, exists := ds.expires[key]; exists {
			ms := expireTime.UnixMilli()
			item.ExpireAt = &ms
		}
		items[key] = item
	}

	if err := ds.rdbHandler.Save(items); err != nil {
		ds.logger.Error("Failed to save RDB", zap.Error(err))
		return err
	}

	ds.logger.Info("Successfully saved RDB",
		zap.Int("keys", len(items)),
	)
	return nil
}

// periodicSave runs periodic RDB saves
func (ds *DataStore) periodicSave() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ds.stopChan:
			// Final save before shutdown
			if err := ds.saveToRDB(); err != nil {
				ds.logger.Error("Failed to save RDB during shutdown", zap.Error(err))
			}
			return
		case <-ticker.C:
			if err := ds.saveToRDB(); err != nil {
				ds.logger.Error("Failed periodic RDB save", zap.Error(err))
			}
		}
	}
}

// periodicCleanup removes expired keys
func (ds *DataStore) periodicCleanup() {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ds.stopChan:
			return
		case <-ticker.C:
			ds.cleanupExpired()
		}
	}
}

// cleanupExpired removes expired keys
func (ds *DataStore) cleanupExpired() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	now := time.Now()
	for key, expireTime := range ds.expires {
		if now.After(expireTime) {
			delete(ds.data, key)
			delete(ds.expires, key)
			ds.logger.Debug("Cleaned up expired key",
				zap.String("key", key),
				zap.Time("expired_at", expireTime),
			)
		}
	}
}

// Stop stops background tasks and performs final save
func (ds *DataStore) Stop() error {
	ds.mu.Lock()
	if ds.shutdown {
		ds.mu.Unlock()
		return nil
	}
	ds.shutdown = true
	ds.mu.Unlock()

	close(ds.stopChan)
	return ds.saveToRDB()
}

// Set stores a key-value pair with optional expiration
func (ds *DataStore) Set(key, value string, px *int) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.data[key] = value

	if px != nil {
		expireTime := time.Now().Add(time.Duration(*px) * time.Millisecond)
		ds.expires[key] = expireTime
		ds.logger.Debug("Set key with expiration",
			zap.String("key", key),
			zap.Int("px", *px),
			zap.Time("expires_at", expireTime),
		)
	} else {
		delete(ds.expires, key) // Remove any existing expiration
	}

	return nil
}

// Get retrieves a value by key
func (ds *DataStore) Get(key string) (string, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	// Check expiration
	if expireTime, exists := ds.expires[key]; exists {
		if time.Now().After(expireTime) {
			ds.mu.RUnlock()
			ds.mu.Lock()
			defer ds.mu.Unlock()
			delete(ds.data, key)
			delete(ds.expires, key)
			return "", false
		}
	}

	value, exists := ds.data[key]
	return value, exists
}

// Delete removes a key
func (ds *DataStore) Delete(key string) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	_, exists := ds.data[key]
	if exists {
		delete(ds.data, key)
		delete(ds.expires, key)
	}
	return exists
}

// Keys returns all non-expired keys
func (ds *DataStore) Keys() []string {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	keys := make([]string, 0, len(ds.data))
	now := time.Now()

	for key := range ds.data {
		if expireTime, hasExpiry := ds.expires[key]; !hasExpiry || now.Before(expireTime) {
			keys = append(keys, key)
		}
	}

	return keys
}
