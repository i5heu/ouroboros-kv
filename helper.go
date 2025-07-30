package ouroboroskv

import (
	"sync/atomic"
	"time"
)

func (k *KV) StartTransactionCounter(paths []string, minimumFreeSpace int) {

	// Start the ticker to log operations per second
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			readOps := atomic.SwapUint64(&k.readCounter, 0)
			writeOps := atomic.SwapUint64(&k.writeCounter, 0)
			k.config.Logger.Infof("Chunk Read operations/sec: %d, Chunk Write operations/sec: %d\n", readOps, writeOps)
		}
	}()
}
