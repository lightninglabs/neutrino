package chanutils

import (
	"sync"
	"time"
)

// BatchWriterConfig holds the configuration options for BatchWriter.
type BatchWriterConfig[T any] struct {
	// QueueBufferSize sets the buffer size of the output channel of the
	// concurrent queue used by the BatchWriter.
	QueueBufferSize int

	// MaxBatch is the maximum number of filters to be persisted to the DB
	// in one go.
	MaxBatch int

	// DBWritesTickerDuration is the time after receiving a filter that the
	// writer will wait for more filters before writing the current batch
	// to the DB.
	DBWritesTickerDuration time.Duration

	// PutItems will be used by the BatchWriter to persist filters in
	// batches.
	PutItems func(...T) error
}

// BatchWriter manages writing Filters to the DB and tries to batch the writes
// as much as possible.
type BatchWriter[T any] struct {
	started sync.Once
	stopped sync.Once

	cfg *BatchWriterConfig[T]

	queue *ConcurrentQueue[T]

	quit chan struct{}
	wg   sync.WaitGroup
}

// NewBatchWriter constructs a new BatchWriter using the given
// BatchWriterConfig.
func NewBatchWriter[T any](cfg *BatchWriterConfig[T]) *BatchWriter[T] {
	return &BatchWriter[T]{
		cfg:   cfg,
		queue: NewConcurrentQueue[T](cfg.QueueBufferSize),
		quit:  make(chan struct{}),
	}
}

// Start starts the BatchWriter.
func (b *BatchWriter[T]) Start() {
	b.started.Do(func() {
		b.queue.Start()

		b.wg.Add(1)
		go b.manageNewItems()
	})
}

// Stop stops the BatchWriter.
func (b *BatchWriter[T]) Stop() {
	b.stopped.Do(func() {
		close(b.quit)
		b.wg.Wait()

		b.queue.Stop()
	})
}

// AddItem adds a given item to the BatchWriter queue.
func (b *BatchWriter[T]) AddItem(item T) {
	b.queue.ChanIn() <- item
}

// manageNewItems manages collecting filters and persisting them to the DB.
// There are two conditions for writing a batch of filters to the DB: the first
// is if a certain threshold (MaxBatch) of filters has been collected and the
// other is if at least one filter has been collected and a timeout has been
// reached.
//
// NOTE: this must be run in a goroutine.
func (b *BatchWriter[T]) manageNewItems() {
	defer b.wg.Done()

	batch := make([]T, 0, b.cfg.MaxBatch)

	// writeBatch writes the current contents of the batch slice to the
	// filters DB.
	writeBatch := func() {
		if len(batch) == 0 {
			return
		}

		err := b.cfg.PutItems(batch...)
		if err != nil {
			log.Errorf("Could not write filters to filterDB: %v",
				err)
		}

		// Empty the batch slice.
		batch = make([]T, 0, b.cfg.MaxBatch)
	}

	ticker := time.NewTicker(b.cfg.DBWritesTickerDuration)
	defer ticker.Stop()

	// Stop the ticker since we don't want it to tick unless there is at
	// least one item in the queue.
	ticker.Stop()

	for {
		select {
		case filter, ok := <-b.queue.ChanOut():
			if !ok {
				return
			}

			batch = append(batch, filter)

			switch len(batch) {
			// If the batch slice is full, we stop the ticker and
			// write the batch contents to disk.
			case b.cfg.MaxBatch:
				ticker.Stop()
				writeBatch()

			// If an item is added to the batch, we reset the timer.
			// This ensures that if the batch threshold is not met
			// then items are still persisted in a timely manner.
			default:
				ticker.Reset(b.cfg.DBWritesTickerDuration)
			}

		case <-ticker.C:
			// If the ticker ticks, then we stop it and write the
			// current batch contents to the db. If any more items
			// are added, the ticker will be reset.
			ticker.Stop()
			writeBatch()

		case <-b.quit:
			writeBatch()

			return
		}
	}
}
