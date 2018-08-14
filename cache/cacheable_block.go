package cache

import "github.com/btcsuite/btcutil"

// CacheableBlock is a wrapper around the btcutil.Block type which provides a
// Size method used by the cache to target certain memory usage.
type CacheableBlock struct {
	*btcutil.Block
}

// Size returns size of this block in bytes.
func (c *CacheableBlock) Size() (uint64, error) {
	f, err := c.Block.Bytes()
	if err != nil {
		return 0, err
	}
	return uint64(len(f)), nil
}
