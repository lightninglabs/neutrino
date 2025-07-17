package headerfs

import (
	"os"
	"runtime"
)

// singleTruncate truncates a single header from the end of the header file.
// This can be used in the case of a re-org to remove the last header from the
// end of the main chain.
//
// TODO(roasbeef): define this and the two methods above on a headerFile
// struct?
func (h *headerStore) singleTruncate() error {
	// In order to truncate the file, we'll need to grab the absolute size
	// of the file as it stands currently.
	fileInfo, err := h.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// Next, we'll determine the number of bytes we need to truncate from
	// the end of the file.
	truncateLength, err := h.indexType.Size()
	if err != nil {
		return err
	}
	newSize := fileSize - int64(truncateLength)

	// On Windows, we need to close, truncate, and reopen the file.
	if runtime.GOOS == "windows" {
		fileName := h.file.Name()
		if err = h.file.Close(); err != nil {
			return err
		}

		if err = os.Truncate(fileName, newSize); err != nil {
			return err
		}

		fileFlags := os.O_RDWR | os.O_APPEND | os.O_CREATE
		h.file, err = os.OpenFile(fileName, fileFlags, 0644)
		return err
	}

	return h.file.Truncate(newSize)
}
