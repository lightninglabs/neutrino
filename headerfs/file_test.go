package headerfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

// TestAppendRow verifies that headerStore.appendRaw correctly appends data to
// the file, handles full and partial write errors, and properly recovers from
// failures.
func TestAppendRow(t *testing.T) {
	tests := []struct {
		name          string
		initialData   []byte
		headerToWrite []byte
		writeFn       func([]byte, File) (int, error)
		truncFn       func(int64, File) error
		expected      []byte
		wantErr       bool
		errMsg        string
	}{
		{
			name:          "ValidWrite AppendsData",
			initialData:   []byte{0x01, 0x02, 0x03},
			headerToWrite: []byte{0x04, 0x05, 0x06},
			expected: []byte{
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
			},
		},
		{
			name:          "WriteError NoData Preserved",
			initialData:   []byte{0x01, 0x02, 0x03},
			headerToWrite: []byte{0x04, 0x05, 0x06},
			writeFn: func(p []byte, _ File) (int, error) {
				return 0, errors.New("simulated write failure")
			},
			expected: []byte{0x01, 0x02, 0x03},
			wantErr:  true,
			errMsg:   "simulated write failure",
		},
		{
			name:          "PartialWrite MidwayError Rollback",
			initialData:   []byte{0x01, 0x02, 0x03},
			headerToWrite: []byte{0x04, 0x05, 0x06},
			writeFn: func(p []byte, file File) (int, error) {
				// Mock a partial write - write the first two
				// bytes.
				n, err := file.Write(p[:2])
				if err != nil {
					return n, err
				}

				return n, errors.New("simulated partial " +
					"write failure")
			},
			expected: []byte{0x01, 0x02, 0x03},
			wantErr:  true,
			errMsg:   "simulated partial write failure",
		},
		{
			name:          "TruncateError CompoundFail",
			initialData:   []byte{0x01, 0x02, 0x03},
			headerToWrite: []byte{0x04, 0x05, 0x06},
			writeFn: func(p []byte, file File) (int, error) {
				// Mock a partial write - write just the first
				// byte.
				n, err := file.Write(p[:1])
				if err != nil {
					return n, err
				}

				return n, errors.New("simulated partial " +
					"write failure")
			},
			truncFn: func(size int64, _ File) error {
				return errors.New("simulated truncate failure")
			},
			expected: []byte{0x01, 0x02, 0x03, 0x04},
			wantErr:  true,
			errMsg: fmt.Sprintf("failed to write header type %s: "+
				"partial write (1 bytes), write error: "+
				"simulated partial write failure, truncate "+
				"error: simulated truncate failure", Block),
		},
		{
			name:          "PartialWrite TruncateFail Unrecovered",
			initialData:   []byte{0x01, 0x02, 0x03},
			headerToWrite: []byte{0x04, 0x05, 0x06},
			writeFn: func(p []byte, file File) (int, error) {
				// Mock a partial write - write the first two
				// bytes.
				n, err := file.Write(p[:2])
				if err != nil {
					return n, err
				}

				return n, errors.New("simulated partial " +
					"write failure")
			},
			truncFn: func(size int64, file File) error {
				// Simulate an incomplete truncation: shrink the
				// file by just one byte, leaving part of the
				// partial write data in place in other words
				// not fully removing the partially written
				// header from the end of the file.
				err := file.Truncate(4)
				if err != nil {
					return err
				}

				return errors.New("simulated truncate failure")
			},
			expected: []byte{0x01, 0x02, 0x03, 0x04},
			wantErr:  true,
			errMsg: fmt.Sprintf("failed to write header type "+
				"%s: partial write (2 bytes), write error: "+
				"simulated partial write failure, truncate "+
				"error: simulated truncate failure", Block),
		},
		{
			name:          "NormalWrite ValidHeader DataAppended",
			initialData:   []byte{},
			headerToWrite: []byte{0x01, 0x02, 0x03},
			expected:      []byte{0x01, 0x02, 0x03},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create a temporary file for testing.
			tmpFile, cleanup := createFile(t, "header_store_test")
			defer cleanup()

			// Write initial data.
			_, err := tmpFile.Write(test.initialData)
			if err != nil {
				t.Fatalf("Failed to write initial "+
					"data: %v", err)
			}

			// Reset the file position to the end of initial data.
			_, err = tmpFile.Seek(
				int64(len(test.initialData)), io.SeekStart,
			)
			if err != nil {
				t.Fatalf("Failed to seek: %v", err)
			}

			// Create a mock file that wraps the real file.
			mockFile := &mockFile{
				File:    tmpFile,
				writeFn: test.writeFn,
				truncFn: test.truncFn,
			}

			// Create a header store with our mock file.
			h := &headerStore{
				file:        mockFile,
				headerIndex: &headerIndex{indexType: Block},
			}

			// Call the function being tested.
			err = h.appendRaw(test.headerToWrite)
			if err == nil && test.wantErr {
				t.Fatal("expected an error, but got none")
			}
			if err != nil && !test.wantErr {
				t.Fatalf("unexpected error: %v", err)
			}
			if err != nil && test.wantErr &&
				!strings.Contains(err.Error(), test.errMsg) {

				t.Errorf("expected error message %q to be "+
					"in %q", test.errMsg, err.Error())
			}

			// Reset file position to start for reading.
			if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
				t.Fatalf("Failed to seek to start: %v", err)
			}

			// Read the file contents.
			actualData, err := io.ReadAll(tmpFile)
			if err != nil {
				t.Fatalf("Failed to read file: %v", err)
			}

			// Compare expected vs. actual file contents.
			if !bytes.Equal(actualData, test.expected) {
				t.Fatalf("Expected file data: %v, "+
					"got: %v", test.expected, actualData)
			}
		})
	}
}

// BenchmarkHeaderStoreAppendRaw measures performance of headerStore.appendRaw
// by writing 80-byte headers to a file and resetting position between writes
// to isolate raw append performance from file size effects.
func BenchmarkHeaderStoreAppendRaw(b *testing.B) {
	// Setup temporary file and headerStore.
	tmpFile, cleanup := createFile(b, "header_benchmark")
	defer cleanup()

	store := &headerStore{
		file:        tmpFile,
		headerIndex: &headerIndex{indexType: Block},
	}

	// Sample header data.
	header := make([]byte, 80)

	// Reset timer to exclude setup time.
	b.ResetTimer()

	// Run benchmark.
	for i := 0; i < b.N; i++ {
		if err := store.appendRaw(header); err != nil {
			b.Fatal(err)
		}

		// Reset file position to beginning to maintain constant file
		// size. This isolates the appendRaw performance overhead
		// without measuring effects of increasing file size.
		if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
	}
}

// mockFile wraps a real file but allows us to override the Write, Sync, and
// Truncate methods.
type mockFile struct {
	*os.File
	writeFn func([]byte, File) (int, error)
	syncFn  func() error
	truncFn func(int64, File) error
}

// Write implements the Write method for FileInterface.
func (m *mockFile) Write(p []byte) (int, error) {
	if m.writeFn != nil {
		return m.writeFn(p, m.File)
	}
	return m.File.Write(p)
}

// Sync implements the Sync method for FileInterface.
func (m *mockFile) Sync() error {
	if m.syncFn != nil {
		return m.syncFn()
	}
	return m.File.Sync()
}

// Truncate implements the Truncate method for FileInterface.
func (m *mockFile) Truncate(size int64) error {
	if m.truncFn != nil {
		return m.truncFn(size, m.File)
	}
	return m.File.Truncate(size)
}

// Ensure mockFile implements necessary interfaces.
var _ io.Writer = &mockFile{}

// createFile creates a temporary file for testing.
func createFile(t testing.TB, filename string) (*os.File, func()) {
	tmpFile, err := os.CreateTemp(t.TempDir(), filename)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	cleanup := func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}

	return tmpFile, cleanup
}
