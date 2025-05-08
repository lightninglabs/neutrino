package headerfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

func TestAppendRow(t *testing.T) {
	tests := []struct {
		name          string
		initialData   []byte
		headerToWrite []byte
		writeFn       func([]byte, FileInterface) (int, error)
		truncFn       func(size int64) error
		expected      []byte
		wantErr       bool
		errMsg        string
	}{
		{
			name:          "NormalWrite_ValidHeader_DataAppendedSuccessfully",
			initialData:   []byte{0x01, 0x02, 0x03},
			headerToWrite: []byte{0x04, 0x05, 0x06},
			expected:      []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06},
		},
		{
			name:          "WriteError_ZeroBytesWritten_OriginalDataPreserved",
			initialData:   []byte{0x01, 0x02, 0x03},
			headerToWrite: []byte{0x04, 0x05, 0x06},
			writeFn: func(p []byte, _ FileInterface) (int, error) {
				return 0, errors.New("simulated write failure")
			},
			expected: []byte{0x01, 0x02, 0x03},
			wantErr:  true,
			errMsg:   "simulated write failure",
		},
		{
			name:          "PartialWrite_WriteErrorMidway_RollsBackToOriginal",
			initialData:   []byte{0x01, 0x02, 0x03},
			headerToWrite: []byte{0x04, 0x05, 0x06},
			writeFn: func(p []byte, file FileInterface) (int, error) {
				// Mock a partial write - write the first two bytes.
				n, _ := file.Write(p[:2])
				return n, errors.New("simulated partial write failure")
			},
			expected: []byte{0x01, 0x02, 0x03},
			wantErr:  true,
			errMsg:   "simulated partial write failure",
		},
		{
			name:          "PartialWrite_TruncateFailure_ReportsCompoundError",
			initialData:   []byte{0x01, 0x02, 0x03},
			headerToWrite: []byte{0x04, 0x05, 0x06},
			writeFn: func(p []byte, file FileInterface) (int, error) {
				// Mock a partial write - write just the first byte.
				n, _ := file.Write(p[:1])
				return n, errors.New("simulated partial write failure")
			},
			truncFn: func(size int64) error {
				return errors.New("simulated truncate failure")
			},
			expected: []byte{0x01, 0x02, 0x03, 0x04},
			wantErr:  true,
			errMsg: "write failed with simulated partial write failure and " +
				"recovery failed with: simulated truncate failure",
		},
		{
			name:          "NormalWrite_ValidHeader_DataAppendedSuccessfully",
			initialData:   []byte{},
			headerToWrite: []byte{0x01, 0x02, 0x03},
			expected:      []byte{0x01, 0x02, 0x03},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create a temporary file for testing.
			tmpFile, err := os.CreateTemp(t.TempDir(), "header_store_test")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())

			// Write initial data.
			if _, err := tmpFile.Write(test.initialData); err != nil {
				t.Fatalf("Failed to write initial data: %v", err)
			}

			// Reset the file position to the end of initial data.
			_, err = tmpFile.Seek(int64(len(test.initialData)), io.SeekStart)
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
				file: mockFile,
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

// mockFile wraps a real file but allows us to override the Write and
// Sync methods.
type mockFile struct {
	*os.File
	writeFn func([]byte, FileInterface) (int, error)
	syncFn  func() error
	truncFn func(size int64) error
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
		return m.truncFn(size)
	}
	return m.File.Truncate(size)
}

// Ensure mockFile implements necessary interfaces.
var _ io.Writer = &mockFile{}
