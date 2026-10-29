package headerfs

import (
	"database/sql"
	"errors"
	"io"
	"os"
	"testing"
)

// TestErrHeaderNotFoundUnwrap checks that ErrHeaderNotFound exposes the read
// error behind it, so a caller can tell a read past the end of the flat file
// or a missing sql row from an I/O failure, which the flat file store also
// reports as a missing header.
func TestErrHeaderNotFoundUnwrap(t *testing.T) {
	for _, inner := range []error{io.EOF, sql.ErrNoRows, os.ErrClosed} {
		var err error = &ErrHeaderNotFound{inner}
		if !errors.Is(err, inner) {
			t.Fatalf("%v does not unwrap to %v", err, inner)
		}
	}
	if errors.Is(&ErrHeaderNotFound{os.ErrClosed}, io.EOF) {
		t.Fatal("an I/O failure unwraps to io.EOF")
	}
}
