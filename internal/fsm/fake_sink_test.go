package fsm

import (
	"bytes"
	"errors"
	"io"
)

// fakeSnapshotSink is a minimal in-memory implementation of raft.SnapshotSink,
// used only in tests so we can exercise Persist()/Restore() without spinning
// up a real Raft node.
type fakeSnapshotSink struct {
	buf *bytes.Buffer
}

func newFakeSnapshotSink() *fakeSnapshotSink {
	return &fakeSnapshotSink{buf: &bytes.Buffer{}}
}

func (f *fakeSnapshotSink) Write(p []byte) (int, error) { return f.buf.Write(p) }
func (f *fakeSnapshotSink) Close() error                { return nil }
func (f *fakeSnapshotSink) ID() string                  { return "fake-snapshot-id" }
func (f *fakeSnapshotSink) Cancel() error               { return nil }

// reader exposes the buffered bytes as an io.ReadCloser, matching what
// FSM.Restore expects to receive from Raft.
func (f *fakeSnapshotSink) reader() io.ReadCloser {
	return io.NopCloser(bytes.NewReader(f.buf.Bytes()))
}

// failingSink is a raft.SnapshotSink whose writes always fail, used to test
// Persist's error path.
type failingSink struct {
	cancelled bool
}

func (f *failingSink) Write(p []byte) (int, error) { return 0, errors.New("disk full") }
func (f *failingSink) Close() error                { return nil }
func (f *failingSink) ID() string                  { return "failing-sink" }
func (f *failingSink) Cancel() error               { f.cancelled = true; return nil }

// trackingReadCloser records whether Close was called.
type trackingReadCloser struct {
	io.Reader
	closed bool
}

func (t *trackingReadCloser) Close() error { t.closed = true; return nil }
