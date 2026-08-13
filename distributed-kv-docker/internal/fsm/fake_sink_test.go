package fsm

import (
	"bytes"
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
