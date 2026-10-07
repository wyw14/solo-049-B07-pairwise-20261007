package api

import (
	"context"
	"testing"
	"time"

	"orbitlink/internal/decoder"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
)

// cancelPlannedPass frees every held resource so an overlapping pass can then
// reserve the same antenna, and cancellation is idempotent.
func TestCancelPlannedPassReleasesResourcesAndLiftsConflict(t *testing.T) {
	runtime := newTestRuntime(t)
	first := runtime.Passes.List()[0]

	// The seeded pass overlaps this second pass, which must be rejected first.
	second, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: first.AntennaID, RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: first.Window.AOS.Add(time.Minute), LOS: first.Window.LOS.Add(time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(second.ID, second.Window); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Reservations.ReservePass(second.ID, second.AntennaID); err == nil {
		t.Fatal("overlapping reservation unexpectedly succeeded before cancel")
	}

	result, err := runtime.Canceler.Cancel(first.ID, "operator abort")
	if err != nil {
		t.Fatal(err)
	}
	if result.Pass.Phase != passsvc.Canceled {
		t.Fatalf("phase = %s, want canceled", result.Pass.Phase)
	}
	if result.ReservationReleased != 1 {
		t.Fatalf("released intervals = %d, want 1", result.ReservationReleased)
	}
	if !result.AntennaReleased {
		t.Fatal("antenna ownership was not released")
	}
	if result.CommandsRetired == 0 {
		t.Fatal("pointing commands were not retired")
	}
	if !result.DeadlineCanceled {
		t.Fatal("deadline was not canceled")
	}

	if state, _ := runtime.Antennas.State(first.AntennaID); state.OwnerPassID != "" {
		t.Fatalf("antenna still owned by %q after cancel", state.OwnerPassID)
	}
	for _, interval := range runtime.Timeline.List("") {
		if interval.PassID == first.ID && interval.Status != "released" {
			t.Fatalf("interval %s still %s", interval.ReservationID, interval.Status)
		}
	}
	for _, command := range runtime.Commands.List(first.ID) {
		if !command.Retired {
			t.Fatalf("command %s was not retired", command.ID)
		}
	}
	if _, found := runtime.Deadlines.Get(first.ID); found {
		t.Fatal("deadline survived cancel")
	}

	// The antenna window is free again: the overlapping reservation succeeds.
	if _, err := runtime.Reservations.ReservePass(second.ID, second.AntennaID); err != nil {
		t.Fatalf("reservation after cancel rejected: %v", err)
	}

	// Idempotent: a second cancel neither errors nor double-releases.
	retry, err := runtime.Canceler.Cancel(first.ID, "operator abort")
	if err != nil {
		t.Fatal(err)
	}
	if retry.ReservationReleased != 0 || retry.CommandsRetired != 0 || retry.AntennaReleased {
		t.Fatalf("retry was not idempotent: %#v", retry)
	}
	if retry.Pass.Phase != passsvc.Canceled {
		t.Fatalf("phase after retry = %s", retry.Pass.Phase)
	}
}

// A tracking pass keeps the frames already written to disk as evidence, but the
// aborted segment must reject new frames and the pass must not be revived by a
// late acquisition result or a finalize call.
func TestCancelTrackingPassSealsEvidenceAndClosesBoundaries(t *testing.T) {
	runtime := newTestRuntime(t)
	first := runtime.Passes.List()[0]

	attempt, err := runtime.Attempts.Begin(first.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(first.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: first.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 3},
		{PassID: first.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 3},
	}
	if decision, err := runtime.Passes.AcceptDiversity(first.ID, results); err != nil || !decision.Ready {
		t.Fatalf("diversity did not converge: %#v %v", decision, err)
	}
	active, found := runtime.Recorder.ActiveForPass(first.ID)
	if !found {
		t.Fatal("tracking did not start a recorder")
	}

	// Decode frames: drain two to disk (evidence), leave one buffered in memory.
	session, err := runtime.NewDecoderSession(first.ID, active.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.OnLock(attempt.ID); err != nil {
		t.Fatal(err)
	}
	if written, err := session.Feed(attempt.ID, make([]byte, 128)); err != nil || written != 2 {
		t.Fatalf("decoder feed wrote %d frames: %v", written, err)
	}
	drained, err := runtime.Recorder.Drain(context.Background(), active.ID)
	if err != nil || drained != 2 {
		t.Fatalf("drain = %d, %v", drained, err)
	}
	evidenceSize, err := runtime.Recorder.OnDiskSize(active.ID)
	if err != nil || evidenceSize == 0 {
		t.Fatalf("expected on-disk evidence, size = %d err = %v", evidenceSize, err)
	}

	if _, err := runtime.Canceler.Cancel(first.ID, "lost downlink"); err != nil {
		t.Fatal(err)
	}
	aborted, found := func() (recorder.Segment, bool) {
		for _, segment := range runtime.Recorder.List() {
			if segment.ID == active.ID {
				return segment, true
			}
		}
		return recorder.Segment{}, false
	}()
	if !found {
		t.Fatal("recording segment vanished after cancel")
	}
	if aborted.Status != recorder.Aborted {
		t.Fatalf("segment status = %s, want aborted", aborted.Status)
	}
	if aborted.FramesWritten != 2 {
		t.Fatalf("written frames = %d, want evidence preserved (2)", aborted.FramesWritten)
	}
	if aborted.FramesBuffered != 0 {
		t.Fatalf("buffered frames = %d, want undelivered buffer discarded", aborted.FramesBuffered)
	}
	if size, _ := runtime.Recorder.OnDiskSize(active.ID); size != evidenceSize {
		t.Fatalf("on-disk evidence changed: %d -> %d", evidenceSize, size)
	}

	// The canceled segment must not accept new frames.
	if err := runtime.Recorder.Buffer(active.ID, recorder.Frame{Sequence: 10, Epoch: 3, Payload: decoder.EncodeTelemetry(10, 64)}); err == nil {
		t.Fatal("aborted segment accepted a new frame")
	}
	if _, err := session.Feed(attempt.ID, make([]byte, 128)); err == nil {
		t.Fatal("decoder kept feeding a canceled recording")
	}
	if _, found := runtime.Recorder.ActiveForPass(first.ID); found {
		t.Fatal("aborted segment still reported as active")
	}
	if _, err := runtime.receiverSession(first.ID); err == nil {
		t.Fatal("new receiver session opened against canceled pass")
	}

	// A late, in-flight acquisition result must not resurrect the pass.
	if err := runtime.Attempts.Deliver(receiver.AcquisitionResult{PassID: first.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 4}); err != nil {
		t.Fatalf("late deliver should be ignored, got %v", err)
	}
	if value, _ := runtime.Passes.Get(first.ID); value.Phase != passsvc.Canceled {
		t.Fatalf("late result revived pass to %s", value.Phase)
	}

	// Finalize is a Tracking-only boundary and must refuse a canceled pass.
	if _, err := runtime.Finalizer.Finalize(context.Background(), first.ID, "RF-01"); err == nil {
		t.Fatal("finalize accepted a canceled pass")
	}
	if value, _ := runtime.Passes.Get(first.ID); value.Phase != passsvc.Canceled {
		t.Fatalf("finalize moved pass to %s", value.Phase)
	}

	// Failure cannot overwrite cancellation either.
	if err := runtime.Passes.Fail(first.ID, "late failure"); err == nil {
		t.Fatal("fail overwrote a canceled pass")
	}
}

// Frames still only in the in-memory buffer (never drained to disk) are
// discarded on cancel; they must not be fabricated into persisted evidence.
func TestCancelDiscardsUndeliveredBuffer(t *testing.T) {
	runtime := newTestRuntime(t)
	first := runtime.Passes.List()[0]

	attempt, err := runtime.Attempts.Begin(first.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(first.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: first.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 1},
		{PassID: first.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 1},
	}
	if decision, err := runtime.Passes.AcceptDiversity(first.ID, results); err != nil || !decision.Ready {
		t.Fatalf("diversity did not converge: %#v %v", decision, err)
	}
	active, found := runtime.Recorder.ActiveForPass(first.ID)
	if !found {
		t.Fatal("tracking did not start a recorder")
	}
	// Buffer frames but never drain: evidence has not been persisted.
	for sequence := uint64(1); sequence <= 2; sequence++ {
		if err := runtime.Recorder.Buffer(active.ID, recorder.Frame{Sequence: sequence, Epoch: 1, Payload: decoder.EncodeTelemetry(sequence, 64)}); err != nil {
			t.Fatal(err)
		}
	}
	if size, _ := runtime.Recorder.OnDiskSize(active.ID); size != 0 {
		t.Fatalf("buffered frames should not be on disk yet, size = %d", size)
	}

	if _, err := runtime.Canceler.Cancel(first.ID, "buffer abandon"); err != nil {
		t.Fatal(err)
	}
	for _, value := range runtime.Recorder.List() {
		if value.ID == active.ID {
			if value.Status != recorder.Aborted || value.FramesBuffered != 0 || value.FramesWritten != 0 {
				t.Fatalf("undelivered buffer not discarded: %#v", value)
			}
		}
	}
	if size, _ := runtime.Recorder.OnDiskSize(active.ID); size != 0 {
		t.Fatalf("cancel fabricated evidence on disk, size = %d", size)
	}
}

// Canceling during acquisition stops a pending attempt from ever transitioning
// the pass when its hardware result arrives.
func TestCancelDuringAcquisitionIgnoresPendingResult(t *testing.T) {
	runtime := newTestRuntime(t)
	first := runtime.Passes.List()[0]

	attempt, err := runtime.Attempts.Begin(first.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(first.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Canceler.Cancel(first.ID, "weather hold"); err != nil {
		t.Fatal(err)
	}
	if current, found := runtime.Attempts.Current(first.ID); !found || current.Status != receiver.AttemptCanceled {
		t.Fatalf("attempt status = %s, want canceled", current.Status)
	}

	if err := runtime.Attempts.Deliver(receiver.AcquisitionResult{PassID: first.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 1}); err != nil {
		t.Fatalf("deliver should be ignored, got %v", err)
	}
	if value, _ := runtime.Passes.Get(first.ID); value.Phase != passsvc.Canceled {
		t.Fatalf("pass phase after late result = %s, want canceled", value.Phase)
	}
}
