package api

import (
	"context"
	"os"
	"testing"
	"time"

	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/scheduler"
)

func TestCancelReleasesResourcesForFollowOnTasks(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]

	result, err := runtime.Canceler.Cancel(context.Background(), value.ID, "weather front")
	if err != nil {
		t.Fatal(err)
	}
	if result.ReservationsReleased != 1 || !result.AntennaReleased || result.CommandsRetired == 0 {
		t.Fatalf("compensation incomplete: %#v", result)
	}
	updated, found := runtime.Passes.Get(value.ID)
	if !found || updated.Phase != passsvc.Canceled {
		t.Fatalf("pass phase = %s, want canceled", updated.Phase)
	}
	for _, interval := range runtime.Timeline.List("") {
		if interval.PassID == value.ID && interval.Status != scheduler.Released {
			t.Fatalf("reservation %s still %s", interval.ReservationID, interval.Status)
		}
	}
	antenna, _ := runtime.Antennas.State(value.AntennaID)
	if antenna.OwnerPassID != "" {
		t.Fatalf("antenna still owned by pass %s", antenna.OwnerPassID)
	}
	for _, command := range runtime.Commands.List(value.ID) {
		if !command.Retired {
			t.Fatalf("pointing command %s survived cancel", command.ID)
		}
	}
	for _, command := range runtime.Commands.Due(time.Now().Add(2 * time.Hour)) {
		if command.PassID == value.ID {
			t.Fatalf("canceled pass command %s still due", command.ID)
		}
	}

	// A follow-on task overlapping the same antenna window must not conflict.
	follow, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: value.AntennaID, RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: value.Window.AOS.Add(time.Minute), LOS: value.Window.LOS.Add(time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(follow.ID, follow.Window); err != nil {
		t.Fatal(err)
	}
	reservation, err := runtime.Reservations.ReservePass(follow.ID, follow.AntennaID)
	if err != nil {
		t.Fatalf("follow-on reservation falsely conflicts after cancel: %v", err)
	}
	if _, err := runtime.Antennas.Claim(reservation); err != nil {
		t.Fatalf("follow-on antenna claim rejected after cancel: %v", err)
	}
}

func TestCancelTrackingPassSealsRecordingButKeepsEvidence(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	attempt, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(value.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 7},
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 7},
	}
	if decision, err := runtime.Passes.AcceptDiversity(value.ID, results); err != nil || !decision.Ready {
		t.Fatalf("diversity did not converge: %#v %v", decision, err)
	}
	active, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found {
		t.Fatal("tracking did not start a recorder")
	}
	session, err := runtime.NewDecoderSession(value.ID, active.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.OnLock(attempt.ID); err != nil {
		t.Fatal(err)
	}
	if written, err := session.Feed(attempt.ID, make([]byte, 128)); err != nil || written != 2 {
		t.Fatalf("decoder feed wrote %d frames: %v", written, err)
	}

	result, err := runtime.Canceler.Cancel(context.Background(), value.ID, "operator abort")
	if err != nil {
		t.Fatal(err)
	}
	if result.SegmentSealed != active.ID || result.FramesDrained != 2 {
		t.Fatalf("recording compensation = %#v, want segment %s sealed with 2 frames", result, active.ID)
	}
	var sealed recorder.Segment
	for _, segment := range runtime.Recorder.List() {
		if segment.ID == active.ID {
			sealed = segment
		}
	}
	if sealed.Status != recorder.Sealed || sealed.FramesWritten != 2 {
		t.Fatalf("segment = %s with %d frames, want sealed with 2", sealed.Status, sealed.FramesWritten)
	}
	// Evidence stays on disk…
	info, err := os.Stat(runtime.Paths.Recording(active.ID))
	if err != nil || info.Size() == 0 {
		t.Fatalf("recording evidence missing on disk: %v", err)
	}
	// …but the segment no longer accepts frames.
	if err := runtime.Recorder.Buffer(active.ID, recorder.Frame{Sequence: 99, Payload: make([]byte, 64)}); err == nil {
		t.Fatal("sealed segment accepted a frame after cancel")
	}
	if _, found := runtime.Recorder.ActiveForPass(value.ID); found {
		t.Fatal("canceled pass still has an active recording")
	}
}

func TestCancelIsIdempotent(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	if _, err := runtime.Canceler.Cancel(context.Background(), value.ID, "first"); err != nil {
		t.Fatal(err)
	}
	second, err := runtime.Canceler.Cancel(context.Background(), value.ID, "retry")
	if err != nil {
		t.Fatalf("repeat cancel must converge: %v", err)
	}
	if second.ReservationsReleased != 0 || second.AntennaReleased || second.CommandsRetired != 0 {
		t.Fatalf("repeat cancel repeated compensation: %#v", second)
	}
}

func TestCancelRejectsCompletedPass(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	attempt, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(value.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 7},
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 7},
	}
	if _, err := runtime.Passes.AcceptDiversity(value.ID, results); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Finalizer.Finalize(context.Background(), value.ID, "RF-01"); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Canceler.Cancel(context.Background(), value.ID, "too late"); err == nil {
		t.Fatal("completed pass must reject cancel")
	}
}

func TestCancelBlocksLateAcquisitionResult(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	attempt, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(value.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Canceler.Cancel(context.Background(), value.ID, "abort"); err != nil {
		t.Fatal(err)
	}
	// A late acquisition result for the in-flight attempt must not resurrect
	// the canceled pass back into tracking.
	err = runtime.Passes.ApplyAcquisitionResult(receiver.AcquisitionResult{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 9})
	if err != nil {
		t.Fatal(err)
	}
	updated, _ := runtime.Passes.Get(value.ID)
	if updated.Phase != passsvc.Canceled {
		t.Fatalf("late acquisition resurrected pass to %s", updated.Phase)
	}
}
