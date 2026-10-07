package pass

import (
	"errors"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/scheduler"
)

// CancelResult reports which resources were compensated when a pass was
// canceled. Every compensation is idempotent, so a failed cancellation can be
// safely retried.
type CancelResult struct {
	Pass                Pass `json:"pass"`
	CommandsRetired     int  `json:"commands_retired"`
	AttemptsCanceled    int  `json:"attempts_canceled"`
	SegmentsAborted     int  `json:"segments_aborted"`
	ReservationReleased int  `json:"reservation_intervals_released"`
	AntennaReleased     bool `json:"antenna_released"`
	DeadlineCanceled    bool `json:"deadline_canceled"`
}

// Canceler performs the full cancellation boundary: move the pass to Canceled
// and then release every resource it still holds so downstream passes are not
// blocked by stale reservations, antenna ownership, commands, attempts or
// recording sessions.
type Canceler struct {
	passes    *Service
	timeline  *scheduler.Timeline
	fleet     *antenna.Fleet
	commands  *scheduler.CommandQueue
	attempts  *receiver.AttemptDispatcher
	recorder  *recorder.Manager
	deadlines *DeadlineManager
	audit     *audit.Trail
}

func NewCanceler(passes *Service, timeline *scheduler.Timeline, fleet *antenna.Fleet, commands *scheduler.CommandQueue, attempts *receiver.AttemptDispatcher, manager *recorder.Manager, deadlines *DeadlineManager, trail *audit.Trail) *Canceler {
	return &Canceler{passes: passes, timeline: timeline, fleet: fleet, commands: commands, attempts: attempts, recorder: manager, deadlines: deadlines, audit: trail}
}

// Cancel transitions the pass to Canceled and compensates all held resources.
// The phase transition happens first so the pass stops accepting new lifecycle
// events; resource release then runs best-effort and is fully idempotent.
func (c *Canceler) Cancel(passID, reason string) (CancelResult, error) {
	value, found := c.passes.Get(passID)
	if !found {
		return CancelResult{}, errors.New("pass " + passID + " not found")
	}

	// Idempotent on the phase transition: an already-canceled pass is safe to
	// re-compensate in case a previous attempt failed partway through.
	if err := c.passes.Cancel(passID, reason); err != nil {
		return CancelResult{}, err
	}

	result := CancelResult{Pass: value}
	var failures []error

	// Stop pointing commands before antenna ownership is released so no due
	// command can execute during teardown.
	retired, err := c.commands.RetireForPass(passID, "pass canceled: "+reason)
	result.CommandsRetired = retired
	if err != nil {
		failures = append(failures, err)
	}

	// Any in-flight acquisition attempt is terminal; late hardware results are
	// ignored at both the dispatcher and the pass boundary.
	canceled, err := c.attempts.CancelPending(passID, reason)
	result.AttemptsCanceled = canceled
	if err != nil {
		failures = append(failures, err)
	}

	// Close the recording segment so it cannot accept more frames. Frames
	// already persisted to disk remain as evidence; only the in-memory buffer
	// that never reached disk is discarded.
	aborted, err := c.recorder.AbortForPass(passID, "pass canceled: "+reason)
	result.SegmentsAborted = aborted
	if err != nil {
		failures = append(failures, err)
	}

	// Release antenna ownership while it still belongs to this pass, then free
	// the timeline window so future passes no longer conflict with it.
	released, err := c.fleet.ReleaseIfOwned(value.AntennaID, passID)
	result.AntennaReleased = released
	if err != nil {
		failures = append(failures, err)
	}
	releasedIntervals, err := c.timeline.ReleaseForPass(passID)
	result.ReservationReleased = releasedIntervals
	if err != nil {
		failures = append(failures, err)
	}

	if _, scheduled := c.deadlines.Get(passID); scheduled {
		c.deadlines.Cancel(passID)
		result.DeadlineCanceled = true
	}

	if current, ok := c.passes.Get(passID); ok {
		result.Pass = current
	}
	_, _ = c.audit.Record(audit.Event{Component: "pass", Action: "canceled", Subject: passID, Fields: map[string]any{
		"reason": reason, "commands_retired": result.CommandsRetired, "attempts_canceled": result.AttemptsCanceled,
		"segments_aborted": result.SegmentsAborted, "intervals_released": result.ReservationReleased,
		"antenna_released": result.AntennaReleased, "deadline_canceled": result.DeadlineCanceled,
	}})
	return result, errors.Join(failures...)
}
