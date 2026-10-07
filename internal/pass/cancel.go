package pass

import (
	"context"
	"errors"
	"fmt"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	"orbitlink/internal/recorder"
	"orbitlink/internal/scheduler"
)

// CancelResult reports which resources a cancellation released. Every
// compensation step is idempotent, so repeating Cancel after a partial
// failure converges to the same released state.
type CancelResult struct {
	PassID               string `json:"pass_id"`
	Phase                Phase  `json:"phase"`
	ReservationsReleased int    `json:"reservations_released"`
	AntennaReleased      bool   `json:"antenna_released"`
	CommandsRetired      int    `json:"commands_retired"`
	FramesDrained        int    `json:"frames_drained"`
	SegmentSealed        string `json:"segment_sealed,omitempty"`
}

// Canceler owns the resource compensation boundary for pass cancellation:
// flipping the phase is never enough — the timeline reservation, antenna
// ownership, queued pointing commands and the recording segment must all be
// released so follow-on tasks are not falsely judged as conflicting.
type Canceler struct {
	passes   *Service
	timeline *scheduler.Timeline
	fleet    *antenna.Fleet
	commands *scheduler.CommandQueue
	recorder *recorder.Manager
	audit    *audit.Trail
}

func NewCanceler(passes *Service, timeline *scheduler.Timeline, fleet *antenna.Fleet, commands *scheduler.CommandQueue, manager *recorder.Manager, trail *audit.Trail) *Canceler {
	return &Canceler{passes: passes, timeline: timeline, fleet: fleet, commands: commands, recorder: manager, audit: trail}
}

func (c *Canceler) Cancel(ctx context.Context, passID, reason string) (CancelResult, error) {
	if reason == "" {
		reason = "operator cancel"
	}
	// State boundary first: the phase flip stops acquisition, extension and
	// replanning from accepting new work for this pass.
	if err := c.passes.Cancel(passID, reason); err != nil {
		return CancelResult{}, err
	}
	value, _ := c.passes.Get(passID)
	result := CancelResult{PassID: passID, Phase: Canceled}

	// Compensation. Each step is idempotent; a failure is collected rather
	// than aborting, so the remaining releases still happen and a retried
	// Cancel converges.
	var errs []error
	for _, interval := range c.timeline.List(value.AntennaID) {
		if interval.PassID != passID || interval.Status == scheduler.Released {
			continue
		}
		if _, err := c.timeline.Release(interval.ReservationID); err != nil {
			errs = append(errs, fmt.Errorf("release reservation %s: %w", interval.ReservationID, err))
			continue
		}
		result.ReservationsReleased++
	}
	released, err := c.fleet.ReleaseForPass(value.AntennaID, passID)
	if err != nil {
		errs = append(errs, fmt.Errorf("release antenna %s: %w", value.AntennaID, err))
	} else {
		result.AntennaReleased = released
	}
	retired, err := c.commands.RetireForPass(passID, "pass canceled")
	if err != nil {
		errs = append(errs, fmt.Errorf("retire pointing commands: %w", err))
	} else {
		result.CommandsRetired = retired
	}
	// Written frames stay on disk as evidence; draining flushes the buffered
	// tail and sealing stops the segment from accepting any more frames.
	if segment, found := c.recorder.ActiveForPass(passID); found {
		drained, err := c.recorder.Drain(ctx, segment.ID)
		result.FramesDrained = drained
		if err != nil {
			errs = append(errs, fmt.Errorf("drain recording %s: %w", segment.ID, err))
		} else if _, err := c.recorder.Seal(segment.ID, "pass canceled: "+reason); err != nil {
			errs = append(errs, fmt.Errorf("seal recording %s: %w", segment.ID, err))
		} else {
			result.SegmentSealed = segment.ID
		}
	}
	_, _ = c.audit.Record(audit.Event{Component: "pass", Action: "canceled", Subject: passID, Fields: map[string]any{
		"reason": reason, "reservations_released": result.ReservationsReleased,
		"antenna_released": result.AntennaReleased, "commands_retired": result.CommandsRetired,
		"frames_drained": result.FramesDrained, "segment_sealed": result.SegmentSealed,
	}})
	return result, errors.Join(errs...)
}
