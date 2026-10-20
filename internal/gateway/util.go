package gateway

import (
	"time"

	"github.com/aidenwallis/disgord/internal/gateway/cmd"
	"github.com/aidenwallis/disgord/internal/gateway/event"
	"github.com/aidenwallis/disgord/internal/gateway/opcode"
)

func CmdNameToOpCode(command string, t ClientType) (op opcode.OpCode) {
	op = opcode.None
	// TODO: refactor command and event name to avoid conversion (?)
	if t == clientTypeVoice {
		switch command {
		case cmd.VoiceSpeaking:
			op = opcode.VoiceSpeaking
		case cmd.VoiceIdentify:
			op = opcode.VoiceIdentify
		case cmd.VoiceSelectProtocol:
			op = opcode.VoiceSelectProtocol
		case cmd.VoiceHeartbeat:
			op = opcode.VoiceHeartbeat
		case cmd.VoiceResume:
			op = opcode.VoiceResume
		}
	} else if t == clientTypeEvent {
		switch command {
		case event.Heartbeat:
			op = opcode.EventHeartbeat
		case event.Identify:
			op = opcode.EventIdentify
		case event.Resume:
			op = opcode.EventResume
		case cmd.RequestGuildMembers:
			op = opcode.EventRequestGuildMembers
		case cmd.UpdateVoiceState:
			op = opcode.EventVoiceStateUpdate
		case cmd.UpdateStatus:
			op = opcode.EventStatusUpdate
		}
	}

	return op
}

// rotateByTime deletes every timestamp before "limit". Assumes the oldest entries are first.
func rotateByTime(times []time.Time, limit time.Time) []time.Time {
	var delim int
	for delim < len(times) && times[delim].Before(limit) {
		delim++
	}

	// shift to avoid re-allocations
	n := copy(times, times[delim:])
	return times[:n]
}
