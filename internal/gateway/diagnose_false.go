//go:build !disgord_diagnosews
// +build !disgord_diagnosews

package gateway

import "github.com/aidenwallis/disgord/internal/logger"

const SaveIncomingPackets = false

func saveOutgoingPacket(_ logger.Logger, _ string, _ ClientType, _ uint, _ *clientPacket) {}
func saveIncomingPacker(_ logger.Logger, _ string, _ ClientType, _ uint, _ *DiscordPacket, _ []byte) {
}
