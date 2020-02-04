package server

import (
	"github.com/coreos/go-systemd/daemon"
)

func daemonReady() {
	// signal readiness, ignore errors
	daemon.SdNotify(false, daemon.SdNotifyReady)
}
