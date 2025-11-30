package openapi

import (
	"time"
)

type SystemInfo struct {
	InterfaceVersion string // JSON: interfaceVersion
	Version          string // JSON: version
	Author           string // JSON: author
	StartedAt        time.Time
}

func NewSystemInfo(interfaceVersion, version, author string) *SystemInfo {
	return &SystemInfo{
		InterfaceVersion: interfaceVersion,
		Version:          version,
		Author:           author,
		StartedAt:        time.Now(),
	}
}

// UptimeSeconds returns system uptime in seconds (int64)
func (s *SystemInfo) UptimeSeconds() int64 {
	return int64(time.Since(s.StartedAt).Seconds())
}
